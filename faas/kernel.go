package faas

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Config struct {
	DefaultTimeoutMillis  int
	DefaultMaxConcurrency int
	DefaultRetries        RetryPolicy
	IdleTTL               time.Duration
	Logger                Logger
	Store                 EventStore
	// RetryWait is overridable in tests. Production defaults to no delay.
	RetryWait func(attempt int)
}

type Kernel struct {
	config   Config
	registry *Registry
	router   *Router
	pool     *InstancePool
	logger   Logger
	store    EventStore

	invokeMu   sync.Mutex
	metadataMu sync.RWMutex
	calls      map[idempotencyScope]*idempotentEntry
}

type idempotencyScope struct {
	Key      string
	Function string
	Version  VersionNumber
}

type idempotentEntry struct {
	done     bool
	response InvokeResponse
	err      *Error
	cond     *sync.Cond
	running  bool
}

func New(config Config) *Kernel {
	if config.DefaultTimeoutMillis <= 0 {
		config.DefaultTimeoutMillis = 1000
	}
	if config.DefaultMaxConcurrency <= 0 {
		config.DefaultMaxConcurrency = 8
	}
	if config.DefaultRetries.MaxAttempts <= 0 {
		config.DefaultRetries.MaxAttempts = 1
	}
	if config.DefaultRetries.Retryable == nil {
		config.DefaultRetries.Retryable = map[FailureClass]bool{}
	}
	if config.IdleTTL < 0 {
		config.IdleTTL = 0
	}
	if config.Logger == nil {
		config.Logger = noopLogger{}
	}
	if config.RetryWait == nil {
		config.RetryWait = func(int) {}
	}
	return &Kernel{
		config:   config,
		registry: NewRegistry(),
		router:   NewRouter(),
		pool:     NewInstancePool(),
		logger:   config.Logger,
		store:    config.Store,
		calls:    make(map[idempotencyScope]*idempotentEntry),
	}
}

func defaultSpec(spec FunctionSpec, config Config) FunctionSpec {
	if spec.TimeoutMillis <= 0 {
		spec.TimeoutMillis = config.DefaultTimeoutMillis
	}
	if spec.MaxConcurrency <= 0 {
		spec.MaxConcurrency = config.DefaultMaxConcurrency
	}
	if spec.Retries.MaxAttempts <= 0 {
		spec.Retries = config.DefaultRetries
	}
	if spec.Retries.Retryable == nil {
		spec.Retries.Retryable = map[FailureClass]bool{}
	}
	return spec
}

func (k *Kernel) appendEvent(event StoredEvent) error {
	if k.store != nil {
		if err := k.store.Append(event); err != nil {
			return platformError("append local event: %v", err)
		}
	}
	return nil
}

// Register reserves a function name but publishes no callable version.
func (k *Kernel) Register(spec FunctionSpec) error {
	spec = defaultSpec(spec, k.config)
	k.metadataMu.Lock()
	defer k.metadataMu.Unlock()
	if err := k.registry.Register(spec); err != nil {
		return err
	}
	return nil
}

func (k *Kernel) Publish(spec FunctionSpec) (PublishedVersion, error) {
	spec = defaultSpec(spec, k.config)
	k.metadataMu.Lock()
	defer k.metadataMu.Unlock()
	version, err := k.registry.RegisterOrPublish(spec)
	if err != nil {
		return PublishedVersion{}, err
	}
	if err := k.appendEvent(StoredEvent{
		Kind:     EventPublished,
		Function: version.Function,
		Version:  version.Number,
		Status:   string(version.State),
		Detail:   map[string]any{"timeout_ms": spec.TimeoutMillis, "max_concurrency": spec.MaxConcurrency},
	}); err != nil {
		return PublishedVersion{}, err
	}
	k.log("published", fmt.Sprintf("function=%s version=%d timeout_ms=%d concurrency=%d", spec.Name, version.Number, spec.TimeoutMillis, spec.MaxConcurrency), nil)
	return version, nil
}

// Rollback atomically changes current and optionally repoints a route. Existing
// pinned routes remain stable; callers that used the pinned old version do not
// silently move. New current calls and the optional route hit target.
func (k *Kernel) Rollback(name string, target VersionNumber, route string) (PublishedVersion, error) {
	k.metadataMu.Lock()
	defer k.metadataMu.Unlock()
	var previousTarget RouteTarget
	hasPreviousTarget := false
	previous, previousErr := k.registry.Current(name)
	if previousErr != nil {
		return PublishedVersion{}, previousErr
	}
	previousVersion := previous.Number
	if route != "" {
		if existing, err := k.router.Resolve(route); err == nil {
			previousTarget = existing
			hasPreviousTarget = true
		}
	}
	// Validate the historical target and an existing route before changing the
	// current pointer. This prevents rollback/route operations from leaving a
	// half-committed metadata state.
	if _, err := k.registry.Version(name, target); err != nil {
		k.log("rollback-rejected", fmt.Sprintf("function=%s target=%d", name, target), err)
		return PublishedVersion{}, err
	}
	if route != "" {
		if _, err := k.router.Resolve(route); err != nil {
			k.log("rollback-route-rejected", fmt.Sprintf("function=%s target=%d route=%s", name, target, route), err)
			return PublishedVersion{}, err
		}
	}
	version, err := k.registry.Rollback(name, target)
	if err != nil {
		k.log("rollback-rejected", fmt.Sprintf("function=%s target=%d", name, target), err)
		return PublishedVersion{}, err
	}
	if route != "" {
		if err := k.router.Replace(route, RouteTarget{Function: name, Version: target}); err != nil {
			k.log("rollback-route-rejected", fmt.Sprintf("function=%s target=%d route=%s", name, target, route), err)
			return PublishedVersion{}, err
		}
	}
	if err := k.appendEvent(StoredEvent{Kind: EventRollback, Function: name, Version: target, Status: "current"}); err != nil {
		_, _ = k.registry.Rollback(name, previousVersion)
		if route != "" && hasPreviousTarget {
			_ = k.router.Replace(route, previousTarget)
		}
		return PublishedVersion{}, err
	}
	k.log("rollback", fmt.Sprintf("function=%s current=%d route=%q", name, target, route), nil)
	return version, nil
}

func (k *Kernel) SetVersionAvailable(name string, version VersionNumber, available bool) error {
	k.metadataMu.Lock()
	defer k.metadataMu.Unlock()
	return k.registry.SetAvailable(name, version, available)
}

// SetRoute creates an explicit route. Use ReplaceRoute after publication to
// move a route; an accidental conflict is never silently overwritten.
func (k *Kernel) SetRoute(route string, target RouteTarget) error {
	k.metadataMu.Lock()
	defer k.metadataMu.Unlock()
	_, resolveErr := k.router.Resolve(route)
	existed := resolveErr == nil
	if err := k.router.Set(route, target); err != nil {
		return err
	}
	if _, err := k.registry.Version(target.Function, target.Version); err != nil {
		if !existed {
			_ = k.router.Remove(route)
		}
		return err
	}
	k.log("route-bound", fmt.Sprintf("route=%q function=%s version=%d", route, target.Function, target.Version), nil)
	return nil
}

func (k *Kernel) ReplaceRoute(route string, target RouteTarget) error {
	k.metadataMu.Lock()
	defer k.metadataMu.Unlock()
	if err := k.router.Replace(route, target); err != nil {
		return err
	}
	if _, err := k.registry.Version(target.Function, target.Version); err != nil {
		return err
	}
	k.log("route-replaced", fmt.Sprintf("route=%q function=%s version=%d", route, target.Function, target.Version), nil)
	return nil
}

func (k *Kernel) ReclaimIdleInstances() int {
	n := k.pool.ReclaimIdle(k.config.IdleTTL)
	if n > 0 {
		k.log("reclaim", fmt.Sprintf("reclaimed=%d idle_ttl=%s", n, k.config.IdleTTL), nil)
	}
	return n
}

func (k *Kernel) DrainVersion(function string, version VersionNumber) int {
	return k.pool.DrainVersion(function, version)
}

func (k *Kernel) ClearIdempotencyRecords() {
	k.invokeMu.Lock()
	defer k.invokeMu.Unlock()
	k.calls = make(map[idempotencyScope]*idempotentEntry)
}

func (k *Kernel) log(decision, reason string, err error) {
	k.logger.Log(DecisionLog{Decision: decision, Reason: reason})
}

func newRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return "req-" + hex.EncodeToString(b[:])
}

// Invoke resolves and executes one call. It never falls back to a default
// function or version, and terminal results for a non-empty idempotency key are
// shared by concurrent duplicate calls exactly once.
func (k *Kernel) Invoke(req InvokeRequest) (InvokeResponse, error) {
	if req.Protocol == "" {
		req.Protocol = ProtocolV1
	}
	if err := ValidateRequest(req); err != nil {
		return InvokeResponse{}, err
	}

	k.metadataMu.RLock()
	version, err := k.resolveVersion(req)
	if err != nil {
		k.metadataMu.RUnlock()
		k.logger.Log(DecisionLog{Input: append([]byte(nil), req.Payload...), Decision: "reject", Reason: "target resolution failed: " + err.Error()})
		return InvokeResponse{}, err
	}
	policy, timeout, maxConcurrency, err := k.effectiveOptions(req, version)
	k.metadataMu.RUnlock()
	if err != nil {
		k.logger.Log(DecisionLog{Input: append([]byte(nil), req.Payload...), Decision: "reject", Reason: "option negotiation failed: " + err.Error()})
		return InvokeResponse{}, err
	}

	requestID := newRequestID()
	log := func(decision, reason string) {
		k.logger.Log(DecisionLog{
			RequestID: requestID,
			Input:     append([]byte(nil), req.Payload...),
			Decision:  decision,
			Reason:    reason,
		})
	}

	key := req.IdempotencyKey
	private := key == ""
	if private {
		key = newRequestID()
	}
	scope := idempotencyScope{Key: key, Function: version.Function, Version: version.Number}

	k.invokeMu.Lock()
	if existing := k.calls[scope]; existing != nil {
		for !existing.done {
			existing.cond.Wait()
		}
		resp := cloneResponse(existing.response)
		var callErr error
		if existing.err != nil {
			callErr = existing.err
		}
		k.invokeMu.Unlock()
		log("deduplicated", "idempotency key="+scope.Key+" hit terminal result")
		return resp, callErr
	}
	entry := &idempotentEntry{running: true}
	entry.cond = sync.NewCond(&k.invokeMu)
	k.calls[scope] = entry
	k.invokeMu.Unlock()

	resp, callErr := k.execute(requestID, req, version, policy, timeout, maxConcurrency, log)

	k.invokeMu.Lock()
	entry.done = true
	entry.response = resp
	if callErr != nil {
		var pe *Error
		if !errors.As(callErr, &pe) {
			callErr = platformError("unclassified execution error: %v", callErr)
		}
		entry.err = pe
	}
	entry.cond.Broadcast()
	k.invokeMu.Unlock()
	return resp, callErr
}

func (k *Kernel) resolveVersion(req InvokeRequest) (PublishedVersion, error) {
	if req.Route != "" {
		target, err := k.router.Resolve(req.Route)
		if err != nil {
			return PublishedVersion{}, err
		}
		return k.registry.Version(target.Function, target.Version)
	}
	if req.Version == NoVersion {
		return k.registry.Current(req.Function)
	}
	return k.registry.Version(req.Function, req.Version)
}

func (k *Kernel) effectiveOptions(req InvokeRequest, version PublishedVersion) (RetryPolicy, time.Duration, int, error) {
	policy := version.Spec.Retries
	if policy.Retryable == nil {
		policy.Retryable = map[FailureClass]bool{}
	} else {
		retryable := make(map[FailureClass]bool, len(policy.Retryable))
		for class, allowed := range policy.Retryable {
			retryable[class] = allowed
		}
		policy.Retryable = retryable
	}
	timeoutMillis := version.Spec.TimeoutMillis
	maxConcurrency := version.Spec.MaxConcurrency

	if req.Protocol == ProtocolV2 {
		if req.Options.TimeoutMillis > 0 {
			if req.Options.TimeoutMillis > version.Spec.TimeoutMillis {
				return policy, 0, 0, unsupportedOption("timeout %dms exceeds function limit %dms", req.Options.TimeoutMillis, version.Spec.TimeoutMillis)
			}
			timeoutMillis = req.Options.TimeoutMillis
		}
		if req.Options.MaxConcurrency > 0 {
			if req.Options.MaxConcurrency > version.Spec.MaxConcurrency {
				return policy, 0, 0, unsupportedOption("concurrency %d exceeds function limit %d", req.Options.MaxConcurrency, version.Spec.MaxConcurrency)
			}
			maxConcurrency = req.Options.MaxConcurrency
		}
		if req.Options.MaxAttempts > 0 {
			if req.Options.MaxAttempts > policy.MaxAttempts {
				return policy, 0, 0, unsupportedOption("max attempts %d exceeds function policy %d", req.Options.MaxAttempts, policy.MaxAttempts)
			}
			policy.MaxAttempts = req.Options.MaxAttempts
		}
		if req.Options.Retryable != nil {
			if *req.Options.Retryable {
				policy.Retryable[FailureTimeout] = true
				policy.Retryable[FailureThrottled] = true
				policy.Retryable[FailurePlatform] = true
			} else {
				policy.Retryable = map[FailureClass]bool{}
			}
		}
	}
	if policy.MaxAttempts > 1 && req.IdempotencyKey == "" {
		return policy, 0, 0, invalidArgument("retryable invocation requires IdempotencyKey to prevent duplicate side effects")
	}
	return policy, time.Duration(timeoutMillis) * time.Millisecond, maxConcurrency, nil
}

type attemptResult struct {
	payload []byte
	err     error
}

func (k *Kernel) execute(requestID string, req InvokeRequest, version PublishedVersion, policy RetryPolicy, timeout time.Duration, maxConcurrency int, log func(string, string)) (InvokeResponse, error) {
	attempt := 1
	var lastErr error
	for {
		log("attempt", fmt.Sprintf("function=%s version=%d attempt=%d timeout=%s concurrency=%d input=%q", version.Function, version.Number, attempt, timeout, maxConcurrency, string(req.Payload)))
		payload, inst, err := k.runAttempt(requestID, req, version, attempt, timeout, maxConcurrency)
		if err == nil {
			resp := InvokeResponse{Function: version.Function, Version: version.Number, Attempt: attempt, Payload: payload}
			if eventErr := k.appendEvent(StoredEvent{Kind: EventInvoke, Function: version.Function, Version: version.Number, Key: req.IdempotencyKey, Status: "ok", Detail: map[string]any{"attempt": attempt, "instance": inst.ID}}); eventErr != nil {
				err = eventErr
			} else {
				log("success", fmt.Sprintf("attempt=%d instance=%s", attempt, inst.ID))
				return resp, nil
			}
		}
		lastErr = err
		class := ClassifyError(err)
		retry, reason := ShouldRetry(policy, class, attempt)
		log("attempt-failed", fmt.Sprintf("attempt=%d class=%s kind=%s retry=%t basis=%s cause=%v", attempt, class, ErrorKindOf(err), retry, reason, err))
		if !retry {
			if attempt > 1 {
				lastErr = newError(KindRetryExhausted, "failed after %d attempt(s); last_class=%s", attempt, class, err)
			}
			if eventErr := k.appendEvent(StoredEvent{Kind: EventInvoke, Function: version.Function, Version: version.Number, Key: req.IdempotencyKey, Status: "error", Detail: map[string]any{"attempt": attempt, "kind": string(ErrorKindOf(lastErr))}}); eventErr != nil {
				return InvokeResponse{}, eventErr
			}
			return InvokeResponse{}, lastErr
		}
		k.config.RetryWait(attempt)
		attempt++
	}
}

func (k *Kernel) runAttempt(requestID string, req InvokeRequest, version PublishedVersion, attempt int, timeout time.Duration, maxConcurrency int) ([]byte, *Instance, error) {
	inst, err := k.pool.Acquire(version.Function, version.Number, maxConcurrency)
	if err != nil {
		return nil, nil, err
	}

	runtime, runtimeErr := runtimeForSpec(version.Spec)
	if runtimeErr != nil {
		k.pool.Retire(inst)
		_ = k.pool.Release(inst)
		return nil, inst, runtimeErr
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	complete := make(chan attemptResult, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				complete <- attemptResult{err: functionError("handler panicked", fmt.Errorf("%v", recovered))}
			}
		}()
		payload, callErr := runtime.Run(ctx, &InvocationContext{
			RequestID:      requestID,
			IdempotencyKey: req.IdempotencyKey,
			Payload:        append([]byte(nil), req.Payload...),
			Attempt:        attempt,
		}, req.IdempotencyKey)
		// Runtime implementations classify handler/function failures; wrapping
		// them again would obscure the original reason.
		complete <- attemptResult{payload: payload, err: callErr}
	}()

	select {
	case result := <-complete:
		if result.err != nil {
			k.pool.Retire(inst)
			_ = k.pool.Release(inst)
			return nil, inst, result.err
		}
		if releaseErr := k.pool.Release(inst); releaseErr != nil {
			return nil, inst, releaseErr
		}
		return result.payload, inst, nil
	case <-ctx.Done():
		// The deadline cancels handler work, removes the call from inflight, and
		// prevents this potentially dirty instance from serving another call.
		k.pool.Retire(inst)
		_ = k.pool.Release(inst)
		return nil, inst, timeoutError("function %s@%d timed out after %s", version.Function, version.Number, timeout)
	}
}

func cloneResponse(resp InvokeResponse) InvokeResponse {
	resp.Payload = append([]byte(nil), resp.Payload...)
	return resp
}
