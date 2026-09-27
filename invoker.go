package faas

import (
	"context"
	"sync"
	"time"
)

// Invoker 把路由、注册表、实例池、运行时组合为一次完整调用链路，
// 负责超时控制、并发拒绝、错误归类与按策略重试。
type Invoker struct {
	registry *Registry
	router   *Router
	pool     *InstancePool

	idemMu sync.Mutex
	idem   map[string]*idemEntry
}

// idemEntry 是幂等键对应的已完成调用结论；进行中的调用共享同一等待，
// 从而并发同键调用只产生一次副作用。
type idemEntry struct {
	done chan struct{}
	resp *InvokeResponse
	err  error
}

// NewInvoker 创建调用编排器。
func NewInvoker(reg *Registry, rt *Router, pool *InstancePool) *Invoker {
	return &Invoker{registry: reg, router: rt, pool: pool, idem: make(map[string]*idemEntry)}
}

// resolveTarget 解析一次调用要命中的 (函数, 版本)。
// 未知路由 / 未知函数 / 无可用版本 / 路由所指函数不存在均为可区分错误。
func (iv *Invoker) resolveTarget(req *InvokeRequest) (*FunctionVersion, error) {
	if req.RoutePath != "" {
		route, err := iv.router.Resolve(req.RoutePath)
		if err != nil {
			return nil, err // not_found(route)
		}
		return iv.registry.ResolveForCall(route.FunctionName, route.VersionPin)
	}
	if req.FunctionName == "" {
		return nil, newError(ErrClassInvalid, "invoke request requires route or function")
	}
	return iv.registry.ResolveForCall(req.FunctionName, req.VersionPin)
}

// effectiveTimeout 计算单次尝试超时：v2 显式超时只能收紧不能放宽版本配置。
func effectiveTimeout(v *FunctionVersion, req *InvokeRequest) (time.Duration, error) {
	t := v.Config.Timeout
	if req.CallTimeoutMS > 0 {
		asked := time.Duration(req.CallTimeoutMS) * time.Millisecond
		if asked > v.Config.Timeout {
			return 0, newError(ErrClassInvalid,
				"timeoutMs exceeds the timeout configured for the target version")
		}
		t = asked
	}
	return t, nil
}

// Invoke 执行一次规范化调用：
//  1. 路由 / 直连解析目标版本（未知路由、未知函数、不可用版本分别报错）
//  2. 幂等去重（并发同键共享，只产生一次副作用）
//  3. 按版本配置获取实例（并发上限拒绝）
//  4. 带硬超时执行（超时则强杀实例，绝不归还污染）
//  5. 对 timeout / concurrency / platform 类错误按策略退避重试，
//     function_error / 确定性参数错误不重试；重试耗尽包装为 retry_exhausted。
func (iv *Invoker) Invoke(ctx context.Context, req *InvokeRequest) (*InvokeResponse, error) {
	v, err := iv.resolveTarget(req)
	if err != nil {
		return nil, err
	}
	if _, err := effectiveTimeout(v, req); err != nil {
		return nil, err
	}

	if req.IdempotencyKey != "" {
		return iv.invokeIdempotent(ctx, req, v)
	}
	return iv.invokeWithRetry(ctx, req, v)
}

// invokeIdempotent 处理带幂等键的调用：并发同键合并、结论缓存。
func (iv *Invoker) invokeIdempotent(ctx context.Context, req *InvokeRequest, v *FunctionVersion) (*InvokeResponse, error) {
	iv.idemMu.Lock()
	if entry, ok := iv.idem[req.IdempotencyKey]; ok {
		iv.idemMu.Unlock()
		return awaitEntry(ctx, entry)
	}
	entry := &idemEntry{done: make(chan struct{})}
	iv.idem[req.IdempotencyKey] = entry
	iv.idemMu.Unlock()

	resp, err := iv.invokeWithRetry(ctx, req, v)

	// 先回填结论再关闭 channel：close 构成 happens-before 屏障，
	// 等待者在 <-done 后读到的 resp/err 必然可见。
	iv.idemMu.Lock()
	entry.resp = resp
	entry.err = err
	iv.idemMu.Unlock()
	close(entry.done)

	return resp, err
}

func awaitEntry(ctx context.Context, e *idemEntry) (*InvokeResponse, error) {
	select {
	case <-e.done:
		return e.resp, e.err
	case <-ctx.Done():
		return nil, wrapError(ErrClassTimeout, "caller canceled while waiting for in-flight idempotent call", ctx.Err())
	}
}

// invokeWithRetry 按版本配置的重试策略驱动多次尝试。
// 即使最终失败，也会返回带 Attempts 的响应骨架，便于调用方判定重试次数。
func (iv *Invoker) invokeWithRetry(ctx context.Context, req *InvokeRequest, v *FunctionVersion) (*InvokeResponse, error) {
	policy := v.Config.Retry
	meta := func(attempt int) *InvokeResponse {
		return &InvokeResponse{FunctionName: v.FunctionName, Version: v.Version, Attempts: attempt}
	}
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return meta(attempt - 1), wrapError(ErrClassTimeout, "caller context already canceled before attempt", err)
		}
		resp, err := iv.invokeOnce(ctx, v, req, attempt)
		if err == nil {
			resp.Attempts = attempt
			return resp, nil
		}
		if resp == nil {
			resp = meta(attempt)
		} else {
			resp.Attempts = attempt
		}
		pe, _ := AsError(err)
		retryable := pe != nil && isRetryable(pe.Class)
		if !retryable || attempt == policy.MaxAttempts {
			if retryable && attempt > 1 {
				// 仅当确实发生过重试（attempt>1）才改判 retry_exhausted；
				// MaxAttempts=1 时保留 timeout 等原始分类，结论才可解释。
				return resp, wrapError(ErrClassRetryExhausted,
					"retry attempts exhausted after "+itoa(attempt)+" attempt(s)", err)
			}
			return resp, err
		}
		if serr := sleepContext(ctx, policy.Backoff); serr != nil {
			return resp, wrapError(ErrClassTimeout, "caller canceled during retry backoff", serr)
		}
	}
	// 理论不可达：循环内必然返回。
	return meta(policy.MaxAttempts),
		wrapError(ErrClassRetryExhausted, "retry attempts exhausted", nil)
}

// itoa 是 strconv.Itoa 的轻量别名，避免再引入导入噪声。
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// invokeOnce 执行单次尝试（不含重试），返回响应与已归类错误。
// 同步路径：超时/取消时等待处理器退出，避免 goroutine 泄漏。
func (iv *Invoker) invokeOnce(ctx context.Context, v *FunctionVersion, req *InvokeRequest, attempt int) (*InvokeResponse, error) {
	return iv.runAttempt(ctx, v, req, attempt, true, nil)
}

// invokeOnceDetached 供异步任务使用的单次尝试：
// 超时/取消时同样强杀实例（不回池、不可复用），但不等待未配合取消的
// 处理器退出，从而任务能在有限时间内到达确定终态。
// onAcquire 在实例实际获取成功（函数即将开始执行）时回调，可为 nil。
func (iv *Invoker) invokeOnceDetached(ctx context.Context, v *FunctionVersion, req *InvokeRequest, attempt int, onAcquire func()) (*InvokeResponse, error) {
	return iv.runAttempt(ctx, v, req, attempt, false, onAcquire)
}

// runAttempt 是单次尝试的公共实现；waitOnAbort 决定终止路径是否等待
// 处理器 goroutine 退出；onAcquire 在成功获取实例后回调。
func (iv *Invoker) runAttempt(ctx context.Context, v *FunctionVersion, req *InvokeRequest, attempt int, waitOnAbort bool, onAcquire func()) (*InvokeResponse, error) {
	in, err := iv.pool.Acquire(v)
	if err != nil {
		return nil, err // concurrency_limited / handler 缺失(not_found) / pool stopped
	}
	if onAcquire != nil {
		onAcquire()
	}

	timeout, _ := effectiveTimeout(v, req)
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if !in.attachCancel(cancel) {
		// 实例在获取与挂载之间被回收：保守按平台错误处理（可重试），
		// 该实例不会回池。
		iv.pool.Abort(in)
		return nil, newError(ErrClassPlatform, "instance was reaped before call started")
	}

	ic := InvokeContext{
		FunctionName:   v.FunctionName,
		Version:        v.Version,
		InstanceID:     in.ID,
		Attempt:        attempt,
		IdempotencyKey: req.IdempotencyKey,
	}

	type result struct {
		out []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, rerr := in.run(execCtx, ic, req.Payload)
		done <- result{out, rerr}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		// 硬超时：强杀实例（不回池、不可复用），按 timeout 归类。
		iv.pool.Abort(in)
		if waitOnAbort {
			<-done // 等待被取消的处理器退出，避免 goroutine 泄漏
		}
		return nil, newError(ErrClassTimeout,
			"function execution exceeded timeout "+timeout.String())
	case <-ctx.Done():
		// 调用方取消：同样终止实例，结论与超时路径一致可解释。
		iv.pool.Abort(in)
		if waitOnAbort {
			<-done
		}
		return nil, wrapError(ErrClassTimeout, "caller canceled function execution", ctx.Err())
	case res := <-done:
		cancel()
		if res.err != nil {
			if pe, ok := AsError(res.err); ok {
				// panic 恢复得到的 platform_error：实例视为被污染，强杀不回池。
				if pe.Class == ErrClassPlatform {
					iv.pool.Abort(in)
					return nil, res.err
				}
				iv.pool.Release(in)
				return nil, res.err // function_error
			}
			// handler 返回的未分类 error（例如 ctx 超时后返回 ctx.Err）：
			// 若执行上下文确实到期则归为 timeout，并强杀实例。
			if execCtx.Err() == context.DeadlineExceeded {
				iv.pool.Abort(in)
				return nil, newError(ErrClassTimeout,
					"function execution exceeded timeout "+timeout.String())
			}
			iv.pool.Release(in)
			return nil, wrapError(ErrClassFunction, "function returned an error", res.err)
		}
		iv.pool.Release(in)
		return &InvokeResponse{
			FunctionName: v.FunctionName,
			Version:      v.Version,
			InstanceID:   in.ID,
			Payload:      res.out,
		}, nil
	}
}

// isRetryable 判断错误类别是否在重试策略覆盖范围内：
// 超时、并发上限拒绝、平台内部错误可重试；其余确定性失败不重试。
func isRetryable(class ErrorClass) bool {
	switch class {
	case ErrClassTimeout, ErrClassConcurrency, ErrClassPlatform:
		return true
	default:
		return false
	}
}

// sleepContext 在 ctx 上可被取消地休眠。
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
