package faas

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// AsyncConfig 控制异步任务链路的排队与调度行为。
type AsyncConfig struct {
	// MaxQueuedPerVersion 每个函数版本允许排队等待的任务上限（不含在执）。
	// 队列已满的新提交立即得到 concurrency_limited 拒绝结论，绝不静默丢弃。
	// <=0 时取默认值。
	MaxQueuedPerVersion int
	// DispatchInterval 调度扫描间隔；<=0 时取默认值。
	DispatchInterval time.Duration
}

// DefaultAsyncConfig 返回异步链路的默认配置。
func DefaultAsyncConfig() AsyncConfig {
	return AsyncConfig{MaxQueuedPerVersion: 64, DispatchInterval: 5 * time.Millisecond}
}

// AsyncManager 承载异步任务的提交、去重、排队、后台执行、
// 截止时间强终态、取消与实例回收。所有任务状态只允许单向推进。
type AsyncManager struct {
	registry *Registry
	router   *Router
	pool     *InstancePool
	cfg      AsyncConfig

	mu      sync.Mutex
	tasks   map[string]*taskRecord  // 任务凭证 -> 记录
	idem    map[string]string       // 去重域键 -> 任务凭证
	queues  map[versionKey][]string // 版本 -> 排队中任务凭证（FIFO）
	running map[versionKey]int      // 版本 -> 在执任务数
	idGen   uint64

	// wake 用于入队 / 取消后立即唤醒调度循环（与定时器二选一先到）。
	wake       chan struct{}
	baseCtx    context.Context
	baseCancel context.CancelFunc
	stopCh     chan struct{}
	stopped    bool
	wg         sync.WaitGroup
}

// NewAsyncManager 装配异步任务管理器并启动单个调度循环；
// 调用方负责在关闭时调用 Stop。
func NewAsyncManager(reg *Registry, rt *Router, pool *InstancePool, cfg AsyncConfig) *AsyncManager {
	if cfg.MaxQueuedPerVersion <= 0 {
		cfg.MaxQueuedPerVersion = DefaultAsyncConfig().MaxQueuedPerVersion
	}
	if cfg.DispatchInterval <= 0 {
		cfg.DispatchInterval = DefaultAsyncConfig().DispatchInterval
	}
	baseCtx, baseCancel := context.WithCancel(context.Background())
	m := &AsyncManager{
		registry:   reg,
		router:     rt,
		pool:       pool,
		cfg:        cfg,
		tasks:      make(map[string]*taskRecord),
		idem:       make(map[string]string),
		queues:     make(map[versionKey][]string),
		running:    make(map[versionKey]int),
		wake:       make(chan struct{}, 1),
		baseCtx:    baseCtx,
		baseCancel: baseCancel,
		stopCh:     make(chan struct{}),
	}
	m.wg.Add(1)
	go m.dispatchLoop()
	return m
}

// idemScope 构造业务操作标识的去重域：同一函数内同键才算同一业务操作。
func idemScope(functionName, key string) string {
	return functionName + "\x00" + key
}

// resolveTarget 与同步链路一致地解析 (函数, 版本)。
func (m *AsyncManager) resolveTarget(req *InvokeRequest) (*FunctionVersion, error) {
	if req.RoutePath != "" {
		route, err := m.router.Resolve(req.RoutePath)
		if err != nil {
			return nil, err
		}
		return m.registry.ResolveForCall(route.FunctionName, route.VersionPin)
	}
	if req.FunctionName == "" {
		return nil, newError(ErrClassInvalid, "async submit requires route or function")
	}
	return m.registry.ResolveForCall(req.FunctionName, req.VersionPin)
}

// Submit 受理一次异步提交：
//  1. 解析目标版本并校验超时（与同步链路同一套确定规则）；
//  2. 重试策略允许多次尝试时，强制要求业务操作标识，否则明确拒绝
//     （没有去重手段就不允许进入可能重复产生副作用的执行）；
//  3. 同一去重域内的并发/重复提交合并为同一任务，返回同一凭证与同一结论；
//  4. 受版本并发上限约束：超出排队容量时立即拒绝（concurrency_limited），
//     不静默丢弃、不静默降级为同步执行。
func (m *AsyncManager) Submit(req *InvokeRequest) (string, error) {
	v, err := m.resolveTarget(req)
	if err != nil {
		return "", err
	}
	timeout, err := effectiveTimeout(v, req)
	if err != nil {
		return "", err
	}
	if v.Config.Retry.MaxAttempts > 1 && req.IdempotencyKey == "" {
		return "", newError(ErrClassInvalid,
			"async submit to a version whose retry policy allows multiple attempts "+
				"requires an idempotencyKey (business operation id); without a dedup key "+
				"the submission is refused because retries could duplicate side effects")
	}

	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return "", newError(ErrClassPlatform, "async task manager is stopped")
	}

	// 业务操作标识去重：同域内所有提交方共享同一任务与同一份确定结论。
	if req.IdempotencyKey != "" {
		scope := idemScope(v.FunctionName, req.IdempotencyKey)
		if existingID, ok := m.idem[scope]; ok {
			if ex := m.tasks[existingID]; ex != nil {
				if !sameAsyncDedupTarget(ex, v) {
					return "", newError(ErrClassConflict,
						"idempotencyKey already used for a different published version; "+
							"reuse is only allowed against the same target version")
				}
				return existingID, nil
			}
		}
	}

	k := versionKey{v.FunctionName, v.Version}
	// 容量以"在执 + 排队"的总在途数计：后台任务受版本并发上限约束，
	// 超出 (并发上限 + 排队容量) 的提交立即得到可区分的拒绝，绝不静默丢弃。
	capacity := v.Config.MaxConcurrency + m.cfg.MaxQueuedPerVersion
	if m.running[k]+len(m.queues[k]) >= capacity {
		return "", newError(ErrClassConcurrency,
			fmt.Sprintf("async backlog for version %s@v%d is full (running+queued>=%d); submission rejected, not dropped",
				v.FunctionName, v.Version, capacity))
	}

	m.idGen++
	id := fmt.Sprintf("task-%d", m.idGen)
	r := &taskRecord{
		id:             id,
		state:          TaskQueued,
		functionName:   v.FunctionName,
		version:        v.Version,
		payload:        append([]byte(nil), req.Payload...),
		idempotencyKey: req.IdempotencyKey,
		createdAt:      now,
		timeout:        timeout,
	}
	m.tasks[id] = r
	m.queues[k] = append(m.queues[k], id)
	if req.IdempotencyKey != "" {
		m.idem[idemScope(v.FunctionName, req.IdempotencyKey)] = id
	}
	m.signalLocked()
	return id, nil
}

// sameAsyncDedupTarget 判断复用任务的目标版本是否与新提交一致。
func sameAsyncDedupTarget(r *taskRecord, v *FunctionVersion) bool {
	return r.functionName == v.FunctionName && r.version == v.Version
}

// Query 凭凭证查询确定结论；未知凭证返回 not_found。
// 同一凭证可反复查询，终态后永远返回同一份确定结论。
func (m *AsyncManager) Query(taskID string) (*TaskSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.tasks[taskID]
	if !ok {
		return nil, newError(ErrClassNotFound, "unknown task id: "+taskID)
	}
	return r.snapshot(), nil
}

// Cancel 请求撤销任务：
//   - 排队中：立即迁移为 canceled 并从队列摘除（尚未产生任何副作用）；
//   - 执行中：记录取消意图并立刻强杀执行实例，调用方在有限时间内看到 canceled；
//   - 已终态：保持并返回原终态（终态单向、不可回退）。
//
// 返回撤销生效时刻任务的确定快照。
func (m *AsyncManager) Cancel(taskID string) (*TaskSnapshot, error) {
	m.mu.Lock()
	r, ok := m.tasks[taskID]
	if !ok {
		m.mu.Unlock()
		return nil, newError(ErrClassNotFound, "unknown task id: "+taskID)
	}
	if r.state.Terminal() {
		snap := r.snapshot()
		m.mu.Unlock()
		return snap, nil
	}
	var abortInstance *Instance
	if r.state == TaskQueued {
		transitionTask(r, TaskCanceled)
		r.finishedAt = time.Now()
		r.errorClass = ErrClassTimeout
		r.errorReason = "task canceled by caller while queued"
		removeQueued(m.queues, versionKey{r.functionName, r.version}, r.id)
	} else { // running
		// 立刻给出确定终态并强杀：无论函数是否配合取消，查询方都在本临界区后
		// 看到 canceled；执行 goroutine 稍后退出时只做实例清理，不能再改结论。
		abortInstance = m.terminateRunningLocked(r, TaskCanceled,
			"task canceled by caller; execution instance reclaimed")
	}
	snap := r.snapshot()
	m.mu.Unlock()

	if abortInstance != nil {
		m.pool.Abort(abortInstance)
	}
	return snap, nil
}

// removeQueued 从版本队列中删除指定任务凭证，保持其余顺序不变。
func removeQueued(queues map[versionKey][]string, k versionKey, id string) {
	q := queues[k]
	for i, qid := range q {
		if qid == id {
			queues[k] = append(q[:i], q[i+1:]...)
			return
		}
	}
}

// signalLocked 唤醒调度循环（非阻塞，缓冲通道合并多余信号）。
func (m *AsyncManager) signalLocked() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// dispatchLoop 是唯一的调度协程：把排队中的任务在版本并发额度内启动。
// 异步与同步共用同一实例池，因此版本 MaxConcurrency 对两类调用统一生效。
func (m *AsyncManager) dispatchLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.DispatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.baseCtx.Done():
			return
		case <-m.wake:
		case <-ticker.C:
		}
		m.pump()
	}
}

// pump 执行一轮尽力调度。每个任务都"原子地"从 queued 迁移为 running 并占用实例：
// 池在持池锁的回调内完成并发闸门判定，回调再取管理器锁做状态迁移与记账，
// 因此不存在"已出队/已占名额但尚未计入在执"的窗口，排队容量门禁始终准确。
//
// 锁序固定为 pool.mu -> AsyncManager.mu（仅此处回调跨两把锁）；
// Cancel/Stop 都是先在 m.mu 内改状态、释放后才调用 pool，故不构成环。
func (m *AsyncManager) pump() {
	for {
		m.mu.Lock()
		if m.stopped {
			m.mu.Unlock()
			return
		}
		type cand struct {
			id string
			k  versionKey
		}
		var picks []cand
		for k, q := range m.queues {
			if len(q) == 0 || m.running[k] >= capOf(m.registry, k) {
				continue
			}
			// 本轮把每个版本队首作为候选；真正是否拿得到实例由池闸门原子判定。
			picks = append(picks, cand{id: q[0], k: k})
		}
		m.mu.Unlock()
		if len(picks) == 0 {
			return
		}

		progressed := false
		for _, c := range picks {
			v, vErr := m.registry.GetVersion(c.k.function, c.k.version)
			if vErr != nil {
				m.dropStaleTask(c.id, vErr)
				progressed = true
				continue
			}
			execCtx, cancel := context.WithCancel(m.baseCtx)
			var launched *Instance
			_, acqErr := m.pool.AcquireForRun(v, func(inst *Instance) bool {
				m.mu.Lock()
				defer m.mu.Unlock()
				r := m.tasks[c.id]
				if r == nil || r.state != TaskQueued || m.stopped {
					cancel()
					return false // 已取消/停止：该实例不服务任何任务
				}
				if !transitionTask(r, TaskRunning) {
					cancel()
					return false
				}
				now := time.Now()
				r.startedAt = now
				r.runningSince = now
				r.attempts = 1
				r.instance = inst
				r.instanceID = inst.ID
				r.cancelExec = cancel
				m.queues[c.k] = m.queues[c.k][1:]
				m.running[c.k]++
				launched = inst
				return true
			})
			if acqErr != nil {
				cancel()
				pe, _ := AsError(acqErr)
				if pe != nil && pe.Class == ErrClassConcurrency {
					continue // 同步调用占满槽位：任务保留在队列，下个周期再调度
				}
				// 回调拒绝（任务已取消）或平台错误：分别收敛。
				m.discardCandidate(c.id, acqErr)
				progressed = true
				continue
			}
			progressed = true
			m.wg.Add(1)
			go m.runTask(c.id, c.k, v, launched, execCtx, cancel)
		}
		if !progressed {
			return
		}
	}
}

// capOf 返回版本的并发上限（取不到时给 0，使该版本本轮不参与调度）。
func capOf(reg *Registry, k versionKey) int {
	v, err := reg.GetVersion(k.function, k.version)
	if err != nil {
		return 0
	}
	return v.Config.MaxConcurrency
}

// dropStaleTask 处理版本记录缺失等无法调度的排队任务：直接 failed 终态。
func (m *AsyncManager) dropStaleTask(id string, cause error) {
	m.failTaskFast(id, cause)
	m.signal()
}

// discardCandidate 处理 AcquireForRun 回调拒绝 / 平台错误：
// 若任务仍在排队（回调未迁移），按平台错误失败；已被取消则保持其终态。
func (m *AsyncManager) discardCandidate(id string, cause error) {
	m.mu.Lock()
	r := m.tasks[id]
	stillQueued := r != nil && r.state == TaskQueued
	m.mu.Unlock()
	if stillQueued {
		m.failTaskFast(id, cause)
	}
	m.signal()
}

// signal 是锁外唤醒辅助。
func (m *AsyncManager) signal() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// failTaskFast 让"未能进入 running"的任务进入 failed 终态（如发布产物缺失）。
func (m *AsyncManager) failTaskFast(id string, cause error) {
	pe, ok := AsError(cause)
	class := ErrClassPlatform
	reason := cause.Error()
	if ok {
		class = pe.Class
		reason = pe.Reason
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.tasks[id]
	if r == nil || r.state.Terminal() {
		return
	}
	r.errorClass = class
	r.errorReason = reason
	r.finishedAt = time.Now()
	if r.state == TaskQueued {
		transitionTask(r, TaskFailed) // queued -> failed（启动期确定失败，仍为单向迁移）
	}
}

// runTask 驱动一个已在 running 的任务：硬截止、取消强杀、按策略退避重试、
// 终态结论一次性写入（单向、无半更新）。
func (m *AsyncManager) runTask(id string, k versionKey, v *FunctionVersion, in *Instance,
	execCtx context.Context, cancel context.CancelFunc) {
	defer m.wg.Done()
	defer func() {
		m.mu.Lock()
		m.running[k]--
		m.signalLocked()
		m.mu.Unlock()
		cancel()
	}()

	policy := v.Config.Retry
	var lastErr error
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		// 先建可被撤销/停止打断的上下文（其取消句柄要登记到任务记录），
		// 在锁内取到该尝试的硬截止后再包一层 deadline。
		attemptBase, attemptCancel := context.WithCancel(execCtx)

		// 尝试前在锁内复核撤销 / 停止，并把"当前实例 + 本次取消句柄"登记到记录：
		// 排队后被取消的任务绝不允许再产生副作用；Cancel/Stop 永远杀的是当前实例。
		m.mu.Lock()
		r := m.tasks[id]
		if r == nil || r.state != TaskRunning {
			current := (*Instance)(nil)
			if r != nil {
				current = r.instance
			}
			m.mu.Unlock()
			if current == in {
				m.pool.Abort(in) // 手上实例（可能是重试冷启动的新实例）立即回收
			}
			attemptCancel()
			return
		}
		if !in.attachCancel(attemptCancel) {
			// 实例在获取与挂载之间被回收：保守按平台错误处理，不回池。
			m.mu.Unlock()
			m.pool.Abort(in)
			attemptCancel()
			m.settle(id, TaskFailed, nil, attempt, ErrClassPlatform,
				"instance was reaped before async attempt started")
			return
		}
		attemptCtx, deadlineCancel := context.WithTimeout(attemptBase, r.timeout)
		r.attempts = attempt
		r.instance = in
		r.instanceID = in.ID
		r.cancelExec = attemptCancel
		ic := InvokeContext{
			FunctionName:   v.FunctionName,
			Version:        v.Version,
			InstanceID:     in.ID,
			Attempt:        attempt,
			IdempotencyKey: r.idempotencyKey,
		}
		payload := append([]byte(nil), r.payload...)
		attemptTimeout := r.timeout
		m.mu.Unlock()
		// 统一取消：撤销/截止任一触发都取消执行上下文。
		cancelBoth := func() { deadlineCancel(); attemptCancel() }
		type result struct {
			out []byte
			err error
		}
		done := make(chan result, 1)
		go func(execIn *Instance) {
			out, rerr := execIn.run(attemptCtx, ic, payload)
			done <- (result{out: out, err: rerr})
		}(in)
		timer := time.NewTimer(attemptTimeout)

		select {
		case <-timer.C:
			timer.Stop()
			cancelBoth()
			m.pool.Abort(in)
			in = nil // 实例已终结，后续尝试必须冷启动新实例
			// 截止与撤销同时发生时，已登记的撤销意图优先（结论仍可区分）。
			m.mu.Lock()
			cancelWanted := r.cancelRequested
			m.mu.Unlock()
			if cancelWanted {
				m.settle(id, TaskCanceled, nil, attempt, ErrClassTimeout,
					"task canceled by caller at its deadline; instance reclaimed")
				return
			}
			lastErr = newError(ErrClassTimeout,
				"async function execution exceeded deadline "+attemptTimeout.String())

		case <-execCtx.Done():
			// 调用方撤销或平台 Stop：立即强杀，终态 canceled。
			timer.Stop()
			cancelBoth()
			m.pool.Abort(in)
			m.settle(id, TaskCanceled, nil, attempt, ErrClassTimeout,
				"task canceled by caller; execution instance reclaimed")
			return

		case res := <-done:
			timer.Stop()
			// 结果到达时先在锁内复核撤销意图：取消一旦登记，迟到的成功/失败
			// 结论都必须让位给 canceled，且该实例已在 Cancel 中被强杀，绝不回池。
			m.mu.Lock()
			cancelWanted := r.cancelRequested
			m.mu.Unlock()
			if cancelWanted {
				cancelBoth()
				m.settle(id, TaskCanceled, nil, attempt, ErrClassTimeout,
					"task canceled by caller; late function result discarded")
				return
			}
			if res.err == nil {
				cancelBoth()
				m.settle(id, TaskSucceeded, res.out, attempt, "", "")
				m.pool.Release(in)
				return
			}
			// 分类本次失败，决定实例去留与是否重试。
			class, reason, killInstance := classifyAttemptFailure(res.err, attemptCtx)
			cancelBoth()
			if killInstance {
				m.pool.Abort(in)
				in = nil
			}
			lastErr = newError(class, reason)
			// function_error 等不可重试错误立即终态。
			if !isRetryable(class) {
				m.settle(id, TaskFailed, nil, attempt, class, reason)
				if in != nil {
					m.pool.Release(in)
				}
				return
			}
		}

		// 可重试失败：退避后在新实例上再试；退避可被撤销/停止打断。
		if attempt == policy.MaxAttempts {
			break
		}
		if in == nil {
			// 冷启动下一次尝试的新实例；获取后立即在锁内登记到任务记录并挂载
			// execCtx 的取消句柄，使退避期发生的 Cancel/Stop 能精确强杀该实例。
			newIn, acqErr := m.acquireForRetry(id, k, v)
			if acqErr != nil {
				pe, _ := AsError(acqErr)
				if pe != nil && pe.Class == ErrClassTimeout {
					m.settle(id, TaskCanceled, nil, attempt, ErrClassTimeout,
						"task canceled while preparing a retry instance")
					return
				}
				m.settle(id, TaskFailed, nil, attempt, pe.Class,
					"could not acquire a fresh instance for retry: "+pe.Reason)
				return
			}
			m.mu.Lock()
			r2 := m.tasks[id]
			if r2 == nil || r2.state != TaskRunning {
				m.mu.Unlock()
				m.pool.Abort(newIn) // 获取与登记之间被取消：新实例不得泄漏
				return
			}
			if !newIn.attachCancel(cancel) {
				m.mu.Unlock()
				m.pool.Abort(newIn)
				m.settle(id, TaskFailed, nil, attempt, ErrClassPlatform,
					"retry instance was reaped before backoff")
				return
			}
			r2.instance = newIn
			r2.instanceID = newIn.ID
			r2.cancelExec = cancel
			m.mu.Unlock()
			in = newIn
		}
		bo := policy.Backoff
		if serr := sleepContext(execCtx, bo); serr != nil {
			// 退避期间被撤销 / 停止：回收手上实例并给 canceled 终态。
			if in != nil {
				m.pool.Abort(in)
			}
			m.settle(id, TaskCanceled, nil, attempt, ErrClassTimeout,
				"task canceled during retry backoff; execution instance reclaimed")
			return
		}
	}

	// 终态归类（四类必须可区分：succeeded / function失败 / 超时 / 取消）：
	//   - 末次结论是硬截止超时      -> timed_out（errorClass=timeout，attempts 可见是否重试过）
	//   - 未实际重试的平台类失败    -> failed（保留 platform 等根因类别）
	//   - 实际重试过仍失败          -> failed(retry_exhausted)，原因保留末次根因
	finalAttempt := policy.MaxAttempts
	pe, _ := AsError(lastErr)
	if pe != nil && pe.Class == ErrClassTimeout {
		m.settle(id, TaskTimedOut, nil, finalAttempt, ErrClassTimeout, pe.Reason)
		return
	}
	class, reason := ErrClassPlatform, "retry attempts exhausted"
	if pe != nil {
		if finalAttempt <= 1 {
			class = pe.Class
			reason = pe.Reason
		} else {
			class = ErrClassRetryExhausted
			reason = "retry attempts exhausted; last error: " + string(pe.Class) + ": " + pe.Reason
		}
	}
	m.settle(id, TaskFailed, nil, finalAttempt, class, reason)
}

// acquireForRetry 在重试间隔后为任务获取新实例；期间若任务已被取消则返回错误。
func (m *AsyncManager) acquireForRetry(id string, k versionKey, v *FunctionVersion) (*Instance, error) {
	for {
		m.mu.Lock()
		r := m.tasks[id]
		if r == nil || r.state != TaskRunning || r.cancelRequested {
			m.mu.Unlock()
			return nil, newError(ErrClassTimeout, "task no longer running while preparing retry")
		}
		m.mu.Unlock()
		in, err := m.pool.Acquire(v)
		if err == nil {
			return in, nil
		}
		pe, _ := AsError(err)
		if pe == nil || pe.Class != ErrClassConcurrency {
			return nil, err
		}
		if serr := sleepContext(m.baseCtx, m.cfg.DispatchInterval); serr != nil {
			return nil, newError(ErrClassTimeout, "canceled while waiting for a retry slot")
		}
	}
}

// classifyAttemptFailure 与同步链路保持同一套失败分类与实例污染判定。
func classifyAttemptFailure(err error, attemptCtx context.Context) (class ErrorClass, reason string, kill bool) {
	if pe, ok := AsError(err); ok {
		if pe.Class == ErrClassPlatform {
			// panic（模拟进程崩溃）：实例被污染，强杀不回池。
			return ErrClassPlatform, pe.Reason, true
		}
		if pe.Class == ErrClassFunction {
			return ErrClassFunction, pe.Reason, false
		}
		return pe.Class, pe.Reason, pe.Class == ErrClassPlatform
	}
	if attemptCtx.Err() == context.DeadlineExceeded {
		return ErrClassTimeout, "function execution exceeded the per-attempt deadline", true
	}
	return ErrClassFunction, "function returned an error: " + err.Error(), false
}

// terminateRunningLocked 在已持锁前提下把一个 running 任务推进到取消类终态
// （canceled 或 timed_out），登记撤销意图、取消执行上下文并返回需要在锁外强杀的实例。
// 状态迁移是单向的；若任务已终态则不再改动（竞速时先到者获胜）。
func (m *AsyncManager) terminateRunningLocked(r *taskRecord, target TaskState, reason string) *Instance {
	if r.state.Terminal() {
		return nil
	}
	r.cancelRequested = true
	if !transitionTask(r, target) {
		return nil
	}
	r.errorClass = ErrClassTimeout
	r.errorReason = reason
	r.finishedAt = time.Now()
	inst := r.instance
	if r.cancelExec != nil {
		r.cancelExec()
	}
	return inst
}

// settle 是终态的唯一提交点：在管理器锁内做一次性单向迁移与结论写入，
// 终态之后到达的任何迟到结果（包括函数返回成功）都被直接丢弃，杜绝回退/半更新。
func (m *AsyncManager) settle(id string, target TaskState, payload []byte, attempts int,
	class ErrorClass, reason string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.tasks[id]
	if r == nil {
		return false
	}
	if r.state.Terminal() {
		// 取消/超时/完成三者竞速：先到的终态获胜，其余结论一律忽略。
		return false
	}
	if !transitionTask(r, target) {
		return false
	}
	r.attempts = attempts
	if target == TaskSucceeded {
		r.payload = append([]byte(nil), payload...)
	}
	r.errorClass = class
	r.errorReason = reason
	r.finishedAt = time.Now()
	r.instance = nil
	r.cancelExec = nil
	return true
}

// Stop 停止调度并取消全部未终态任务、回收其执行实例。幂等。
func (m *AsyncManager) Stop() {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		m.wg.Wait()
		return
	}
	m.stopped = true
	close(m.stopCh)
	var busyInstances []*Instance
	for _, r := range m.tasks {
		switch r.state {
		case TaskQueued:
			transitionTask(r, TaskCanceled)
			r.finishedAt = time.Now()
			r.errorClass = ErrClassTimeout
			r.errorReason = "task canceled because platform is shutting down"
		case TaskRunning:
			if inst := m.terminateRunningLocked(r, TaskCanceled,
				"task canceled because platform is shutting down"); inst != nil {
				busyInstances = append(busyInstances, inst)
			}
		}
	}
	m.queues = map[versionKey][]string{}
	m.mu.Unlock()

	// 先取消所有执行上下文并强杀忙碌实例，再等待执行协程退出。
	m.baseCancel()
	for _, in := range busyInstances {
		m.pool.Abort(in)
	}
	m.wg.Wait()
}
