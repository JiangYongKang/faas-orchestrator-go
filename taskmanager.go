package faas

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// TaskManagerConfig 控制异步任务调度行为。
type TaskManagerConfig struct {
	// MaxQueued 允许同时在途（queued+running）的任务数；超出即拒绝提交。
	MaxQueued int
	// QueuePollInterval 并发槽位不足时排队等待的轮询间隔。
	QueuePollInterval time.Duration
}

// DefaultTaskManagerConfig 返回默认调度配置。
func DefaultTaskManagerConfig() TaskManagerConfig {
	return TaskManagerConfig{MaxQueued: 64, QueuePollInterval: 5 * time.Millisecond}
}

// TaskManager 负责异步任务的受理、去重、后台执行、取消与查询。
//
// 核心保证：
//   - 提交即得稳定凭证，执行在后台推进；同一凭证反复查询结论一致；
//   - 状态单向推进（queued -> running -> 终态，或 -> canceled），终态不可变；
//   - 同一业务操作标识（幂等键）的并发/重复提交合并为同一任务，
//     一次提交内部的多次尝试共享同一幂等键，副作用最多一次；
//   - 到达单次执行截止时间或被撤销时，无论函数是否配合取消，
//     任务都在有限时间内到达确定终态，占用实例被强杀回收、绝不复用；
//   - 后台执行仍受版本并发上限约束：槽位不足时任务处于可观测的
//     queued 状态排队，在途任务数超上限时提交被显式拒绝，绝不静默丢弃。
type TaskManager struct {
	cfg     TaskManagerConfig
	invoker *Invoker

	mu       sync.Mutex
	tasks    map[string]*task
	byOp     map[string]string // 业务操作标识 -> 任务 ID
	inflight int               // queued+running 任务数
	idGen    uint64
	stopped  bool
	wg       sync.WaitGroup
}

// NewTaskManager 创建任务管理器。
func NewTaskManager(inv *Invoker, cfg TaskManagerConfig) *TaskManager {
	if cfg.MaxQueued <= 0 {
		cfg.MaxQueued = DefaultTaskManagerConfig().MaxQueued
	}
	if cfg.QueuePollInterval <= 0 {
		cfg.QueuePollInterval = DefaultTaskManagerConfig().QueuePollInterval
	}
	return &TaskManager{
		cfg:     cfg,
		invoker: inv,
		tasks:   make(map[string]*task),
		byOp:    make(map[string]string),
	}
}

// Submit 受理一次异步任务提交，立即返回任务凭证。
//
// 拒绝路径（均可区分，绝不静默降级为同步执行）：
//   - 目标解析失败：not_found / unavailable（与同步调用一致）；
//   - timeoutMs 超过版本配置：invalid_request；
//   - 未知选项：invalid_request（不静默忽略）；
//   - 版本重试策略允许多次尝试但未提供幂等键：invalid_request
//     （多次尝试可能重复产生副作用，必须可用业务标识去重）；
//   - 在途任务数达到上限：concurrency_limited（显式拒绝，不静默丢弃）。
//
// 幂等键非空时的重复/并发提交：返回已存在的同一任务凭证，
// 所有提交方看到同一个确定结论，副作用最多发生一次。
func (tm *TaskManager) Submit(req *InvokeRequest) (*TaskSnapshot, error) {
	v, err := tm.invoker.resolveTarget(req)
	if err != nil {
		return nil, err
	}
	if _, err := effectiveTimeout(v, req); err != nil {
		return nil, err
	}
	for k := range req.Options {
		if !v2AllowedOptions[k] {
			return nil, newError(ErrClassInvalid,
				fmt.Sprintf("unsupported option %q (not silently ignored)", k))
		}
	}
	if v.Config.Retry.MaxAttempts > 1 && req.IdempotencyKey == "" {
		return nil, newError(ErrClassInvalid,
			"async submit requires idempotencyKey: retry policy allows multiple attempts, "+
				"retries could duplicate side effects without a dedup key")
	}

	tm.mu.Lock()
	if tm.stopped {
		tm.mu.Unlock()
		return nil, newError(ErrClassPlatform, "task manager is stopped")
	}
	if req.IdempotencyKey != "" {
		if id, ok := tm.byOp[req.IdempotencyKey]; ok {
			// 重复提交：同一业务操作只存在一个任务，返回同一凭证。
			snap := tm.tasks[id].snapshot()
			tm.mu.Unlock()
			return snap, nil
		}
	}
	if tm.inflight >= tm.cfg.MaxQueued {
		tm.mu.Unlock()
		return nil, newError(ErrClassConcurrency,
			fmt.Sprintf("task queue full: %d task(s) in flight (limit %d)",
				tm.inflight, tm.cfg.MaxQueued))
	}
	tm.idGen++
	t := &task{
		id:          fmt.Sprintf("task-%d", tm.idGen),
		opKey:       req.IdempotencyKey,
		req:         req,
		version:     v.Clone(),
		status:      TaskQueued,
		submittedAt: time.Now(),
		done:        make(chan struct{}),
	}
	tm.tasks[t.id] = t
	if t.opKey != "" {
		tm.byOp[t.opKey] = t.id
	}
	tm.inflight++
	tm.wg.Add(1)
	go tm.runTask(t)
	snap := t.snapshot()
	tm.mu.Unlock()
	return snap, nil
}

// Query 按任务凭证查询当前确定结论；未知凭证返回 not_found。
// 终态后的任意次查询返回内容完全一致。
func (tm *TaskManager) Query(taskID string) (*TaskSnapshot, error) {
	if taskID == "" {
		return nil, newError(ErrClassInvalid, "taskId must not be empty")
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	t, ok := tm.tasks[taskID]
	if !ok {
		return nil, newError(ErrClassNotFound, "unknown task id: "+taskID)
	}
	return t.snapshot(), nil
}

// Cancel 撤销一次提交：
//   - queued：立即转入 canceled 终态；
//   - running：标记撤销并取消执行上下文，执行协程在有限时间内
//     强杀实例并落定 canceled 终态（不等待未配合取消的处理器）；
//   - 已终态：幂等返回当前结论，状态不再变化。
func (tm *TaskManager) Cancel(taskID string) (*TaskSnapshot, error) {
	if taskID == "" {
		return nil, newError(ErrClassInvalid, "taskId must not be empty")
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	t, ok := tm.tasks[taskID]
	if !ok {
		return nil, newError(ErrClassNotFound, "unknown task id: "+taskID)
	}
	switch t.status {
	case TaskQueued:
		// 置位撤销标记并取消执行上下文：执行协程可能正处于排队轮询、
		// 也可能与本撤销竞争并发槽位；标记保证任何路径都不会再开始执行。
		t.cancelReqFlag = true
		if t.cancel != nil {
			t.cancel()
		}
		tm.finishLocked(t, TaskCanceled, nil, ErrClassCanceled,
			"task canceled before execution started")
	case TaskRunning:
		t.cancelReqFlag = true
		if t.cancel != nil {
			t.cancel()
		}
	}
	return t.snapshot(), nil
}

// Stop 取消全部在途任务并等待执行协程退出；之后的提交被拒绝。
func (tm *TaskManager) Stop() {
	tm.mu.Lock()
	if tm.stopped {
		tm.mu.Unlock()
		tm.wg.Wait()
		return
	}
	tm.stopped = true
	for _, t := range tm.tasks {
		switch t.status {
		case TaskQueued:
			t.cancelReqFlag = true
			if t.cancel != nil {
				t.cancel()
			}
			tm.finishLocked(t, TaskCanceled, nil, ErrClassCanceled,
				"task manager stopped before execution started")
		case TaskRunning:
			t.cancelReqFlag = true
			if t.cancel != nil {
				t.cancel()
			}
		}
	}
	tm.mu.Unlock()
	tm.wg.Wait()
}

// finishLocked 在锁内把任务落定到终态；终态不可逆，重复落定被忽略。
// 调用方必须持有 tm.mu。
func (tm *TaskManager) finishLocked(t *task, status TaskStatus, resp *InvokeResponse, class ErrorClass, reason string) {
	if t.status.IsTerminal() {
		return // 终态不可变：杜绝回退、跳变与半更新
	}
	t.status = status
	t.resp = resp
	t.errClass = class
	t.errReason = reason
	t.finishedAt = time.Now()
	close(t.done)
}

// cancelRequested 读取撤销标记。
func (tm *TaskManager) cancelRequested(t *task) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return t.cancelReqFlag
}

// beginRunningLocked 在锁内尝试把任务从 queued 原子认领为 running。
// 已被撤销（或任何非 queued 状态）时返回 false：抢到槽位但认领失败的
// 调用必须放弃该槽位、绝不启动函数。调用方必须持有 tm.mu。
func (tm *TaskManager) beginRunningLocked(t *task) bool {
	if t.status != TaskQueued {
		return false
	}
	t.status = TaskRunning
	return true
}

// runTask 是任务的后台执行协程：负责状态推进、按策略重试与终态归类。
func (tm *TaskManager) runTask(t *task) {
	defer tm.wg.Done()
	defer func() {
		tm.mu.Lock()
		tm.inflight--
		tm.mu.Unlock()
	}()

	tm.mu.Lock()
	if t.status != TaskQueued {
		tm.mu.Unlock()
		return // 提交后、启动前已被取消/关停
	}
	// 取消句柄在排队等待并发槽位期间就就位，保证撤销在有限时间内生效；
	// queued -> running 的迁移发生在槽位就绪、最终认领成功时
	// （见 invokeAttemptQueued 的 claimForAttempt）。
	ctx, cancel := context.WithCancel(context.Background())
	t.cancel = cancel
	tm.mu.Unlock()
	defer cancel()

	v := t.version
	policy := v.Config.Retry
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if tm.cancelRequested(t) {
			tm.finishCanceled(t, attempt-1, "task canceled by caller")
			return
		}
		resp, started, err := tm.invokeAttemptQueued(ctx, t, v, attempt)
		if tm.cancelRequested(t) {
			// 已真正启动处理器的尝试计入 attempts；排队中撤销、从未启动的不计入。
			n := attempt - 1
			if started {
				n = attempt
			}
			tm.finishCanceled(t, n, "task canceled by caller")
			return
		}
		if pe, _ := AsError(err); pe != nil && pe.Class == ErrClassCanceled {
			// 尝试在启动处理器前因撤销/关停被放弃：attempts 不增（从未执行）。
			tm.finishCanceled(t, attempt-1, pe.Reason)
			return
		}
		if err == nil {
			tm.mu.Lock()
			t.attempts = attempt
			tm.finishLocked(t, TaskSucceeded, resp, "", "")
			tm.mu.Unlock()
			return
		}
		pe, _ := AsError(err)
		retryable := pe != nil && isRetryable(pe.Class)
		if !retryable || attempt == policy.MaxAttempts {
			tm.finishFailed(t, attempt, resp, err)
			return
		}
		if serr := sleepContext(ctx, policy.Backoff); serr != nil {
			// 退避中被取消（execCtx 仅由撤销/关停触发）。
			tm.finishCanceled(t, attempt, "task canceled during retry backoff")
			return
		}
	}
}

// finishCanceled 落定 canceled 终态。
func (tm *TaskManager) finishCanceled(t *task, attempts int, reason string) {
	tm.mu.Lock()
	t.attempts = attempts
	tm.finishLocked(t, TaskCanceled, nil, ErrClassCanceled, reason)
	tm.mu.Unlock()
}

// finishFailed 按末次错误类别落定失败终态：
// timeout -> timed_out；其余（function_error / platform_error 等）-> failed，
// 原始错误类别保留在快照中，结论可区分、可解释。
func (tm *TaskManager) finishFailed(t *task, attempts int, resp *InvokeResponse, err error) {
	pe, ok := AsError(err)
	class := ErrClassPlatform
	reason := err.Error()
	if ok {
		class = pe.Class
		reason = pe.Reason
	}
	status := TaskFailed
	if class == ErrClassTimeout {
		status = TaskTimedOut
	}
	tm.mu.Lock()
	t.attempts = attempts
	tm.finishLocked(t, status, resp, class, reason)
	tm.mu.Unlock()
}

// claimForAttempt 在锁内确认任务仍可执行（未撤销、未终态）。
// begin=true 且任务仍为 queued 时，把状态原子推进到 running（仅在槽位就绪、
// 即将 Acquire 时发生一次）；begin=false 只做排队期间的可执行性复查，
// 不改变状态。返回 false 表示任务已失去执行资格——调用方绝不能去抢占槽位。
func (tm *TaskManager) claimForAttempt(t *task, begin bool) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if t.cancelReqFlag || t.status.IsTerminal() {
		return false
	}
	if begin {
		// queued -> running 仅发生一次；重试时已是 running，直接确认即可。
		return tm.beginRunningLocked(t) || t.status == TaskRunning
	}
	return t.status == TaskQueued || t.status == TaskRunning
}

// waitForSlot 探测并发槽位是否可用（不实际占用）；不可用时在 queued/running
// 语义下轮询等待（不消耗尝试次数、不静默丢弃），直到槽位腾出或任务被撤销。
func (tm *TaskManager) waitForSlot(ctx context.Context, t *task, v *FunctionVersion) error {
	for {
		if tm.invoker.pool.HasAvailableSlot(v) {
			return nil
		}
		if serr := sleepContext(ctx, tm.cfg.QueuePollInterval); serr != nil {
			return wrapError(ErrClassCanceled, "task canceled while queued for a concurrency slot", serr)
		}
	}
}

// invokeAttemptQueued 执行一次尝试，返回 (响应, 本次是否真正启动过处理器, 错误)。
// started 只在成功 Acquire 并进入 invokeOnceDetached 后为 true——
// 排队等待、槽位竞态导致的认领放弃都不算"执行过"。
//
// 执行流程：
//  1. 先在 TaskManager 锁内复查执行资格（与撤销线性化，已撤销立即退出，
//     状态保持 queued，连 Acquire 都不会发生，因而绝不占用实例）；
//  2. 再探测并发槽位，不足则停留在 queued 轮询等待；
//  3. 真正 Acquire 之前做最后一次原子认领：queued -> running 与撤销在
//     同一把锁上线性化，封死"槽位腾出与撤销同时发生"的竞态窗口；
//     认领成功后立即 Acquire 并启动执行。
//
// 重试时任务已是 running，步骤 3 的迁移自然为空操作。
func (tm *TaskManager) invokeAttemptQueued(ctx context.Context, t *task, v *FunctionVersion, attempt int) (*InvokeResponse, bool, error) {
	if !tm.claimForAttempt(t, false) {
		return nil, false, newError(ErrClassCanceled, "task canceled before execution started")
	}
	if err := tm.waitForSlot(ctx, t, v); err != nil {
		return nil, false, err
	}
	// 槽位就绪：最终认领（queued -> running 原子发生）；
	// 若撤销先于本临界区落定，则认领失败，绝不 Acquire。
	if !tm.claimForAttempt(t, true) {
		return nil, false, newError(ErrClassCanceled, "task canceled before execution started")
	}
	resp, err := tm.invoker.invokeOnceDetached(ctx, v, t.req, attempt)
	return resp, true, err
}
