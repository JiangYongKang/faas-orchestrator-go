package faas

import (
	"context"
	"time"
)

// TaskState 是异步任务状态机中的单一状态取值。
//
// 合法迁移（单向推进，禁止回退/跳变）：
//
//	queued  -> running | canceled
//	running -> succeeded | failed | timed_out | canceled
//	succeeded / failed / timed_out / canceled 为终态，永不再迁移。
type TaskState string

const (
	// TaskQueued 已受理，等待并发槽位（排队中可被查询/取消）。
	TaskQueued TaskState = "queued"
	// TaskRunning 已获得实例，后台执行中。
	TaskRunning TaskState = "running"
	// TaskSucceeded 终态：函数成功返回。
	TaskSucceeded TaskState = "succeeded"
	// TaskFailed 终态：函数自身失败，或可重试错误耗尽（结论中保留 errorClass）。
	TaskFailed TaskState = "failed"
	// TaskTimedOut 终态：到达单次执行硬截止时间，实例已被强制回收。
	TaskTimedOut TaskState = "timed_out"
	// TaskCanceled 终态：调用方主动撤销，执行实例已被强制回收。
	TaskCanceled TaskState = "canceled"
)

// Terminal 报告状态是否为不可再迁移的终态。
func (s TaskState) Terminal() bool {
	switch s {
	case TaskSucceeded, TaskFailed, TaskTimedOut, TaskCanceled:
		return true
	default:
		return false
	}
}

// TaskSnapshot 是任务记录对外的不可变快照；同一凭证反复查询得到的内容确定一致。
type TaskSnapshot struct {
	ID             string
	State          TaskState
	FunctionName   string
	Version        int
	Attempts       int
	InstanceID     string
	Payload        []byte
	ErrorClass     ErrorClass
	ErrorReason    string
	IdempotencyKey string
	CreatedAt      time.Time
	StartedAt      time.Time
	FinishedAt     time.Time
}

// taskRecord 是异步任务的内部记录；所有字段仅在 AsyncManager.mu 下读写，
// 状态迁移与结论写入在同一临界区完成，杜绝半更新。
type taskRecord struct {
	id             string
	state          TaskState
	functionName   string
	version        int
	attempts       int
	instanceID     string
	payload        []byte
	errorClass     ErrorClass
	errorReason    string
	idempotencyKey string
	createdAt      time.Time
	startedAt      time.Time
	finishedAt     time.Time

	// 以下执行期字段仅在 AsyncManager.mu 下访问。
	timeout         time.Duration      // 单次尝试硬截止
	instance        *Instance          // running 时占用的实例（终态时必须已回收）
	cancelExec      context.CancelFunc // running 时的执行取消句柄（供 cancel/stop 强杀）
	cancelRequested bool               // 调用方撤销意图已登记：此后任何迟到结论一律归 canceled
	runningSince    time.Time          // 进入 running 的时刻（诊断用）
}

// snapshot 在持锁前提下拷贝出对外快照。
func (r *taskRecord) snapshot() *TaskSnapshot {
	return &TaskSnapshot{
		ID:             r.id,
		State:          r.state,
		FunctionName:   r.functionName,
		Version:        r.version,
		Attempts:       r.attempts,
		InstanceID:     r.instanceID,
		Payload:        append([]byte(nil), r.payload...),
		ErrorClass:     r.errorClass,
		ErrorReason:    r.errorReason,
		IdempotencyKey: r.idempotencyKey,
		CreatedAt:      r.createdAt,
		StartedAt:      r.startedAt,
		FinishedAt:     r.finishedAt,
	}
}

// transitionTask 校验并执行单向状态迁移；非法迁移返回 false。
// 合法迁移只有：
//
//	queued  -> running | failed | canceled
//	running -> succeeded | failed | timed_out | canceled
//
// queued -> failed 仅用于"任务尚未获得实例即被确定性失败拦截"（如发布产物缺失），
// 终态不允许任何迁移，因此不可能回退、跳变或被终态后的迟到结论覆盖。
func transitionTask(r *taskRecord, next TaskState) bool {
	switch r.state {
	case TaskQueued:
		if next == TaskRunning || next == TaskFailed || next == TaskCanceled {
			r.state = next
			return true
		}
	case TaskRunning:
		switch next {
		case TaskSucceeded, TaskFailed, TaskTimedOut, TaskCanceled:
			r.state = next
			return true
		}
	}
	return false
}
