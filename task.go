package faas

import (
	"context"
	"time"
)

// TaskStatus 是异步任务的生命周期状态。
// 状态只能单向推进：queued -> running -> {succeeded|failed|timed_out}，
// queued/running -> canceled；终态不可再迁移（不回退、不跳变、不半更新）。
type TaskStatus string

const (
	// TaskQueued 已受理，等待并发槽位或首次尝试。
	TaskQueued TaskStatus = "queued"
	// TaskRunning 正在执行（含尝试间退避）。
	TaskRunning TaskStatus = "running"
	// TaskSucceeded 执行成功（终态）。
	TaskSucceeded TaskStatus = "succeeded"
	// TaskFailed 失败终态：函数自身错误或平台类错误耗尽重试。
	TaskFailed TaskStatus = "failed"
	// TaskTimedOut 达到单次执行截止时间且重试耗尽（终态）。
	TaskTimedOut TaskStatus = "timed_out"
	// TaskCanceled 调用方撤销或平台关停（终态）。
	TaskCanceled TaskStatus = "canceled"
)

// IsTerminal 报告状态是否为终态。终态不可逆。
func (s TaskStatus) IsTerminal() bool {
	switch s {
	case TaskSucceeded, TaskFailed, TaskTimedOut, TaskCanceled:
		return true
	default:
		return false
	}
}

// task 是平台内部的任务记录；全部可变字段仅在 TaskManager.mu 下读写，
// 执行协程与查询/取消路径通过同一把锁保证无半更新。
type task struct {
	id      string
	opKey   string           // 业务操作标识（幂等键），可为空
	req     *InvokeRequest   // 提交时的规范化请求（不可变）
	version *FunctionVersion // 提交时解析的版本快照（配置不可变）

	status        TaskStatus
	resp          *InvokeResponse
	errClass      ErrorClass
	errReason     string
	attempts      int
	submittedAt   time.Time
	finishedAt    time.Time
	cancelReqFlag bool
	cancel        context.CancelFunc
	done          chan struct{} // 进入终态时关闭
}

// TaskSnapshot 是任务在某时刻的不可变视图；同一任务终态后的任意次查询
// 返回内容完全一致。
type TaskSnapshot struct {
	TaskID       string
	Status       TaskStatus
	FunctionName string
	Version      int
	Attempts     int
	Payload      []byte // 仅成功时携带
	ErrorClass   string // 终态失败时的错误类别（含 canceled）
	ErrorReason  string
	SubmittedAt  time.Time
	FinishedAt   time.Time // 非终态为零值
}

// snapshot 拷贝当前视图；调用方必须持有 TaskManager.mu。
func (t *task) snapshot() *TaskSnapshot {
	s := &TaskSnapshot{
		TaskID:       t.id,
		Status:       t.status,
		FunctionName: t.version.FunctionName,
		Version:      t.version.Version,
		Attempts:     t.attempts,
		SubmittedAt:  t.submittedAt,
		FinishedAt:   t.finishedAt,
	}
	if t.resp != nil {
		s.Payload = append([]byte(nil), t.resp.Payload...)
	}
	if t.errClass != "" {
		s.ErrorClass = string(t.errClass)
		s.ErrorReason = t.errReason
	}
	return s
}
