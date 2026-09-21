package faas

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// InstanceState 是实例生命周期状态。
type InstanceState string

const (
	InstanceIdle       InstanceState = "idle"       // 空闲，可接收调用
	InstanceBusy       InstanceState = "busy"       // 正在执行调用
	InstanceDraining   InstanceState = "draining"   // 回收中：执行完手头调用后终结，不再接新调用
	InstanceTerminated InstanceState = "terminated" // 已终结，进程已"退出"
)

// Instance 模拟一个承载某函数版本的本地进程。
type Instance struct {
	ID           string
	FunctionName string
	Version      int
	handler      Handler

	mu         sync.Mutex
	state      InstanceState
	createdAt  time.Time
	lastUsedAt time.Time
	// busy 期间用于强制超时终止的取消句柄。
	cancelExec context.CancelFunc
	// busy 时收到回收请求则置位；调用结束后实例转入 draining 而非 idle。
	drainRequested bool
}

// State 返回当前状态（加锁读取）。
func (in *Instance) State() InstanceState {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.state
}

// LastUsedAt 返回最近一次释放回池的时间。
func (in *Instance) LastUsedAt() time.Time {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.lastUsedAt
}

// tryBeginCall 尝试把 idle 实例转为 busy 并登记取消句柄。
// draining / terminated / busy 状态返回 false，保证回收中的实例不接新调用。
func (in *Instance) tryBeginCall(cancel context.CancelFunc) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.state != InstanceIdle {
		return false
	}
	in.state = InstanceBusy
	in.cancelExec = cancel
	in.drainRequested = false
	return true
}

// attachCancel 在 busy 实例上登记本次执行的取消句柄（供平台超时强杀使用）。
// 仅在 busy 状态下生效，其余状态说明实例已被回收路径接管。
func (in *Instance) attachCancel(cancel context.CancelFunc) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.state != InstanceBusy {
		return false
	}
	in.cancelExec = cancel
	return true
}

// finishCall 把 busy 实例结束执行：
// 正常情况下恢复 idle 可复用；若 busy 期间已被请求回收，则转入 draining。
// 非 busy 状态调用是非法使用（返回 false），由池按平台错误处理。
func (in *Instance) finishCall() bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.state != InstanceBusy {
		return false
	}
	in.cancelExec = nil
	if in.drainRequested {
		in.state = InstanceDraining
	} else {
		in.state = InstanceIdle
		in.lastUsedAt = time.Now()
	}
	return true
}

// markDraining 请求回收：idle -> draining（可立即终结），
// busy -> 登记回收请求（等当前调用结束后转 draining）。
// terminated 或已在 draining 返回 false。
func (in *Instance) markDraining() bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	switch in.state {
	case InstanceIdle:
		in.state = InstanceDraining
		return true
	case InstanceBusy:
		if in.drainRequested {
			return false
		}
		in.drainRequested = true
		return true
	default:
		return false
	}
}

// terminate 终结实例；仅允许在 idle/draining 下终结（busy 必须先走强杀路径）。
// 成功返回 true；重复终结或对 busy 实例终结返回 false，杜绝重复回收。
func (in *Instance) terminate() bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.state != InstanceIdle && in.state != InstanceDraining {
		return false
	}
	in.state = InstanceTerminated
	in.cancelExec = nil
	return true
}

// forceAbort 平台超时强杀：取消执行上下文并直接置为 terminated。
// 幂等：重复调用安全。被终止实例不会回到池中，避免污染后续调用。
func (in *Instance) forceAbort() {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.state == InstanceTerminated {
		return
	}
	if in.cancelExec != nil {
		in.cancelExec()
		in.cancelExec = nil
	}
	in.state = InstanceTerminated
}

// run 在该实例内执行一次处理器调用，恢复处理器 panic 并归类为平台错误
// （模拟进程崩溃，与函数主动返回的业务错误区分）。
func (in *Instance) run(ctx context.Context, ic InvokeContext, payload []byte) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = wrapError(ErrClassPlatform,
				"function handler crashed (panic recovered, simulating process crash)",
				fmt.Errorf("panic: %v", r))
		}
	}()
	return in.handler.Invoke(ctx, ic, payload)
}
