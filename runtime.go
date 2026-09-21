package faas

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// InvokeContext 暴露给函数处理器的调用上下文。
type InvokeContext struct {
	FunctionName   string
	Version        int
	InstanceID     string
	Attempt        int    // 当前是第几次尝试（1 起）
	IdempotencyKey string // 平台幂等键，处理器可用它对副作用做去重
}

// Handler 模拟一个本地函数实现。
// 返回 (nil, nil) 表示成功无返回；error 非空表示函数自身错误（ErrClassFunction）。
// ctx 被取消时，处理器应尽快让出；平台会以 timeout 归类本次调用。
// panic 会被平台恢复并归类为 platform_error（模拟进程崩溃，可重试），
// 与函数主动返回的 function_error 严格区分。
type Handler interface {
	Invoke(ctx context.Context, ic InvokeContext, payload []byte) ([]byte, error)
}

// HandlerFunc 便于用闭包注册处理器。
type HandlerFunc func(ctx context.Context, ic InvokeContext, payload []byte) ([]byte, error)

// Invoke 实现 Handler。
func (f HandlerFunc) Invoke(ctx context.Context, ic InvokeContext, payload []byte) ([]byte, error) {
	return f(ctx, ic, payload)
}

// Runtime 是处理器名 -> 本地实现 的注册表（模拟发布产物装载）。
type Runtime struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRuntime 创建运行时并装载内置演示处理器。
func NewRuntime() *Runtime {
	rt := &Runtime{handlers: make(map[string]Handler)}
	rt.mustRegister("echo", HandlerFunc(echoHandler))
	rt.mustRegister("slow", HandlerFunc(slowHandler))
	rt.mustRegister("fail", HandlerFunc(failHandler))
	rt.mustRegister("panic", HandlerFunc(panicHandler))
	rt.mustRegister("flaky-timeout", HandlerFunc(flakyTimeoutHandler))
	rt.mustRegister("idempotent-sideeffect", HandlerFunc(idempotentSideEffectHandler))
	return rt
}

func (rt *Runtime) mustRegister(name string, h Handler) {
	if err := rt.Register(name, h); err != nil {
		panic(err)
	}
}

// Register 装载一个命名处理器；重复装载被拒绝。
func (rt *Runtime) Register(name string, h Handler) error {
	if name == "" {
		return newError(ErrClassInvalid, "handler name must not be empty")
	}
	if h == nil {
		return newError(ErrClassInvalid, "handler must not be nil")
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if _, ok := rt.handlers[name]; ok {
		return newError(ErrClassConflict, "handler already registered: "+name)
	}
	rt.handlers[name] = h
	return nil
}

// lookup 按名取处理器；缺失返回 not_found（对应平台错误：发布产物不可用）。
func (rt *Runtime) lookup(name string) (Handler, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	h, ok := rt.handlers[name]
	if !ok {
		return nil, newError(ErrClassNotFound, "deployed handler artifact not found: "+name)
	}
	return h, nil
}

// ---- 内置处理器 ----

// echoHandler 原样回显输入并附带判定依据，便于日志核对。
func echoHandler(_ context.Context, ic InvokeContext, payload []byte) ([]byte, error) {
	out := map[string]any{
		"echo":     string(payload),
		"instance": ic.InstanceID,
		"attempt":  ic.Attempt,
		"idemKey":  ic.IdempotencyKey,
	}
	return json.Marshal(out)
}

// slowPayload 控制 slow 处理器的睡眠时长。
type slowPayload struct {
	SleepMS int `json:"sleepMs"`
}

// slowHandler 睡眠指定毫秒（用于触发硬超时）；ctx 取消则立刻让出。
func slowHandler(ctx context.Context, ic InvokeContext, payload []byte) ([]byte, error) {
	var p slowPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("slow: invalid payload: %w", err)
	}
	select {
	case <-time.After(time.Duration(p.SleepMS) * time.Millisecond):
		return json.Marshal(map[string]any{"sleptMs": p.SleepMS, "instance": ic.InstanceID})
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// failHandler 始终返回函数业务错误（不可重试）。
func failHandler(_ context.Context, _ InvokeContext, payload []byte) ([]byte, error) {
	return nil, fmt.Errorf("function business failure on input=%q", string(payload))
}

// panicHandler 始终 panic，模拟进程崩溃（归类 platform_error，可重试）。
func panicHandler(_ context.Context, _ InvokeContext, _ []byte) ([]byte, error) {
	panic("simulated process crash inside function runtime")
}

// flakyTimeoutPayload 在达到目标尝试次数前故意睡过头触发超时。
type flakyTimeoutPayload struct {
	SlowUntilAttempt int `json:"slowUntilAttempt"` // attempt < 该值时睡 SlowMS
	SlowMS           int `json:"slowMs"`
	FastMS           int `json:"fastMs"`
}

// flakyTimeoutHandler 前几次尝试超时（实例会被强杀），达到目标尝试后快速成功，
// 用于验证"超时重试 + 被杀实例不回池 + 新实例冷启动"的完整链路。
func flakyTimeoutHandler(ctx context.Context, ic InvokeContext, payload []byte) ([]byte, error) {
	var p flakyTimeoutPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("flaky-timeout: invalid payload: %w", err)
	}
	wait := p.FastMS
	if ic.Attempt < p.SlowUntilAttempt {
		wait = p.SlowMS
	}
	select {
	case <-time.After(time.Duration(wait) * time.Millisecond):
		return json.Marshal(map[string]any{
			"attempt": ic.Attempt, "instance": ic.InstanceID, "waitedMs": wait,
		})
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// sideEffectLog 是处理器内的副作用"外部系统"模拟，按幂等键去重写入。
var sideEffectLog sync.Map // idempotencyKey -> count

// idempotentSideEffectHandler 演示处理器侧基于幂等键的副作用去重：
// 即便平台发生重试，同一幂等键的副作用只提交一次。
func idempotentSideEffectHandler(_ context.Context, ic InvokeContext, payload []byte) ([]byte, error) {
	if ic.IdempotencyKey == "" {
		return nil, fmt.Errorf("idempotent-sideeffect requires idempotencyKey")
	}
	_, loaded := sideEffectLog.LoadOrStore(ic.IdempotencyKey, 1)
	if loaded {
		return json.Marshal(map[string]any{"sideEffect": "deduped", "key": ic.IdempotencyKey})
	}
	return json.Marshal(map[string]any{"sideEffect": "committed", "key": ic.IdempotencyKey, "input": string(payload)})
}

// resetSideEffectLog 仅供测试复位。
func resetSideEffectLog() {
	sideEffectLog.Range(func(k, _ any) bool {
		sideEffectLog.Delete(k)
		return true
	})
}
