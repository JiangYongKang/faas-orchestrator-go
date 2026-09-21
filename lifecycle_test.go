package faas

import (
	"context"
	"sync"
	"testing"
	"time"
)

func newTestPool(t *testing.T, cfg PoolConfig) (*Runtime, *InstancePool) {
	t.Helper()
	rt := NewRuntime()
	return rt, NewInstancePool(rt, cfg)
}

func echoVersion(name string, maxConc int) *FunctionVersion {
	return &FunctionVersion{
		FunctionName: name,
		Version:      1,
		Config: VersionConfig{
			HandlerName:    "echo",
			Timeout:        time.Second,
			MaxConcurrency: maxConc,
			Retry:          DefaultRetryPolicy(),
		},
		State: VersionActive,
	}
}

func TestInstance_StateMachine_NoDoubleReap(t *testing.T) {
	rt := NewRuntime()
	h, _ := rt.lookup("echo")
	in := &Instance{ID: "x", FunctionName: "f", Version: 1, handler: h,
		state: InstanceIdle, createdAt: time.Now(), lastUsedAt: time.Now()}

	if in.terminate() != true {
		t.Fatal("idle 实例应可终结")
	}
	if in.terminate() != false {
		t.Fatal("重复终结必须返回 false，杜绝重复回收")
	}
	if in.markDraining() {
		t.Fatal("terminated 实例不得再进入 draining")
	}
	if in.tryBeginCall(nil) {
		t.Fatal("terminated 实例不得接收新调用")
	}
	logCase(t, "输入 terminate×2 / markDraining / tryBeginCall → 仅首次成功，其余拒绝")

	// busy -> draining 期间不接新调用，结束后落 draining。
	in2 := &Instance{ID: "y", FunctionName: "f", Version: 1, handler: h,
		state: InstanceBusy, createdAt: time.Now(), lastUsedAt: time.Now()}
	if !in2.markDraining() {
		t.Fatal("busy 实例应可登记回收请求")
	}
	if in2.tryBeginCall(nil) {
		t.Fatal("回收中的实例不得再接收新调用")
	}
	if in2.State() != InstanceBusy {
		t.Fatal("手头调用未结束前状态应仍为 busy")
	}
	if !in2.finishCall() || in2.State() != InstanceDraining {
		t.Fatal("当前调用结束后应转入 draining")
	}
	logCase(t, "输入 busy 实例回收 → 手头调用完成前仍 busy 且拒绝新调用，完成后 draining")
}

func TestPool_ConcurrencyCapAndReuse(t *testing.T) {
	_, pool := newTestPool(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 4, ReapInterval: 0})
	defer pool.Stop()

	block := make(chan struct{})
	rtH := NewRuntime()
	_ = rtH.Register("blocker", HandlerFunc(func(ctx context.Context, _ InvokeContext, _ []byte) ([]byte, error) {
		<-block
		return []byte("done"), nil
	}))
	// 直接替换池的运行时以注入阻塞处理器。
	pool.runtime = rtH
	v2 := echoVersion("fn", 2)
	v2.Config.HandlerName = "blocker"

	i1, err := pool.Acquire(v2)
	assertOK(t, err, "获取第 1 个实例")
	i2, err := pool.Acquire(v2)
	assertOK(t, err, "获取第 2 个实例")
	_, err = pool.Acquire(v2)
	assertClass(t, err, ErrClassConcurrency)
	logCase(t, "输入并发=2 且上限=2 → 第 3 个 Acquire 判 concurrency_limited，不排队不丢弃静默")

	stats := pool.Stats()
	if stats.Busy != 2 {
		t.Fatalf("应有 2 个 busy 实例, got %+v", stats)
	}

	// 释放一个，闸门立即放开。
	pool.Release(i1)
	i3, err := pool.Acquire(v2)
	assertOK(t, err, "释放后再次获取")
	logCase(t, "输入 release 1 个 → inflight 回落，第 3 个 Acquire 成功")

	close(block)
	pool.Release(i2)
	pool.Release(i3)
	stats = pool.Stats()
	if stats.Idle < 1 {
		t.Fatalf("正常归还后实例应可复用(idle), got %+v", stats)
	}
	logCase(t, "输入全部正常归还 → idle=%d（判定依据：实例复用而非反复冷启动）", stats.Idle)
}

func TestPool_ReapIdleAndDrain(t *testing.T) {
	_, pool := newTestPool(t, PoolConfig{IdleTTL: 50 * time.Millisecond, MaxIdlePerVersion: 4, ReapInterval: 0})
	defer pool.Stop()
	v := echoVersion("fn", 4)

	var held []*Instance
	for i := 0; i < 3; i++ {
		in, err := pool.Acquire(v)
		assertOK(t, err, "获取实例")
		held = append(held, in)
	}
	for _, in := range held {
		pool.Release(in)
	}
	if got := pool.Stats().Idle; got != 3 {
		t.Fatalf("应累积 3 个空闲实例, got %d", got)
	}

	// 立即回收：未超 TTL，不终结。
	if n := pool.ReapIdle(time.Now()); n != 0 {
		t.Fatalf("未超过 IdleTTL 不应回收, reaped=%d", n)
	}
	// 越过 TTL 后回收：全部终结。
	time.Sleep(70 * time.Millisecond)
	n := pool.ReapIdle(time.Now())
	if n != 3 {
		t.Fatalf("超过 IdleTTL 应回收 3 个, got %d", n)
	}
	if st := pool.Stats(); st.Idle != 0 || st.Busy != 0 {
		t.Fatalf("回收后池应为空, got %+v", st)
	}
	logCase(t, "输入 3 空闲实例越过 TTL → ReapIdle=%d，判定依据 lastUsedAt+IdleTTL", n)

	// DrainVersion：忙碌实例标记回收中，释放后立即终结，不回空闲链。
	in, _ := pool.Acquire(v)
	pool.DrainVersion("fn", 1)
	if in.State() != InstanceDraining && in.State() != InstanceBusy {
		t.Fatal("忙碌实例 Drain 后应为 busy(带回收标记) 或 draining")
	}
	pool.Release(in)
	if st := pool.Stats(); st.Idle != 0 {
		t.Fatalf("回收中的忙碌实例释放后不得回池, got %+v", st)
	}
	// Drain 幂等：再次调用安全。
	pool.DrainVersion("fn", 1)
	pool.DrainVersion("fn", 1)
	logCase(t, "输入忙碌中 DrainVersion×3 → 实例释放后终结且不回池，重复回收幂等")
}

func TestPool_ConcurrentScaleSelfConsistent(t *testing.T) {
	_, pool := newTestPool(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 8, ReapInterval: 0})
	defer pool.Stop()
	v := echoVersion("fn", 8)

	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			in, err := pool.Acquire(v)
			if err != nil {
				errs <- err
				return
			}
			time.Sleep(2 * time.Millisecond)
			pool.Release(in)
		}()
	}
	wg.Wait()
	close(errs)
	granted, limited := 0, 0
	for e := range errs {
		if pe, ok := AsError(e); ok && pe.Class == ErrClassConcurrency {
			limited++
		} else {
			t.Fatalf("并发扩缩只允许 concurrency 拒绝，出现其他错误: %v", e)
		}
	}
	granted = 50 - limited
	st := pool.Stats()
	if st.Busy != 0 {
		t.Fatalf("全部协程结束后不应残留 busy, got %+v", st)
	}
	if st.Idle > 8 {
		t.Fatalf("空闲实例数必须受上限约束, got %d", st.Idle)
	}
	logCase(t, "输入 50 并发 / 上限 8 → 放行=%d 拒绝=%d 终态 idle=%d busy=0（状态自洽）",
		granted, limited, st.Idle)
}
