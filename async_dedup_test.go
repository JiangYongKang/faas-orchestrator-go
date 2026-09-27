package faas

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// sideEffectCfg 构造一个 sideeffect-counter 版本配置（默认单次、可覆盖重试）。
func sideEffectCfg(retryN int) VersionConfig {
	cfg := testCfg("sideeffect-counter")
	cfg.Timeout = 200 * time.Millisecond
	if retryN > 0 {
		cfg.Retry = RetryPolicy{MaxAttempts: retryN, Backoff: 2 * time.Millisecond}
	}
	return cfg
}

func TestAsync_DuplicateAndConcurrentSubmissionsOneSideEffect(t *testing.T) {
	resetSideEffectCounter()
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 8})
	assertOK(t, p.RegisterFunction("pay"), "注册 pay")
	_, err := p.Publish("pay", sideEffectCfg(1))
	assertOK(t, err, "发布副作用函数（不重试）")

	const bizKey = "order-async-1"
	req := func() *InvokeRequest {
		return &InvokeRequest{
			FunctionName:   "pay",
			IdempotencyKey: bizKey,
			Payload:        []byte(`{"key":"` + bizKey + `","fastMs":20}`),
		}
	}

	// 先串行重复提交两次：必须返回同一凭证。
	id1, err := p.SubmitAsync(req())
	assertOK(t, err, "首次提交")
	id2, err := p.SubmitAsync(req())
	assertOK(t, err, "重复提交")
	if id1 != id2 {
		t.Fatalf("重复提交必须返回同一凭证: %s != %s", id1, id2)
	}

	// 再并发提交 16 次（含新键首次并发的极端情况另测，这里复用同键）：全部同凭证同结论。
	var wg sync.WaitGroup
	ids := make([]string, 24)
	for i := 0; i < 24; i++ {
		if i < 2 {
			ids[i] = id1 // 前两格填已提交的，保证数组同质
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if idx < 2 {
				return
			}
			got, sErr := p.SubmitAsync(req())
			if sErr != nil {
				t.Errorf("并发提交失败: %v", sErr)
				return
			}
			ids[idx] = got
		}(i)
	}
	wg.Wait()
	for _, got := range ids {
		if got != id1 {
			t.Fatalf("所有提交方必须拿到同一凭证，发现 %s != %s", got, id1)
		}
	}

	snap := waitForState(t, p, id1, TaskSucceeded)
	if sideEffectInvocations(bizKey) != 1 {
		t.Fatalf("同一业务操作只允许一次实际执行，invocations=%d", sideEffectInvocations(bizKey))
	}
	if !sideEffectCommitted(bizKey) {
		t.Fatal("副作用应恰好提交一次")
	}
	logCase(t, "2 次串行 + 22 次并发同键提交 → 同一凭证、invocations=1、committed=true")

	// 终态后所有提交方反复查询，结论逐字节一致。
	var m1 map[string]any
	_ = json.Unmarshal(snap.Payload, &m1)
	for i := 0; i < 10; i++ {
		s, _ := p.QueryAsync(id1)
		if s.State != TaskSucceeded || string(s.Payload) != string(snap.Payload) {
			t.Fatalf("查询结论不一致: %s vs %s", s.Payload, snap.Payload)
		}
	}
}

func TestAsync_FirstTimeConcurrentSubmissionsCoalesce(t *testing.T) {
	resetSideEffectCounter()
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 8})
	assertOK(t, p.RegisterFunction("pay2"), "注册 pay2")
	_, err := p.Publish("pay2", sideEffectCfg(1))
	assertOK(t, err, "发布")

	const bizKey = "order-async-race"
	ids := make([]string, 16)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			id, sErr := p.SubmitAsync(&InvokeRequest{
				FunctionName:   "pay2",
				IdempotencyKey: bizKey,
				Payload:        []byte(`{"key":"` + bizKey + `","fastMs":20}`),
			})
			if sErr != nil {
				t.Errorf("并发提交失败: %v", sErr)
				return
			}
			ids[idx] = id
		}(i)
	}
	close(start) // 同时放行，强制在 Submit 临界区上竞争
	wg.Wait()
	for i := 1; i < len(ids); i++ {
		if ids[i] != ids[0] || ids[0] == "" {
			t.Fatalf("首次并发提交必须合并为同一任务: %v", ids)
		}
	}
	waitForState(t, p, ids[0], TaskSucceeded)
	if got := sideEffectInvocations(bizKey); got != 1 {
		t.Fatalf("首次并发提交只允许一次执行, invocations=%d", got)
	}
	logCase(t, "16 路首次并发提交 → 合并为一个任务，invocations=1")
}

func TestAsync_RetriesWithinOneSubmissionCommitSideEffectOnce(t *testing.T) {
	resetSideEffectCounter()
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 8})
	assertOK(t, p.RegisterFunction("pay3"), "注册 pay3")
	cfg := sideEffectCfg(3) // 允许 3 次尝试
	cfg.Timeout = 80 * time.Millisecond
	_, err := p.Publish("pay3", cfg)
	assertOK(t, err, "发布：前两次尝试超时，第三次成功")

	const bizKey = "order-retry-1"
	// 第 1、2 次尝试睡 300ms（>deadline 80ms）触发可重试超时；第 3 次 10ms 成功。
	id, err := p.SubmitAsync(&InvokeRequest{
		FunctionName:   "pay3",
		IdempotencyKey: bizKey,
		Payload: []byte(`{"key":"` + bizKey +
			`","slowUntilAttempt":3,"slowMs":300,"fastMs":10}`),
	})
	assertOK(t, err, "带业务操作标识的提交必须受理")
	snap := waitForState(t, p, id, TaskSucceeded)
	if snap.Attempts != 3 {
		t.Fatalf("应在第 3 次尝试成功, attempts=%d", snap.Attempts)
	}
	if got := sideEffectInvocations(bizKey); got != 3 {
		t.Fatalf("一次提交内应实际执行 3 次（平台无法抑制重试执行）, got %d", got)
	}
	if !sideEffectCommitted(bizKey) {
		t.Fatal("副作用必须已提交")
	}
	// 副作用"提交"按幂等键只发生一次：committed 标记唯一即证。
	logCase(t, "一次提交内 3 次尝试（2 次超时被杀）→ invocations=3 但副作用只提交一次，终态成功")
}

func TestAsync_QueueOverflowRejectedNotDropped(t *testing.T) {
	// 并发 1、排队容量 2：占 1 跑、再排 2，第 4 个提交必须得到可区分的拒绝结论。
	p, err := NewPlatformWithAsyncConfig("", PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 2},
		AsyncConfig{MaxQueuedPerVersion: 2, DispatchInterval: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	release := make(chan struct{})
	assertOK(t, p.RegisterHandler("ovf-blocker", HandlerFunc(func(_ context.Context, _ InvokeContext, _ []byte) ([]byte, error) {
		<-release
		return nil, nil
	})), "注册阻塞处理器")
	assertOK(t, p.RegisterFunction("ovf"), "注册 ovf")
	cfg := testCfg("ovf-blocker")
	cfg.MaxConcurrency = 1
	cfg.Timeout = 10 * time.Second
	_, err = p.Publish("ovf", cfg)
	assertOK(t, err, "发布并发上限 1 版本")

	submit := func(k string) (string, error) {
		return p.SubmitAsync(&InvokeRequest{FunctionName: "ovf", IdempotencyKey: k})
	}
	holder, err := submit("h")
	assertOK(t, err, "占槽")
	waitForState(t, p, holder, TaskRunning)
	q1, err := submit("q1")
	assertOK(t, err, "排队 1")
	q2, err := submit("q2")
	assertOK(t, err, "排队 2")
	// 并发 1 已被 holder 占满，q1/q2 必须稳定停留在 queued；确认后在途数=1 跑+2 排。
	waitForStateIn(t, p, q1, 100*time.Millisecond, TaskQueued)
	waitForStateIn(t, p, q2, 50*time.Millisecond, TaskQueued)
	_, err = submit("overflow")
	assertClass(t, err, ErrClassConcurrency)
	logCase(t, "在途(1 跑 + 2 排)达到上限(并发1+排队2)后的提交 → concurrency_limited，可区分、未静默丢弃")

	// 释放槽位后，排队任务按 FIFO 正常推进并成功。
	close(release)
	waitForState(t, p, q1, TaskSucceeded)
	waitForState(t, p, q2, TaskSucceeded)
	logCase(t, "容量恢复后排队任务依次成功（排队而非丢弃得到验证）")
}

func TestAsync_ConcurrentQueryCancelAndReclaimStayConsistent(t *testing.T) {
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 8})
	assertOK(t, p.RegisterFunction("cc"), "注册 cc")
	cfg := testCfg("ignore-cancel")
	cfg.Timeout = 120 * time.Millisecond
	_, err := p.Publish("cc", cfg)
	assertOK(t, err, "发布：不配合取消 + 120ms 截止")

	var wg sync.WaitGroup
	terminalSeen := make(map[TaskState]int)
	var mu sync.Mutex
	for i := 0; i < 12; i++ {
		id, sErr := p.SubmitAsync(&InvokeRequest{
			FunctionName:   "cc",
			IdempotencyKey: "cc-" + itoa(i+1),
			Payload:        []byte(`{"sleepMs":1000}`),
		})
		if sErr != nil {
			t.Fatal(sErr)
		}
		// 一半任务在随机时刻取消，一半任其超时；同时多 goroutine 反复查询。
		wg.Add(3)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(20+i%5*30) * time.Millisecond)
			_, _ = p.CancelAsync(id)
		}()
		go func() {
			defer wg.Done()
			var last TaskState
			for j := 0; j < 60; j++ {
				snap, _ := p.QueryAsync(id)
				if snap == nil {
					continue
				}
				if last != "" && snap.State.Terminal() && last.Terminal() && snap.State != last {
					t.Errorf("终态发生跳变: %s -> %s (task %s)", last, snap.State, id)
				}
				if snap.State.Terminal() {
					mu.Lock()
					terminalSeen[snap.State]++
					mu.Unlock()
				}
				last = snap.State
				time.Sleep(2 * time.Millisecond)
			}
		}()
		go func() { // 并发回收扫描，与取消/查询同时发生
			defer wg.Done()
			for j := 0; j < 30; j++ {
				p.ReapIdle()
				time.Sleep(3 * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	// 所有任务必须有确定终态，且只可能是 timed_out 或 canceled（不可能 failed/succeeded）。
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < 12; i++ {
		id := "task-" + itoa(i+1)
		snap, qErr := p.QueryAsync(id)
		if qErr != nil || !snap.State.Terminal() {
			t.Fatalf("任务 %s 必须已有确定终态: %+v err=%v", id, snap, qErr)
		}
		if snap.State != TaskTimedOut && snap.State != TaskCanceled {
			t.Fatalf("终态只能是 timed_out/canceled, got %s", snap.State)
		}
	}
	if st := p.PoolStats(); st.Busy != 0 {
		t.Fatalf("全部终态后 busy 必须归零: %+v", st)
	}
	logCase(t, "12 任务 × 并发查询/取消/回收：终态分布=%v，无跳变、busy=0", terminalSeen)
}
