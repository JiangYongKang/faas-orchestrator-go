package faas

import (
	"testing"
	"time"
)

func TestAsync_DeadlineTerminatesEvenWhenFunctionIgnoresCancellation(t *testing.T) {
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 4})
	assertOK(t, p.RegisterFunction("stub"), "注册 stub")
	cfg := testCfg("ignore-cancel")
	cfg.Timeout = 80 * time.Millisecond // 硬截止
	_, err := p.Publish("stub", cfg)
	assertOK(t, err, "发布不配合取消的版本（睡眠 500ms）")

	start := time.Now()
	id, err := p.SubmitAsync(&InvokeRequest{
		FunctionName: "stub",
		Payload:      []byte(`{"sleepMs":500}`), // 刻意远超 deadline
	})
	assertOK(t, err, "提交")
	snap := waitForState(t, p, id, TaskTimedOut)
	elapsed := time.Since(start)
	if snap.ErrorClass != ErrClassTimeout {
		t.Fatalf("截止终态 errorClass 应为 timeout, got %q", snap.ErrorClass)
	}
	// 关键保证：即使函数完全不配合取消，查询方也必须在"有限且接近 deadline"的时间内见终态。
	if elapsed > 400*time.Millisecond {
		t.Fatalf("未在硬截止附近得到终态，耗时 %v（应 ~80ms，绝不等满 500ms）", elapsed)
	}
	logCase(t, "函数忽略 ctx 且 sleep=500ms > deadline=80ms → %v 内 timed_out", elapsed)

	// 该次执行占用的实例必须已回收：池内 busy=0。
	if st := p.PoolStats(); st.Busy != 0 {
		t.Fatalf("截止后执行实例必须回收，busy 应为 0, got %+v", st)
	}
}

func TestAsync_CancelRunningIgnoresCancellationAndReclaimsInstance(t *testing.T) {
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 4})
	assertOK(t, p.RegisterFunction("stub2"), "注册 stub2")
	cfg := testCfg("ignore-cancel")
	cfg.Timeout = 5 * time.Second // 截止很长：终态只能来自主动撤销
	_, err := p.Publish("stub2", cfg)
	assertOK(t, err, "发布")

	id, err := p.SubmitAsync(&InvokeRequest{
		FunctionName: "stub2",
		Payload:      []byte(`{"sleepMs":5000}`),
	})
	assertOK(t, err, "提交")
	waitForState(t, p, id, TaskRunning)

	start := time.Now()
	snap, err := p.CancelAsync(id)
	assertOK(t, err, "撤销")
	if snap.State != TaskRunning && snap.State != TaskCanceled {
		t.Fatalf("撤销返回快照异常: %+v", snap)
	}
	final := waitForState(t, p, id, TaskCanceled)
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("撤销后必须在有限时间内见到 canceled，而不是等函数睡满")
	}
	if final.ErrorClass != ErrClassTimeout {
		t.Fatalf("canceled 终态应保留 timeout 类取消原因, got %q", final.ErrorClass)
	}
	if st := p.PoolStats(); st.Busy != 0 {
		t.Fatalf("撤销后实例必须回收，busy=0, got %+v", st)
	}
	logCase(t, "运行中撤销不配合取消的函数 → canceled 且 busy 立即归零")
}

func TestAsync_CancelIsTerminalAndIdempotent(t *testing.T) {
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 4})
	assertOK(t, p.RegisterFunction("stub3"), "注册 stub3")
	_, err := p.Publish("stub3", testCfg("echo"))
	assertOK(t, err, "发布 echo")

	id, err := p.SubmitAsync(&InvokeRequest{FunctionName: "stub3"})
	assertOK(t, err, "提交")
	final := waitForState(t, p, id, TaskSucceeded)

	// 终态后取消：必须保持原终态（succeeded），状态绝不回退/跳变。
	snap, err := p.CancelAsync(id)
	assertOK(t, err, "对终态任务取消不报错但保持原终态")
	if snap.State != TaskSucceeded {
		t.Fatalf("终态不可被取消覆盖: got %s want succeeded", snap.State)
	}
	snap2, _ := p.QueryAsync(id)
	if snap2.State != TaskSucceeded || string(snap2.Payload) != string(final.Payload) {
		t.Fatalf("终态结论必须保持稳定: %+v", snap2)
	}
	logCase(t, "succeeded 后再 cancel → 仍 succeeded（状态机单向）")
}

func TestAsync_CancelQueuedTaskNeverStarts(t *testing.T) {
	// 并发上限 1：先占住唯一槽位，后续任务排队；撤销排队任务必须立即 canceled，
	// 且永远不产生执行。
	p, done := newBlockedAsyncPlatform(t, "qfn", 1)
	defer close(done)

	holder, err := p.SubmitAsync(&InvokeRequest{FunctionName: "qfn", IdempotencyKey: "hold"})
	assertOK(t, err, "占槽任务提交")
	waitForState(t, p, holder, TaskRunning)

	queued, err := p.SubmitAsync(&InvokeRequest{FunctionName: "qfn", IdempotencyKey: "waiting"})
	assertOK(t, err, "排队任务提交")
	// 给调度器一个周期确认它停留在 queued（拿不到槽位）。
	waitForStateIn(t, p, queued, 200*time.Millisecond, TaskQueued)

	start := time.Now()
	snap, err := p.CancelAsync(queued)
	assertOK(t, err, "撤销排队任务")
	if snap.State != TaskCanceled {
		t.Fatalf("排队任务应立即 canceled, got %s", snap.State)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("排队任务撤销必须立即生效")
	}
	// 槽位释放后，被撤销的排队任务不得复活执行（最终仍 canceled）。
	time.Sleep(80 * time.Millisecond)
	snap2, _ := p.QueryAsync(queued)
	if snap2.State != TaskCanceled || snap2.Attempts != 0 {
		t.Fatalf("撤销的排队任务不得被调度执行: %+v", snap2)
	}
	logCase(t, "排队中撤销 → 立即 canceled，attempts=0，槽位空出后不复活")
}

func TestAsync_ReclaimedInstanceDoesNotAffectLaterTasks(t *testing.T) {
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 4})
	assertOK(t, p.RegisterFunction("d1"), "注册 d1")
	cfg := testCfg("ignore-cancel")
	cfg.Timeout = 60 * time.Millisecond
	_, err := p.Publish("d1", cfg)
	assertOK(t, err, "发布超时版本")

	// 连续 5 个超时任务，每个都强杀占用实例。
	for i := 0; i < 5; i++ {
		id, sErr := p.SubmitAsync(&InvokeRequest{
			FunctionName:   "d1",
			IdempotencyKey: "deadline-" + itoa(i+1),
			Payload:        []byte(`{"sleepMs":500}`),
		})
		assertOK(t, sErr, "超时任务提交")
		waitForState(t, p, id, TaskTimedOut)
	}
	if st := p.PoolStats(); st.Busy != 0 {
		t.Fatalf("全部超时任务结束后 busy 必须为 0, got %+v", st)
	}

	// 回收后另一个函数版本的任务必须在干净实例上正常完成。
	assertOK(t, p.RegisterFunction("d2"), "注册 d2")
	_, err = p.Publish("d2", testCfg("echo"))
	assertOK(t, err, "发布 echo")
	id, err := p.SubmitAsync(&InvokeRequest{FunctionName: "d2", Payload: []byte(`"clean"`)})
	assertOK(t, err, "后续任务提交")
	snap := waitForState(t, p, id, TaskSucceeded)
	if string(snap.Payload) == "" {
		t.Fatalf("回收后的后续任务必须正常成功: %+v", snap)
	}
	logCase(t, "5 次超时强杀后新函数任务仍成功，被杀实例不复用、不污染")
}

// waitForStateIn 在给定时间内确认任务处于指定状态（用于验证"稳定停留"在 queued）。
func waitForStateIn(t *testing.T, p *Platform, taskID string, dur time.Duration, want TaskState) *TaskSnapshot {
	t.Helper()
	deadline := time.Now().Add(dur)
	var last *TaskSnapshot
	for time.Now().Before(deadline) {
		snap, err := p.QueryAsync(taskID)
		if err != nil {
			t.Fatalf("查询 %s: %v", taskID, err)
		}
		if snap.State != want {
			t.Fatalf("任务 %s 应稳定停留在 %s，实际进入 %s", taskID, want, snap.State)
		}
		last = snap
		time.Sleep(2 * time.Millisecond)
	}
	return last
}

func TestAsync_TimeoutRetriesUseFreshInstancesAndSucceed(t *testing.T) {
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 8})
	assertOK(t, p.RegisterFunction("afk"), "注册 afk")
	cfg := testCfg("flaky-timeout")
	cfg.Timeout = 80 * time.Millisecond
	cfg.Retry = RetryPolicy{MaxAttempts: 3, Backoff: 3 * time.Millisecond}
	_, err := p.Publish("afk", cfg)
	assertOK(t, err, "发布：第 1 次超时，第 2 次成功")

	id, err := p.SubmitAsync(&InvokeRequest{
		FunctionName:   "afk",
		IdempotencyKey: "afk-1",
		Payload:        []byte(`{"slowUntilAttempt":2,"slowMs":300,"fastMs":10}`),
	})
	assertOK(t, err, "提交（重试策略要求业务操作标识，已提供）")
	snap := waitForState(t, p, id, TaskSucceeded)
	if snap.Attempts != 2 {
		t.Fatalf("应在第 2 次尝试成功, attempts=%d", snap.Attempts)
	}
	if st := p.PoolStats(); st.Busy != 0 {
		t.Fatalf("被杀的第 1 个实例不得残留在 busy: %+v", st)
	}
	logCase(t, "异步重试：第 1 次超时强杀、新实例第 2 次成功，busy=0")

	// 紧接的新任务必须正常（被杀实例绝不复用）。
	id2, err := p.SubmitAsync(&InvokeRequest{FunctionName: "afk", IdempotencyKey: "afk-2",
		Payload: []byte(`{"slowUntilAttempt":0,"fastMs":5}`)})
	assertOK(t, err, "后续任务提交")
	waitForState(t, p, id2, TaskSucceeded)
}

func TestAsync_CancelDuringRetryBackoffIsCanceled(t *testing.T) {
	// 构造：第 1 次快速失败为可重试的平台错误较难，这里用"取消发生在排队->运行交界"
	// 已在 TestAsync_CancelQueuedTaskNeverStarts 覆盖；此处验证超时后重试退避期取消。
	resetSideEffectCounter()
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 8})
	assertOK(t, p.RegisterFunction("bo"), "注册 bo")
	cfg := sideEffectCfg(5)
	cfg.Timeout = 40 * time.Millisecond
	cfg.Retry = RetryPolicy{MaxAttempts: 5, Backoff: 200 * time.Millisecond} // 退避足够长，便于在其间取消
	_, err := p.Publish("bo", cfg)
	assertOK(t, err, "发布：持续超时 + 长退避")

	id, err := p.SubmitAsync(&InvokeRequest{
		FunctionName:   "bo",
		IdempotencyKey: "bo-1",
		Payload:        []byte(`{"key":"bo-1","slowUntilAttempt":99,"slowMs":300,"fastMs":5}`),
	})
	assertOK(t, err, "提交")
	waitForState(t, p, id, TaskRunning)
	// 等首个尝试超时（~40ms），任务进入退避，随后撤销。
	time.Sleep(70 * time.Millisecond)
	start := time.Now()
	_, err = p.CancelAsync(id)
	assertOK(t, err, "退避期撤销")
	waitForState(t, p, id, TaskCanceled)
	if time.Since(start) > 300*time.Millisecond {
		t.Fatal("退避期撤销必须快速得到 canceled，不能等到下一次尝试")
	}
	if st := p.PoolStats(); st.Busy != 0 {
		t.Fatalf("取消后 busy 必须归零: %+v", st)
	}
	logCase(t, "重试退避期取消 → 快速 canceled，busy=0")
}

func TestAsync_StopCancelsInflightAndQueued(t *testing.T) {
	p, release := newBlockedAsyncPlatform(t, "stopfn", 1)
	holder, err := p.SubmitAsync(&InvokeRequest{FunctionName: "stopfn", IdempotencyKey: "h"})
	assertOK(t, err, "占槽")
	waitForState(t, p, holder, TaskRunning)
	qid, err := p.SubmitAsync(&InvokeRequest{FunctionName: "stopfn", IdempotencyKey: "q"})
	assertOK(t, err, "排队")
	waitForStateIn(t, p, qid, 100*time.Millisecond, TaskQueued)

	// 关闭平台：在执与排队任务都必须进入确定终态，实例被回收。
	p.Close()
	close(release) // 释放阻塞处理器，确认不会有复活/泄漏

	s1, e1 := p.QueryAsync(holder)
	assertOK(t, e1, "在执任务仍可查询")
	if s1.State != TaskCanceled {
		t.Fatalf("关闭后在执任务应 canceled, got %s", s1.State)
	}
	s2, e2 := p.QueryAsync(qid)
	assertOK(t, e2, "排队任务仍可查询")
	if s2.State != TaskCanceled {
		t.Fatalf("关闭后排队任务应 canceled, got %s", s2.State)
	}
	logCase(t, "平台关闭：在执/排队任务均 canceled，终态确定可查")
}
