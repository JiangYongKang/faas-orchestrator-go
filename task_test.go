package faas

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitTaskTerminal 轮询直到任务进入终态；超时未达终态则失败。
// 判定依据打印在日志中，便于核对状态推进路径。
func waitTaskTerminal(t *testing.T, p *Platform, id string) *TaskSnapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		snap, err := p.QueryTask(id)
		if err != nil {
			t.Fatalf("QueryTask(%s): %v", id, err)
		}
		if snap.Status.IsTerminal() {
			logCase(t, "任务 %s → 终态 %s attempts=%d class=%q reason=%q",
				id, snap.Status, snap.Attempts, snap.ErrorClass, snap.ErrorReason)
			return snap
		}
		if time.Now().After(deadline) {
			t.Fatalf("任务 %s 未在限定时间内到达终态，当前状态 %s", id, snap.Status)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// waitTaskStatus 轮询直到任务进入指定非终态（用于观测 queued/running）。
func waitTaskStatus(t *testing.T, p *Platform, id string, want TaskStatus) *TaskSnapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		snap, err := p.QueryTask(id)
		if err != nil {
			t.Fatalf("QueryTask(%s): %v", id, err)
		}
		if snap.Status == want {
			return snap
		}
		if snap.Status.IsTerminal() {
			t.Fatalf("任务 %s 在到达状态 %s 前已进入终态 %s", id, want, snap.Status)
		}
		if time.Now().After(deadline) {
			t.Fatalf("任务 %s 未在限定时间内到达状态 %s，当前 %s", id, want, snap.Status)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// stubbornHandler 模拟不配合取消的函数实现：payload 为 "block" 时无视
// ctx 取消、睡够才返回并记录一次"迟到副作用"；其余 payload 立即成功。
func stubbornHandler(lateSideEffects *atomic.Int32) HandlerFunc {
	return func(_ context.Context, _ InvokeContext, payload []byte) ([]byte, error) {
		if string(payload) == "block" {
			time.Sleep(300 * time.Millisecond) // 故意不响应取消
			lateSideEffects.Add(1)
			return []byte("blocked-done"), nil
		}
		return []byte("fast:" + string(payload)), nil
	}
}

// publishTaskFn 注册函数并发布指定配置的版本。
func publishTaskFn(t *testing.T, p *Platform, name string, cfg VersionConfig) {
	t.Helper()
	assertOK(t, p.RegisterFunction(name), "注册 "+name)
	_, err := p.Publish(name, cfg)
	assertOK(t, err, "发布 "+name+" v1")
}

func TestTask_SubmitQuerySuccessAndTerminalImmutable(t *testing.T) {
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 2})
	publishTaskFn(t, p, "fn", testCfg("echo"))

	snap, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn", Payload: []byte("hi")})
	assertOK(t, err, "提交异步任务")
	if snap.TaskID == "" || snap.Status != TaskQueued {
		t.Fatalf("提交后应立即拿到凭证且处于 queued，得到 %+v", snap)
	}
	logCase(t, "提交后立即返回凭证 %s 状态 %s（调用方不阻塞）", snap.TaskID, snap.Status)

	final := waitTaskTerminal(t, p, snap.TaskID)
	if final.Status != TaskSucceeded {
		t.Fatalf("期望 succeeded，得到 %s (%s)", final.Status, final.ErrorReason)
	}
	if final.Attempts != 1 {
		t.Fatalf("无重试策略下 attempts 应为 1，得到 %d", final.Attempts)
	}
	var echo map[string]any
	if err := json.Unmarshal(final.Payload, &echo); err != nil || echo["echo"] != "hi" {
		t.Fatalf("成功任务应携带函数载荷，得到 %s err=%v", final.Payload, err)
	}

	// 同一凭证反复查询：终态快照逐字段一致。
	again, err := p.QueryTask(snap.TaskID)
	assertOK(t, err, "终态后再次查询")
	if !reflect.DeepEqual(final, again) {
		t.Fatalf("终态后重复查询结论必须一致:\n%+v\n%+v", final, again)
	}
	logCase(t, "同一凭证两次查询结论逐字段一致（判定依据：DeepEqual）")

	// 终态后撤销是幂等空操作：状态不得回退或跳变。
	afterCancel, err := p.CancelTask(snap.TaskID)
	assertOK(t, err, "终态后撤销应幂等成功")
	if afterCancel.Status != TaskSucceeded {
		t.Fatalf("终态不可变：撤销后状态仍为 %s", afterCancel.Status)
	}
}

func TestTask_FunctionErrorMapsToFailed(t *testing.T) {
	p := newTestPlatform(t, DefaultPoolConfig())
	publishTaskFn(t, p, "fn", testCfg("fail"))

	snap, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn", Payload: []byte("x")})
	assertOK(t, err, "提交必然业务失败的任务")
	final := waitTaskTerminal(t, p, snap.TaskID)
	if final.Status != TaskFailed || final.ErrorClass != string(ErrClassFunction) {
		t.Fatalf("函数自身失败 → failed/function_error，得到 %s/%s", final.Status, final.ErrorClass)
	}
	logCase(t, "函数业务失败 → status=failed 且 class=function_error，与超时/取消可区分")
}

func TestTask_TimeoutWithNonCooperativeHandler(t *testing.T) {
	var late atomic.Int32
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 2})
	assertOK(t, p.RegisterHandler("stubborn", stubbornHandler(&late)), "装载不配合取消的处理器")
	cfg := testCfg("stubborn")
	cfg.Timeout = 60 * time.Millisecond
	publishTaskFn(t, p, "fn", cfg)

	start := time.Now()
	snap, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn", Payload: []byte("block")})
	assertOK(t, err, "提交将超时的任务")
	final := waitTaskTerminal(t, p, snap.TaskID)
	elapsed := time.Since(start)
	if final.Status != TaskTimedOut || final.ErrorClass != string(ErrClassTimeout) {
		t.Fatalf("期望 timed_out/timeout，得到 %s/%s", final.Status, final.ErrorClass)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("函数不配合取消也必须在有限时间内到达终态，耗时 %v", elapsed)
	}
	logCase(t, "函数无视取消（睡 300ms > 超时 60ms）→ %v 内到达终态 timed_out", elapsed.Round(time.Millisecond))

	st := p.PoolStats()
	if st.Busy != 0 || st.Idle != 0 {
		t.Fatalf("超时实例必须被回收且不回池，busy/idle 应为 0，得到 %+v", st)
	}
	logCase(t, "超时实例已强杀回收：busy=%d idle=%d", st.Busy, st.Idle)

	// 回收后后续任务不受影响：同函数快速任务在新实例上成功。
	snap2, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn", Payload: []byte("ok")})
	assertOK(t, err, "超时回收后提交新任务")
	final2 := waitTaskTerminal(t, p, snap2.TaskID)
	if final2.Status != TaskSucceeded || string(final2.Payload) != "fast:ok" {
		t.Fatalf("后续任务应在新实例上成功，得到 %s %s", final2.Status, final2.Payload)
	}

	// 迟到的处理器完成后，终态结论不得被改写（终态之后无平台侧副作用）。
	time.Sleep(400 * time.Millisecond)
	if late.Load() != 1 {
		t.Fatalf("不配合的处理器最终会迟到完成一次，得到 %d", late.Load())
	}
	after, err := p.QueryTask(snap.TaskID)
	assertOK(t, err, "迟到完成后再次查询")
	if !reflect.DeepEqual(final, after) {
		t.Fatalf("迟到完成不得改写终态:\n%+v\n%+v", final, after)
	}
	logCase(t, "处理器迟到完成后任务仍为 timed_out（终态不可逆，无半更新）")
}

func TestTask_CancelRunningNonCooperativeHandler(t *testing.T) {
	var late atomic.Int32
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 2})
	assertOK(t, p.RegisterHandler("stubborn", stubbornHandler(&late)), "装载不配合取消的处理器")
	cfg := testCfg("stubborn")
	cfg.Timeout = 5 * time.Second // 长超时：只有撤销能让它停下
	publishTaskFn(t, p, "fn", cfg)

	snap, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn", Payload: []byte("block")})
	assertOK(t, err, "提交长任务")
	waitTaskStatus(t, p, snap.TaskID, TaskRunning)

	start := time.Now()
	cancelSnap, err := p.CancelTask(snap.TaskID)
	assertOK(t, err, "撤销运行中任务")
	logCase(t, "运行中撤销 → 立即返回，当前观测状态 %s", cancelSnap.Status)

	final := waitTaskTerminal(t, p, snap.TaskID)
	if final.Status != TaskCanceled || final.ErrorClass != string(ErrClassCanceled) {
		t.Fatalf("期望 canceled/canceled，得到 %s/%s", final.Status, final.ErrorClass)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("不配合取消的函数也必须在有限时间内落定 canceled，耗时 %v", elapsed)
	}
	st := p.PoolStats()
	if st.Busy != 0 {
		t.Fatalf("被取消任务占用的实例必须回收，busy 应为 0，得到 %+v", st)
	}
	logCase(t, "撤销后实例已强杀：busy=%d（不等待未配合的处理器退出）", st.Busy)

	// 重复撤销幂等；后续任务不受影响。
	again, err := p.CancelTask(snap.TaskID)
	assertOK(t, err, "重复撤销")
	if again.Status != TaskCanceled {
		t.Fatalf("重复撤销不得改变终态，得到 %s", again.Status)
	}
	snap2, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn", Payload: []byte("next")})
	assertOK(t, err, "取消回收后提交新任务")
	if final2 := waitTaskTerminal(t, p, snap2.TaskID); final2.Status != TaskSucceeded {
		t.Fatalf("后续任务应成功，得到 %s", final2.Status)
	}
}

func TestTask_CancelQueuedTask(t *testing.T) {
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 2})
	cfg := testCfg("slow")
	cfg.MaxConcurrency = 1
	publishTaskFn(t, p, "fn", cfg)

	a, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn", Payload: []byte(`{"sleepMs":200}`)})
	assertOK(t, err, "提交任务 A（占满唯一并发槽位）")
	waitTaskStatus(t, p, a.TaskID, TaskRunning)

	b, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn", Payload: []byte(`{"sleepMs":10}`)})
	assertOK(t, err, "提交任务 B（应排队而非丢弃）")
	bs := waitTaskStatus(t, p, b.TaskID, TaskQueued)
	logCase(t, "并发上限=1 时第二个任务处于可观测的 queued 状态（%s）", bs.Status)

	cancelSnap, err := p.CancelTask(b.TaskID)
	assertOK(t, err, "撤销排队中的任务")
	if cancelSnap.Status != TaskCanceled {
		t.Fatalf("排队任务撤销应立即落定 canceled，得到 %s", cancelSnap.Status)
	}
	if final := waitTaskTerminal(t, p, a.TaskID); final.Status != TaskSucceeded {
		t.Fatalf("任务 A 应正常成功，得到 %s", final.Status)
	}
	// 队列腾挪后后续任务正常执行。
	c, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn", Payload: []byte(`{"sleepMs":5}`)})
	assertOK(t, err, "队列腾出后提交任务 C")
	if final := waitTaskTerminal(t, p, c.TaskID); final.Status != TaskSucceeded {
		t.Fatalf("任务 C 应成功，得到 %s", final.Status)
	}
	logCase(t, "排队任务被取消后，后续任务不受影响（A、C 均成功）")
}

// countedHandler 记录每个 token 对应任务"真正开始执行函数"的次数：
// 自增发生在处理器入口（任何业务动作之前），是"函数被调用"最严格的判据。
// blocked=true 的任务在槽位释放前会一直阻塞，用于占满唯一并发槽位。
type countedCancellationHandler struct {
	mu      sync.Mutex
	entered map[string]int
	release chan struct{}
}

func (h *countedCancellationHandler) Invoke(ctx context.Context, _ InvokeContext, payload []byte) ([]byte, error) {
	var p struct {
		Token   string `json:"token"`
		Blocked bool   `json:"blocked"`
	}
	_ = json.Unmarshal(payload, &p)
	h.mu.Lock()
	h.entered[p.Token]++
	h.mu.Unlock()
	if p.Blocked {
		select {
		case <-h.release:
		case <-ctx.Done():
		}
	}
	return []byte("done:" + p.Token), nil
}

func (h *countedCancellationHandler) entries(token string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.entered[token]
}

// TestTask_CancelQueuedTaskNeverExecutes 稳定复现并守护以下正确性问题：
// 并发上限=1 下，排队中（从未开始执行）的任务在占槽任务结束前被撤销后，
// 绝不能在槽位腾出时被真的执行一遍：
//   - 被撤销任务的处理器入口调用计数恒为 0（副作用判据）；
//   - 状态自撤销起稳定停在 canceled，任何时刻都查不到 running/succeeded；
//   - 被撤销任务不抢槽位、不拖后续：占槽任务结束后正常任务立即成功。
//
// 用多轮重复 + 并发查询，覆盖"槽位腾出"与"撤销/轮询"之间的竞态窗口。
func TestTask_CancelQueuedTaskNeverExecutes(t *testing.T) {
	const rounds = 25
	for round := 0; round < rounds; round++ {
		h := &countedCancellationHandler{
			entered: map[string]int{},
			release: make(chan struct{}),
		}
		p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 2})
		assertOK(t, p.RegisterHandler("counted-cancel", h), "装载计数处理器")
		cfg := testCfg("counted-cancel")
		cfg.MaxConcurrency = 1
		publishTaskFn(t, p, "fn", cfg)

		tokenA := fmt.Sprintf("a-%d", round)
		tokenB := fmt.Sprintf("b-%d", round)
		tokenC := fmt.Sprintf("c-%d", round)

		// A：占满唯一槽位，直到本用例主动放行才结束。
		a, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn",
			Payload: []byte(fmt.Sprintf(`{"token":%q,"blocked":true}`, tokenA))})
		assertOK(t, err, "提交占槽任务 A")
		waitTaskStatus(t, p, a.TaskID, TaskRunning)

		// B：槽位已满，停在 queued。
		b, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn",
			Payload: []byte(fmt.Sprintf(`{"token":%q}`, tokenB))})
		assertOK(t, err, "提交排队任务 B")
		waitTaskStatus(t, p, b.TaskID, TaskQueued)

		// 撤销期间并发反复查询：任何一次观测都不允许出现 running/成功等跳变。
		stopObs := make(chan struct{})
		var obsWG sync.WaitGroup
		obsWG.Add(4)
		for i := 0; i < 4; i++ {
			go func() {
				defer obsWG.Done()
				for {
					select {
					case <-stopObs:
						return
					default:
						s, qerr := p.QueryTask(b.TaskID)
						if qerr != nil {
							t.Errorf("查询被撤销任务失败: %v", qerr)
							return
						}
						if s.Status != TaskQueued && s.Status != TaskCanceled {
							t.Errorf("被撤销排队任务状态非法跳变到 %s", s.Status)
							return
						}
						time.Sleep(time.Millisecond)
					}
				}
			}()
		}

		cancelSnap, err := p.CancelTask(b.TaskID)
		assertOK(t, err, "撤销排队任务 B")
		if cancelSnap.Status != TaskCanceled {
			t.Fatalf("round %d: B 撤销后应立即 canceled，得到 %s", round, cancelSnap.Status)
		}

		// 此时 A 仍占着槽位：再多等两个轮询节拍，确认 B 在槽位紧张时没有偷跑。
		time.Sleep(20 * time.Millisecond)
		if n := h.entries(tokenB); n != 0 {
			t.Fatalf("round %d: 被撤销任务在占槽期间已被执行 %d 次（期望 0）", round, n)
		}

		// 放行 A，唯一槽位腾出：有缺陷的实现会在此刻让 B 真跑一遍。
		close(h.release)
		if final := waitTaskTerminal(t, p, a.TaskID); final.Status != TaskSucceeded {
			t.Fatalf("round %d: 占槽任务 A 应成功，得到 %s", round, final.Status)
		}

		// C：槽位腾出后提交的正常任务，必须立即（按并发上限）拿到槽位并成功，
		// 不能被已撤销的 B 拖住或抢槽。
		c, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn",
			Payload: []byte(fmt.Sprintf(`{"token":%q}`, tokenC))})
		assertOK(t, err, "提交后续正常任务 C")
		if final := waitTaskTerminal(t, p, c.TaskID); final.Status != TaskSucceeded {
			t.Fatalf("round %d: 后续任务 C 应成功，得到 %s (%s)",
				round, final.Status, final.ErrorReason)
		}

		// 给"偷跑"留足时间后做最终判据。
		time.Sleep(30 * time.Millisecond)
		close(stopObs)
		obsWG.Wait()

		if n := h.entries(tokenB); n != 0 {
			t.Fatalf("round %d: 被撤销任务的函数被调用了 %d 次（必须 0 次，绝不执行）", round, n)
		}
		// 被撤销任务不得在实例池留下任何占用：A、C 都已正常结束后 busy 必须为 0。
		if st := p.PoolStats(); st.Busy != 0 {
			t.Fatalf("round %d: 被撤销任务不应占用实例，busy=%d stats=%+v", round, st.Busy, st)
		}
		bf, err := p.QueryTask(b.TaskID)
		assertOK(t, err, "最终查询 B")
		if bf.Status != TaskCanceled || bf.ErrorClass != string(ErrClassCanceled) || bf.Attempts != 0 {
			t.Fatalf("round %d: B 终态必须稳定为 canceled/canceled/attempts=0，得到 %s/%s/attempts=%d",
				round, bf.Status, bf.ErrorClass, bf.Attempts)
		}
		if n := h.entries(tokenC); n != 1 {
			t.Fatalf("round %d: 正常任务 C 应恰好执行 1 次，得到 %d 次", round, n)
		}
	}
	logCase(t, "并发上限=1 ×%d 轮：占槽+排队撤销场景下，被撤销任务函数入口计数恒为 0；A、C 均成功；并发查询未见状态跳变", rounds)
}

func TestTask_ConcurrentDuplicateSubmitSingleSideEffect(t *testing.T) {
	resetSideEffectLog()
	p := newTestPlatform(t, DefaultPoolConfig())
	cfg := testCfg("idempotent-sideeffect")
	cfg.Retry = RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}
	publishTaskFn(t, p, "fn", cfg)

	const n = 16
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			snap, err := p.SubmitTask(&InvokeRequest{
				FunctionName: "fn", Payload: []byte("biz"), IdempotencyKey: "order-42",
			})
			if err != nil {
				t.Errorf("并发提交 %d 失败: %v", i, err)
				return
			}
			ids[i] = snap.TaskID
		}(i)
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("同一业务标识的并发提交必须返回同一凭证: %s vs %s", ids[0], ids[i])
		}
	}
	logCase(t, "%d 个并发重复提交全部得到同一凭证 %s", n, ids[0])

	final := waitTaskTerminal(t, p, ids[0])
	if final.Status != TaskSucceeded {
		t.Fatalf("任务应成功，得到 %s (%s)", final.Status, final.ErrorReason)
	}
	if v, ok := sideEffectLog.Load("order-42"); !ok || v.(int) != 1 {
		t.Fatalf("同一业务标识的副作用必须恰好一次，得到 %v", v)
	}
	var out map[string]any
	_ = json.Unmarshal(final.Payload, &out)
	if out["sideEffect"] != "committed" {
		t.Fatalf("唯一执行应提交副作用，得到 %v", out)
	}
	logCase(t, "并发+重复提交下副作用恰好一次（sideEffectLog[order-42]=1），结论 %s", final.Status)
}

func TestTask_RetryPolicyRequiresIdempotencyKey(t *testing.T) {
	p := newTestPlatform(t, DefaultPoolConfig())
	cfg := testCfg("echo")
	cfg.Retry = RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}
	publishTaskFn(t, p, "retryfn", cfg)
	publishTaskFn(t, p, "oncefn", testCfg("echo"))

	_, err := p.SubmitTask(&InvokeRequest{FunctionName: "retryfn"})
	assertClass(t, err, ErrClassInvalid)
	logCase(t, "策略允许多次尝试但无幂等键 → 明确拒绝（防止重复副作用）")

	snap, err := p.SubmitTask(&InvokeRequest{FunctionName: "retryfn", IdempotencyKey: "k1"})
	assertOK(t, err, "带幂等键的可重试提交")
	if final := waitTaskTerminal(t, p, snap.TaskID); final.Status != TaskSucceeded {
		t.Fatalf("应成功，得到 %s", final.Status)
	}

	snap2, err := p.SubmitTask(&InvokeRequest{FunctionName: "oncefn"})
	assertOK(t, err, "单次尝试策略允许无幂等键提交")
	if final := waitTaskTerminal(t, p, snap2.TaskID); final.Status != TaskSucceeded {
		t.Fatalf("应成功，得到 %s", final.Status)
	}
}

func TestTask_RetryAcrossAttempts(t *testing.T) {
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 2})
	cfg := testCfg("flaky-timeout")
	cfg.Timeout = 60 * time.Millisecond
	cfg.Retry = RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}
	publishTaskFn(t, p, "flaky", cfg)

	snap, err := p.SubmitTask(&InvokeRequest{
		FunctionName: "flaky", IdempotencyKey: "flk",
		Payload: []byte(`{"slowUntilAttempt":2,"slowMs":300,"fastMs":5}`),
	})
	assertOK(t, err, "提交首次超时、二次成功的任务")
	final := waitTaskTerminal(t, p, snap.TaskID)
	if final.Status != TaskSucceeded || final.Attempts != 2 {
		t.Fatalf("首次超时重试后应成功且 attempts=2，得到 %s attempts=%d", final.Status, final.Attempts)
	}
	logCase(t, "尝试 1 超时（实例强杀）→ 尝试 2 新实例成功：status=succeeded attempts=2")
}

func TestTask_TimeoutExhaustsRetries(t *testing.T) {
	p := newTestPlatform(t, DefaultPoolConfig())
	cfg := testCfg("slow")
	cfg.Timeout = 60 * time.Millisecond
	cfg.Retry = RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}
	publishTaskFn(t, p, "fn", cfg)

	snap, err := p.SubmitTask(&InvokeRequest{
		FunctionName: "fn", IdempotencyKey: "to", Payload: []byte(`{"sleepMs":300}`),
	})
	assertOK(t, err, "提交必然超时且重试一次的任务")
	final := waitTaskTerminal(t, p, snap.TaskID)
	if final.Status != TaskTimedOut || final.Attempts != 2 ||
		final.ErrorClass != string(ErrClassTimeout) {
		t.Fatalf("重试耗尽后应为 timed_out/timeout attempts=2，得到 %s/%s attempts=%d",
			final.Status, final.ErrorClass, final.Attempts)
	}
	if st := p.PoolStats(); st.Busy != 0 {
		t.Fatalf("两次超时实例都应被回收，busy 应为 0， %+v", st)
	}
}

func TestTask_QueueFullRejectedExplicitly(t *testing.T) {
	reg := NewRegistry()
	rt := NewRouter()
	runtime := NewRuntime()
	pool := NewInstancePool(runtime, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 1})
	inv := NewInvoker(reg, rt, pool)
	tm := NewTaskManager(inv, TaskManagerConfig{MaxQueued: 1, QueuePollInterval: time.Millisecond})
	t.Cleanup(tm.Stop)
	t.Cleanup(pool.Stop)

	assertOK(t, reg.Register("fn"), "注册 fn")
	cfg := testCfg("slow")
	_, err := reg.Publish("fn", cfg)
	assertOK(t, err, "发布 slow v1")

	a, err := tm.Submit(&InvokeRequest{FunctionName: "fn", Payload: []byte(`{"sleepMs":200}`)})
	assertOK(t, err, "提交任务 A（占满在途上限）")
	_, err = tm.Submit(&InvokeRequest{FunctionName: "fn", Payload: []byte(`{"sleepMs":5}`)})
	assertClass(t, err, ErrClassConcurrency)
	logCase(t, "在途任务超上限 → 提交被显式拒绝 concurrency_limited（不静默丢弃）")

	deadline := time.Now().Add(5 * time.Second)
	for {
		snap, _ := tm.Query(a.TaskID)
		if snap.Status.IsTerminal() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("任务 A 未到达终态")
		}
		time.Sleep(2 * time.Millisecond)
	}
	b, err := tm.Submit(&InvokeRequest{FunctionName: "fn", Payload: []byte(`{"sleepMs":5}`)})
	assertOK(t, err, "在途腾出后可再次提交")
	_ = b
}

func TestTask_UnknownAndInvalidRequests(t *testing.T) {
	p := newTestPlatform(t, DefaultPoolConfig())
	cfg := testCfg("echo")
	cfg.Timeout = 100 * time.Millisecond
	publishTaskFn(t, p, "fn", cfg)

	_, err := p.QueryTask("ghost")
	assertClass(t, err, ErrClassNotFound)
	_, err = p.CancelTask("ghost")
	assertClass(t, err, ErrClassNotFound)
	_, err = p.QueryTask("")
	assertClass(t, err, ErrClassInvalid)
	_, err = p.SubmitTask(&InvokeRequest{FunctionName: "fn", Options: map[string]string{"bogus": "1"}})
	assertClass(t, err, ErrClassInvalid)
	_, err = p.SubmitTask(&InvokeRequest{FunctionName: "fn", CallTimeoutMS: 200})
	assertClass(t, err, ErrClassInvalid)
	_, err = p.SubmitTask(&InvokeRequest{FunctionName: "ghost"})
	assertClass(t, err, ErrClassNotFound)
	// 任务操作不得静默降级为同步执行。
	_, err = p.Invoke(context.Background(), &InvokeRequest{Op: OpTaskSubmit, FunctionName: "fn"})
	assertClass(t, err, ErrClassInvalid)
	logCase(t, "未知凭证/空凭证/未知选项/超宽超时/未知函数/任务操作误走同步入口 → 全部明确拒绝")
}

func TestTask_WireProtocolRoundTrip(t *testing.T) {
	p := newTestPlatform(t, DefaultPoolConfig())
	publishTaskFn(t, p, "fn", testCfg("echo"))

	// v2 异步提交。
	m, err := callRaw(t, p, `{"protocol":"2.0","op":"task.submit","function":"fn","payload":"aGk="}`)
	assertOK(t, err, "v2 异步提交")
	taskID, _ := m["taskId"].(string)
	if taskID == "" {
		t.Fatalf("提交响应必须携带 taskId: %v", m)
	}

	// 轮询查询直到终态。
	var final map[string]any
	deadline := time.Now().Add(5 * time.Second)
	for {
		q, err := callRaw(t, p, `{"protocol":"2.0","op":"task.query","taskId":"`+taskID+`"}`)
		assertOK(t, err, "查询任务")
		if q["taskStatus"] == string(TaskSucceeded) {
			final = q
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("任务未到达终态: %v", q)
		}
		time.Sleep(2 * time.Millisecond)
	}
	payloadB64, _ := final["payload"].(string)
	var echoOut map[string]any
	rawPayload, _ := base64.StdEncoding.DecodeString(payloadB64)
	if err := json.Unmarshal(rawPayload, &echoOut); err != nil || echoOut["echo"] != "hi" {
		t.Fatalf("成功任务应回显函数载荷，得到 %v", final["payload"])
	}
	logCase(t, "线上协议 submit→query 闭环：taskId=%s 终态 %s", taskID, final["taskStatus"])

	// 撤销操作走线上协议。
	m2, err := callRaw(t, p, `{"protocol":"2.0","op":"task.submit","function":"fn"}`)
	assertOK(t, err, "提交待撤销任务")
	id2 := m2["taskId"].(string)
	cm, err := callRaw(t, p, `{"protocol":"2.0","op":"task.cancel","taskId":"`+id2+`"}`)
	assertOK(t, err, "线上撤销")
	if cm["taskId"] != id2 {
		t.Fatalf("撤销响应应携带同一凭证: %v", cm)
	}

	// 非法字段组合与未知 op：全部明确拒绝，不静默降级。
	_, err = callRaw(t, p, `{"protocol":"2.0","op":"task.query","taskId":"x","payload":"aGk="}`)
	assertClass(t, err, ErrClassInvalid)
	_, err = callRaw(t, p, `{"protocol":"2.0","op":"task.query"}`)
	assertClass(t, err, ErrClassInvalid)
	_, err = callRaw(t, p, `{"protocol":"2.0","op":"task.submit","function":"fn","taskId":"x"}`)
	assertClass(t, err, ErrClassInvalid)
	_, err = callRaw(t, p, `{"protocol":"2.0","op":"bogus","function":"fn"}`)
	assertClass(t, err, ErrClassInvalid)
	_, err = callRaw(t, p, `{"protocol":"2.0","op":"task.submit","function":"fn","options":{"bogus":"1"}}`)
	assertClass(t, err, ErrClassInvalid)
	logCase(t, "query 混入调用字段 / 缺 taskId / submit 带 taskId / 未知 op / 未知选项 → invalid_request")

	// 旧版 v1 同步行为保持兼容（无 op 概念）。
	v1, err := callRaw(t, p, `{"protocol":"1.0","function":"fn","payload":"aGk="}`)
	assertOK(t, err, "v1 同步调用保持兼容")
	if v1["ok"] != true {
		t.Fatalf("v1 调用应成功: %v", v1)
	}
}

func TestTask_ConcurrentQueryCancelConsistency(t *testing.T) {
	var late atomic.Int32
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 2})
	assertOK(t, p.RegisterHandler("stubborn", stubbornHandler(&late)), "装载不配合取消的处理器")
	cfg := testCfg("stubborn")
	cfg.Timeout = 5 * time.Second
	publishTaskFn(t, p, "fn", cfg)

	snap, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn", Payload: []byte("block")})
	assertOK(t, err, "提交长任务")
	waitTaskStatus(t, p, snap.TaskID, TaskRunning)

	const queriers = 8
	results := make([]*TaskSnapshot, queriers)
	var wg sync.WaitGroup
	for i := 0; i < queriers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				s, qerr := p.QueryTask(snap.TaskID)
				if qerr != nil {
					t.Errorf("并发查询失败: %v", qerr)
					return
				}
				if s.Status.IsTerminal() {
					results[i] = s
					return
				}
				time.Sleep(time.Millisecond)
			}
		}(i)
	}
	_, err = p.CancelTask(snap.TaskID)
	assertOK(t, err, "并发查询期间撤销")
	wg.Wait()

	for i := 1; i < queriers; i++ {
		if !reflect.DeepEqual(results[0], results[i]) {
			t.Fatalf("并发查询/撤销下终态结论必须自洽:\n%+v\n%+v", results[0], results[i])
		}
	}
	if results[0].Status != TaskCanceled {
		t.Fatalf("撤销后终态应为 canceled，得到 %s", results[0].Status)
	}
	logCase(t, "%d 路并发查询 + 并发撤销：全部观测到同一终态 canceled（-race 守护）", queriers)
}
