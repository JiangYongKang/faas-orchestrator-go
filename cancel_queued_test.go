package faas

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// waitForCount 轮询等待原子计数器到达期望值，用于稳定建立处理器侧的
// happens-before（观测到 running 不等于处理器内副作用计数已自增）。
func waitForCount(t *testing.T, c *atomic.Int32, want int32, within time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if c.Load() == want {
			logCase(t, "%s（判定依据：计数器==%d）", what, want)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s：期望计数 %d，实际 %d", what, want, c.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// countedBlockerHandler 用调用计数记录副作用：payload 为 "block" 时占住槽位
// 直到 release 关闭；其余调用立即成功并按 payload 分类计数。
func countedBlockerHandler(
	total, bCalls *atomic.Int32, release <-chan struct{},
) HandlerFunc {
	return func(ctx context.Context, _ InvokeContext, payload []byte) ([]byte, error) {
		total.Add(1)
		if string(payload) == "block" {
			select {
			case <-release:
				return []byte("a-done"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if string(payload) == "b" {
			bCalls.Add(1) // 被撤销任务 B 的"业务动作"：期望永远为 0
		}
		return []byte("fast:" + string(payload)), nil
	}
}

// TestTask_CanceledQueuedTaskNeverExecutes 稳定复现并守护以下保证：
// 并发上限=1 时，排队中（从未开始执行）被撤销的任务——
//  1. 函数从头到尾一次都不会被调用（调用计数恒为 0）；
//  2. 状态从撤销一刻起稳定停在 canceled，不会再跳到 running/succeeded；
//  3. 不抢占并发槽位、不拖住后续排队任务，槽位腾出后后续任务正常成功。
func TestTask_CanceledQueuedTaskNeverExecutes(t *testing.T) {
	var totalCalls, bCalls atomic.Int32
	release := make(chan struct{})
	t.Cleanup(func() {
		// 兜底释放，避免异常路径下占槽协程泄漏。
		select {
		case <-release:
		default:
			close(release)
		}
	})

	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 2})
	t.Cleanup(p.Close)
	assertOK(t, p.RegisterHandler("counted-blocker",
		countedBlockerHandler(&totalCalls, &bCalls, release)), "装载计数处理器")
	cfg := testCfg("counted-blocker")
	cfg.MaxConcurrency = 1 // 关键：唯一并发槽位
	publishTaskFn(t, p, "fn", cfg)

	// A：占住唯一槽位，直到测试显式放行。
	a, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn", Payload: []byte("block")})
	assertOK(t, err, "提交占槽任务 A")
	waitTaskStatus(t, p, a.TaskID, TaskRunning)
	waitForCount(t, &totalCalls, 1, 2*time.Second, "任务 A 处理器已进入并计数")

	// B：槽位已满，稳定排队。
	b, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn", Payload: []byte("b")})
	assertOK(t, err, "提交排队任务 B")
	waitTaskStatus(t, p, b.TaskID, TaskQueued)
	logCase(t, "任务 B 已受理并停在 queued（A 占满唯一槽位）")

	// C：排在 B 后面的正常任务，同样排队（验证撤销不拖住后续任务）。
	c, err := p.SubmitTask(&InvokeRequest{FunctionName: "fn", Payload: []byte("c")})
	assertOK(t, err, "提交后续排队任务 C")

	// 立刻撤销 B（B 从未开始执行）。
	cs, err := p.CancelTask(b.TaskID)
	assertOK(t, err, "撤销排队中的任务 B")
	if cs.Status != TaskCanceled {
		t.Fatalf("撤销后应立即落定 canceled，得到 %s", cs.Status)
	}
	bf := waitTaskTerminal(t, p, b.TaskID)
	if bf.Status != TaskCanceled || bf.Attempts != 0 {
		t.Fatalf("B 应为 canceled 且 attempts=0（从未尝试执行），得到 %s attempts=%d",
			bf.Status, bf.Attempts)
	}
	logCase(t, "B 撤销后立即终态 canceled/attempts=0，此时 A 仍占着槽位")

	// 放行 A：槽位腾出。若存在缺陷，排队协程会在此刻抢到槽位执行 B。
	close(release)
	af := waitTaskTerminal(t, p, a.TaskID)
	if af.Status != TaskSucceeded {
		t.Fatalf("占槽任务 A 应正常成功，得到 %s", af.Status)
	}

	// 在默认 5ms 轮询间隔下，留足让"错误执行 B"暴露的窗口（数百个轮询周期）。
	time.Sleep(200 * time.Millisecond)
	if n := bCalls.Load(); n != 0 {
		t.Fatalf("被撤销任务 B 的函数绝不能被调用，期望 0 次，实际 %d 次", n)
	}
	logCase(t, "槽位腾出后观察 200ms：B 调用计数=0（B 从未执行、未占实例）；正常任务 C 可先行")

	// B 的状态必须稳定停在 canceled：密集查询不得观测到任何跳变。
	for i := 0; i < 100; i++ {
		s, qerr := p.QueryTask(b.TaskID)
		if qerr != nil {
			t.Fatalf("查询 B: %v", qerr)
		}
		if s.Status != TaskCanceled {
			t.Fatalf("已撤销任务状态发生跳变：canceled -> %s", s.Status)
		}
		time.Sleep(time.Millisecond)
	}

	// C 不受 B 影响，按并发上限正常执行并成功。
	cf := waitTaskTerminal(t, p, c.TaskID)
	if cf.Status != TaskSucceeded {
		t.Fatalf("后续任务 C 应成功（撤销任务不拖住后续），得到 %s", cf.Status)
	}
	if n := bCalls.Load(); n != 0 {
		t.Fatalf("被撤销任务 B 最终仍被调用 %d 次", n)
	}
	if n := totalCalls.Load(); n != 2 {
		t.Fatalf("全程应只有 A、C 各执行一次，总调用=2，实际 %d", n)
	}
	logCase(t, "后续任务 C 成功，全程总调用=2（A+C），B 调用=0：撤销不抢槽、不拖队")
}
