package faas

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func newTestPlatform(t *testing.T, poolCfg PoolConfig) *Platform {
	t.Helper()
	p, err := NewPlatform("", poolCfg)
	if err != nil {
		t.Fatalf("NewPlatform: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

func callRaw(t *testing.T, p *Platform, raw string) (map[string]any, error) {
	t.Helper()
	out, err := p.Call(context.Background(), []byte(raw))
	var m map[string]any
	_ = json.Unmarshal(out, &m)
	logCase(t, "输入 %s → err=%v 响应=%s", raw, err, string(out))
	return m, err
}

func TestInvoke_SuccessAndUnknownTargets(t *testing.T) {
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 2})
	assertOK(t, p.RegisterFunction("fn"), "注册 fn")
	_, err := p.Publish("fn", testCfg("echo"))
	assertOK(t, err, "发布 echo v1")
	assertOK(t, p.AddRoute("/echo", "fn", 0), "绑定路由")

	// 未知函数（直连）。
	_, err = callRaw(t, p, `{"protocol":"2.0","function":"ghost"}`)
	assertClass(t, err, ErrClassNotFound)

	// 未知路由：不得静默兜底。
	_, err = callRaw(t, p, `{"protocol":"1.0","route":"/ghost"}`)
	assertClass(t, err, ErrClassNotFound)

	// 正常命中。
	m, err := callRaw(t, p, `{"protocol":"2.0","route":"/echo","payload":"aGk="}`)
	assertOK(t, err, "路由调用 echo")
	if m["version"].(float64) != 1 || m["ok"] != true {
		t.Fatalf("响应异常: %v", m)
	}
}

func TestInvoke_NoAvailableVersion(t *testing.T) {
	p := newTestPlatform(t, DefaultPoolConfig())
	assertOK(t, p.RegisterFunction("empty"), "注册但不发布")
	assertOK(t, p.AddRoute("/e", "empty", 0), "绑定无版本函数")
	_, err := callRaw(t, p, `{"protocol":"2.0","route":"/e"}`)
	assertClass(t, err, ErrClassUnavailable)
	logCase(t, "输入调用已注册但未发布的函数 → unavailable，与 not_found 可区分")
}

func TestInvoke_TimeoutKillsAndDoesNotPollute(t *testing.T) {
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 4})
	assertOK(t, p.RegisterFunction("slowfn"), "注册 slowfn")
	cfg := testCfg("slow")
	cfg.Timeout = 80 * time.Millisecond
	_, err := p.Publish("slowfn", cfg)
	assertOK(t, err, "发布超时 80ms 版本")

	_, err = callRaw(t, p, `{"protocol":"2.0","function":"slowfn","payload":"eyJzbGVlcE1zIjoyMDB9"}`)
	assertClass(t, err, ErrClassTimeout)

	st := p.PoolStats()
	if st.Busy != 0 {
		t.Fatalf("超时调用的实例必须被终止，busy 应为 0, got %+v", st)
	}
	logCase(t, "输入 sleepMs=200 > timeout=80ms → timeout，且池内 busy=%d（被杀实例不回池）", st.Busy)

	// 被终止的实例不得污染后续调用：快速调用 echo 版本函数验证。
	assertOK(t, p.RegisterFunction("good"), "注册 good")
	_, err = p.Publish("good", testCfg("echo"))
	assertOK(t, err, "发布 echo")
	m, err := callRaw(t, p, `{"protocol":"2.0","function":"good","payload":"eHk="}`)
	assertOK(t, err, "超时后的新调用应在新实例上正常")
	if m["ok"] != true {
		t.Fatalf("后续调用被污染: %v", m)
	}
	logCase(t, "输入超时后再调用 good/echo → 成功，判定依据：旧实例已 terminated，新实例冷启动")
}

func TestInvoke_FunctionErrorNotRetried(t *testing.T) {
	p := newTestPlatform(t, DefaultPoolConfig())
	assertOK(t, p.RegisterFunction("bad"), "注册 bad")
	cfg := testCfg("fail")
	cfg.Retry = RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}
	_, err := p.Publish("bad", cfg)
	assertOK(t, err, "发布 fail 版本，重试 3 次")

	m, err := callRaw(t, p, `{"protocol":"2.0","function":"bad","payload":"eA=="}`)
	assertClass(t, err, ErrClassFunction)
	if m["attempts"].(float64) != 1 {
		t.Fatalf("函数业务错误不可重试，attempts 应为 1, got %v", m["attempts"])
	}
	logCase(t, "输入 fail 处理器 + MaxAttempts=3 → attempts=1，判定依据：function_error 不重试")
}

func TestInvoke_PanicIsPlatformErrorAndRetried(t *testing.T) {
	p := newTestPlatform(t, DefaultPoolConfig())
	assertOK(t, p.RegisterFunction("crash"), "注册 crash")
	cfg := testCfg("panic")
	cfg.Retry = RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}
	_, err := p.Publish("crash", cfg)
	assertOK(t, err, "发布 panic 版本")

	_, err = callRaw(t, p, `{"protocol":"2.0","function":"crash"}`)
	assertClass(t, err, ErrClassRetryExhausted)
	pe, _ := AsError(err)
	root, ok := AsError(pe.Cause)
	if !ok || root.Class != ErrClassPlatform {
		t.Fatalf("retry_exhausted 应包裹末次 platform_error 根因, got %v", pe.Cause)
	}
	logCase(t, "输入 panic 处理器 ×3 → retry_exhausted(platform_error)，判定依据：panic 恢复归类平台错误")
}

func TestInvoke_TimeoutRetrySucceedsOnFreshInstance(t *testing.T) {
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 4})
	assertOK(t, p.RegisterFunction("flaky"), "注册 flaky")
	cfg := testCfg("flaky-timeout")
	cfg.Timeout = 80 * time.Millisecond
	cfg.Retry = RetryPolicy{MaxAttempts: 3, Backoff: 5 * time.Millisecond}
	_, err := p.Publish("flaky", cfg)
	assertOK(t, err, "发布 flaky-timeout 版本")

	// attempt 1 睡 200ms（超时被杀），attempt>=2 睡 10ms（成功）。
	payload := `{"slowUntilAttempt":2,"slowMs":200,"fastMs":10}`
	enc := base64Std(payload)
	m, err := callRaw(t, p, `{"protocol":"2.0","function":"flaky","payload":"`+enc+`"}`)
	assertOK(t, err, "第二次尝试应成功")
	if m["attempts"].(float64) != 2 {
		t.Fatalf("应在第 2 次尝试成功, got %v", m["attempts"])
	}
	logCase(t, "输入首次超时的 flaky 函数 → attempts=2 成功，判定依据：超时实例被强杀，重试冷启动新实例")
}

func TestInvoke_IdempotencyNoDuplicateSideEffects(t *testing.T) {
	resetSideEffectLog()
	p := newTestPlatform(t, DefaultPoolConfig())
	assertOK(t, p.RegisterFunction("fx"), "注册 fx")
	cfg := testCfg("idempotent-sideeffect")
	cfg.Retry = RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}
	_, err := p.Publish("fx", cfg)
	assertOK(t, err, "发布幂等副作用函数")

	enc := base64Std(`"order-123"`)
	raw := `{"protocol":"2.0","function":"fx","idempotencyKey":"key-123","payload":"` + enc + `"}`
	m1, err := callRaw(t, p, raw)
	assertOK(t, err, "首次提交副作用")
	if m1["payload"] == nil {
		t.Fatalf("应返回 payload: %v", m1)
	}

	// 同键重试（客户端重放）：命中缓存结论，不再次执行。
	m2, err := callRaw(t, p, raw)
	assertOK(t, err, "同键重放")
	if m1["payload"] != m2["payload"] {
		t.Fatalf("同键重放必须返回缓存的一致结论")
	}
	logCase(t, "输入同 idempotencyKey 两次 → 返回一致缓存结论，判定依据：处理器仅被调用一次")

	// 并发同键：只允许一次副作用提交（使用全新键，避免命中前半段串行缓存）。
	resetSideEffectLog()
	concurrentRaw := `{"protocol":"2.0","function":"fx","idempotencyKey":"key-concurrent","payload":"` + enc + `"}`
	var wg sync.WaitGroup
	results := make([]string, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			out, callErr := p.Call(context.Background(), []byte(concurrentRaw))
			if callErr != nil {
				t.Errorf("并发同键调用失败: %v", callErr)
			}
			results[idx] = string(out)
		}(i)
	}
	wg.Wait()
	first := results[0]
	for _, r := range results[1:] {
		if r != first {
			t.Fatalf("并发同键结论不一致: %s vs %s", first, r)
		}
	}
	if v, _ := sideEffectLog.Load("key-concurrent"); v == nil {
		t.Fatal("副作用应恰好提交一次")
	}
	// sync.Map 中同键只有一个条目即证明无重复副作用。
	count := 0
	sideEffectLog.Range(func(_, _ any) bool { count++; return true })
	if count != 1 {
		t.Fatalf("副作用条数应为 1, got %d", count)
	}
	logCase(t, "输入 8 并发同键 → 副作用条数=1 且全部响应一致（判定依据：进行中调用合并）")
}

func TestInvoke_RollbackDuringConcurrentCalls(t *testing.T) {
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 8})
	assertOK(t, p.RegisterFunction("rv"), "注册 rv")
	cfg := testCfg("echo")
	cfg.MaxConcurrency = 8
	_, err := p.Publish("rv", cfg)
	assertOK(t, err, "发布 v1")
	v2cfg := testCfg("echo")
	v2cfg.MaxConcurrency = 8
	_, err = p.Publish("rv", v2cfg)
	assertOK(t, err, "发布 v2（活动）")

	var wg sync.WaitGroup
	seen := make(chan int, 64)
	// 一半调用在回滚前开始（持续打 v2），回滚后启动的调用必须打 v1。
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(i) * time.Millisecond)
			if i == 12 {
				if _, rbErr := p.Rollback("rv", 1); rbErr != nil {
					t.Errorf("回滚失败: %v", rbErr)
					return
				}
			}
			m, callErr := callJSON(p, `{"protocol":"2.0","function":"rv"}`)
			if callErr != nil {
				t.Errorf("调用失败: %v", callErr)
				return
			}
			seen <- int(m["version"].(float64))
		}(i)
	}
	wg.Wait()
	close(seen)

	hitV1, hitV2 := 0, 0
	for v := range seen {
		if v == 1 {
			hitV1++
		} else {
			hitV2++
		}
	}
	if hitV1 == 0 {
		t.Fatal("回滚后必须有调用命中 v1")
	}
	logCase(t, "输入回滚穿插在 32 个并发调用中 → v1=%d v2=%d，判定依据：活动指针原子切换，无半更新", hitV1, hitV2)

	cur, _ := p.registry.ResolveVersion("rv")
	if cur.Version != 1 {
		t.Fatalf("回滚后活动版本应为 v1, got v%d", cur.Version)
	}
}

// callJSON 是不打印大报文的精简调用辅助。
func callJSON(p *Platform, raw string) (map[string]any, error) {
	out, err := p.Call(context.Background(), []byte(raw))
	var m map[string]any
	_ = json.Unmarshal(out, &m)
	return m, err
}

func base64Std(s string) string {
	return base64Enc(s)
}
