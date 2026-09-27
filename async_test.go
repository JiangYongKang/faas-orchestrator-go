package faas

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// waitForState 轮询任务直到进入任一目标状态或超时，返回最终快照。
func waitForState(t *testing.T, p *Platform, taskID string, want ...TaskState) *TaskSnapshot {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snap, err := p.QueryAsync(taskID)
		if err != nil {
			t.Fatalf("查询任务 %s 失败: %v", taskID, err)
		}
		for _, w := range want {
			if snap.State == w {
				logCase(t, "任务 %s 到达状态 %q（attempts=%d, errorClass=%q）",
					taskID, snap.State, snap.Attempts, snap.ErrorClass)
				return snap
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	snap, _ := p.QueryAsync(taskID)
	t.Fatalf("任务 %s 未在时限内到达期望状态 %v，当前 %+v", taskID, want, snap)
	return nil
}

func TestAsync_BasicSuccessAndStableCredential(t *testing.T) {
	p := newTestPlatform(t, PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 4})
	assertOK(t, p.RegisterFunction("afn"), "注册 afn")
	_, err := p.Publish("afn", testCfg("echo"))
	assertOK(t, err, "发布 echo")

	id, err := p.SubmitAsync(&InvokeRequest{FunctionName: "afn", Payload: []byte(`"hi"`)})
	assertOK(t, err, "异步提交")
	if id == "" {
		t.Fatal("提交必须返回稳定任务凭证")
	}
	snap := waitForState(t, p, id, TaskSucceeded)
	if snap.FunctionName != "afn" || snap.Version != 1 {
		t.Fatalf("结论目标异常: %+v", snap)
	}
	var m map[string]any
	if err := json.Unmarshal(snap.Payload, &m); err != nil {
		t.Fatalf("成功载荷应可解析: %s", snap.Payload)
	}
	if m["echo"] != `"hi"` {
		t.Fatalf("载荷回显异常: %v", m)
	}
	// 同一凭证反复查询始终得到同一份确定结论（逐字节一致）。
	for i := 0; i < 5; i++ {
		s2, qErr := p.QueryAsync(id)
		assertOK(t, qErr, "重复查询")
		if s2.State != TaskSucceeded || string(s2.Payload) != string(snap.Payload) ||
			s2.Attempts != snap.Attempts || s2.InstanceID != snap.InstanceID {
			t.Fatalf("第 %d 次查询结论不稳定: %+v vs %+v", i, s2, snap)
		}
	}
}

func TestAsync_UnknownCredentialAndStatesDistinguishable(t *testing.T) {
	p := newTestPlatform(t, DefaultPoolConfig())
	_, err := p.QueryAsync("task-ghost")
	assertClass(t, err, ErrClassNotFound)
	_, err = p.CancelAsync("task-ghost")
	assertClass(t, err, ErrClassNotFound)
}

func TestAsync_FunctionErrorIsFailedDistinctFromTimeoutAndCancel(t *testing.T) {
	p := newTestPlatform(t, DefaultPoolConfig())
	assertOK(t, p.RegisterFunction("abad"), "注册 abad")
	cfg := testCfg("fail")
	cfg.Retry = RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond} // 业务错误也不重试
	_, err := p.Publish("abad", cfg)
	assertOK(t, err, "发布 fail（重试 3 次但业务错误不重试）")

	id, err := p.SubmitAsync(&InvokeRequest{
		FunctionName: "abad", IdempotencyKey: "biz-fail-1", Payload: []byte(`"x"`)})
	assertOK(t, err, "提交")
	snap := waitForState(t, p, id, TaskFailed)
	if snap.ErrorClass != ErrClassFunction || snap.Attempts != 1 {
		t.Fatalf("函数自身失败应 failed/function_error/attempts=1, got %+v", snap)
	}
}

func TestAsync_RetryWithoutDedupKeyRejected(t *testing.T) {
	p := newTestPlatform(t, DefaultPoolConfig())
	assertOK(t, p.RegisterFunction("ar"), "注册 ar")
	cfg := testCfg("echo")
	cfg.Retry = RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}
	_, err := p.Publish("ar", cfg)
	assertOK(t, err, "发布允许重试的版本")

	// 无业务操作标识 + 允许重试：必须明确拒绝，不能进入可能重复副作用的执行。
	_, err = p.SubmitAsync(&InvokeRequest{FunctionName: "ar"})
	assertClass(t, err, ErrClassInvalid)
	logCase(t, "允许重试但缺少 idempotencyKey 的异步提交 → invalid_request 被拒绝")

	// 同一版本在提供业务操作标识时必须被受理。
	id, err := p.SubmitAsync(&InvokeRequest{FunctionName: "ar", IdempotencyKey: "biz-1"})
	assertOK(t, err, "带业务操作标识的提交应受理")
	waitForState(t, p, id, TaskSucceeded)
}

func TestAsync_ProtocolRejections(t *testing.T) {
	p := newTestPlatform(t, DefaultPoolConfig())
	assertOK(t, p.RegisterFunction("pf"), "注册 pf")
	_, err := p.Publish("pf", testCfg("echo"))
	assertOK(t, err, "发布")

	// v1 协议不得静默降级为同步：明确拒绝。
	_, err = p.SubmitAsyncRaw([]byte(`{"protocol":"1.0","function":"pf"}`))
	assertClass(t, err, ErrClassInvalid)

	// v2 但缺少 mode=async（误用同步报文形态）：明确拒绝。
	_, err = p.SubmitAsyncRaw([]byte(`{"protocol":"2.0","function":"pf"}`))
	assertClass(t, err, ErrClassInvalid)

	// 未知选项不静默忽略。
	_, err = p.SubmitAsyncRaw([]byte(`{"protocol":"2.0","mode":"async","function":"pf","options":{"future":"1"}}`))
	assertClass(t, err, ErrClassInvalid)

	// 未知顶层字段严格拒绝。
	_, err = p.SubmitAsyncRaw([]byte(`{"protocol":"2.0","mode":"async","function":"pf","surprise":1}`))
	assertClass(t, err, ErrClassInvalid)

	// 未知函数：not_found，可区分。
	_, err = p.SubmitAsyncRaw([]byte(`{"protocol":"2.0","mode":"async","function":"ghost"}`))
	assertClass(t, err, ErrClassNotFound)

	// 查询报文 op 非法 / 缺 taskId 被拒绝。
	_, err = p.QueryAsyncRaw([]byte(`{"protocol":"2.0","mode":"async","op":"frob","taskId":"t"}`))
	assertClass(t, err, ErrClassInvalid)
	_, err = p.CancelAsyncRaw([]byte(`{"protocol":"2.0","mode":"async","op":"cancel"}`))
	assertClass(t, err, ErrClassInvalid)
}

// 保持 context 包被引用（后续用例使用）。
var _ = context.Background
var _ sync.WaitGroup

// newBlockedAsyncPlatform 装配一个注册了阻塞处理器函数 fn 的平台：
// 该函数并发上限为 maxConc，处理器在 done 关闭前一直占着实例（用于确定性排队测试）。
// 返回的 done 由调用方在测试结束时关闭以释放占槽任务。
func newBlockedAsyncPlatform(t *testing.T, fn string, maxConc int) (*Platform, chan struct{}) {
	t.Helper()
	p, err := NewPlatformWithAsyncConfig("", PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 4},
		AsyncConfig{MaxQueuedPerVersion: 8, DispatchInterval: 2 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewPlatformWithAsyncConfig: %v", err)
	}
	t.Cleanup(p.Close)
	release := make(chan struct{})
	blocker := HandlerFunc(func(_ context.Context, _ InvokeContext, _ []byte) ([]byte, error) {
		<-release
		return []byte("released"), nil
	})
	if err := p.RegisterHandler("async-blocker-"+fn, blocker); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	assertOK(t, p.RegisterFunction(fn), "注册 "+fn)
	cfg := testCfg("async-blocker-" + fn)
	cfg.MaxConcurrency = maxConc
	cfg.Timeout = 10 * time.Second
	if _, err := p.Publish(fn, cfg); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return p, release
}

func TestAsync_RawProtocolAckAndStatus(t *testing.T) {
	p := newTestPlatform(t, DefaultPoolConfig())
	assertOK(t, p.RegisterFunction("rp"), "注册 rp")
	_, err := p.Publish("rp", testCfg("echo"))
	assertOK(t, err, "发布")

	ack, err := p.SubmitAsyncRaw([]byte(
		`{"protocol":"2.0","mode":"async","function":"rp","idempotencyKey":"rp-1","payload":"ImFiIg=="}`))
	assertOK(t, err, "异步提交受理")
	var ackM map[string]any
	if err := json.Unmarshal(ack, &ackM); err != nil {
		t.Fatal(err)
	}
	if ackM["mode"] != "async" || ackM["ok"] != true || ackM["taskId"] == "" || ackM["state"] != "queued" {
		t.Fatalf("凭证报文字段异常: %v", ackM)
	}
	taskID := ackM["taskId"].(string)
	logCase(t, "异步提交凭证报文: %s", string(ack))

	waitForState(t, p, taskID, TaskSucceeded)
	qraw, err := p.QueryAsyncRaw([]byte(
		`{"protocol":"2.0","mode":"async","op":"query","taskId":"` + taskID + `"}`))
	assertOK(t, err, "原始报文查询")
	var qm map[string]any
	if err := json.Unmarshal(qraw, &qm); err != nil {
		t.Fatal(err)
	}
	if qm["state"] != "succeeded" || qm["ok"] != true || qm["op"] != "query" {
		t.Fatalf("状态报文异常: %v", qm)
	}
	if qm["payload"] == nil {
		t.Fatalf("成功状态应回显函数载荷: %v", qm)
	}
	logCase(t, "原始报文查询结论: %s", string(qraw))
}

func TestAsync_TimeoutMSCanOnlyTighten(t *testing.T) {
	p := newTestPlatform(t, DefaultPoolConfig())
	assertOK(t, p.RegisterFunction("tm"), "注册 tm")
	cfg := testCfg("slow")
	cfg.Timeout = 200 * time.Millisecond
	_, err := p.Publish("tm", cfg)
	assertOK(t, err, "发布版本超时 200ms")

	// timeoutMs 放宽超过版本配置：明确拒绝。
	_, err = p.SubmitAsyncRaw([]byte(
		`{"protocol":"2.0","mode":"async","function":"tm","timeoutMs":5000,"idempotencyKey":"tm-bad"}`))
	assertClass(t, err, ErrClassInvalid)

	// timeoutMs 收紧到 40ms，函数睡 200ms：必须 timed_out 而非等到 200ms。
	start := time.Now()
	ack, err := p.SubmitAsyncRaw([]byte(
		`{"protocol":"2.0","mode":"async","function":"tm","timeoutMs":40,` +
			`"idempotencyKey":"tm-ok","payload":"eyJzbGVlcE1zIjoyMDB9"}`))
	assertOK(t, err, "收紧超时的提交受理")
	var ackM map[string]any
	_ = json.Unmarshal(ack, &ackM)
	id := ackM["taskId"].(string)
	waitForState(t, p, id, TaskTimedOut)
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("应按收紧后的 40ms 截止，实际 %v", elapsed)
	}
}

func TestAsync_SyncBehaviorUnchanged(t *testing.T) {
	// 回归守护：同步 v1/v2 调用行为与结论必须保持兼容。
	p := newTestPlatform(t, DefaultPoolConfig())
	assertOK(t, p.RegisterFunction("sy"), "注册 sy")
	_, err := p.Publish("sy", testCfg("echo"))
	assertOK(t, err, "发布")

	m, err := callRaw(t, p, `{"protocol":"1.0","function":"sy","payload":"eHk="}`)
	assertOK(t, err, "v1 同步调用仍工作")
	if m["ok"] != true {
		t.Fatalf("v1 同步结论异常: %v", m)
	}
	if _, hasAttempts := m["attempts"]; hasAttempts {
		t.Fatalf("v1 同步响应不得出现 attempts 等新字段: %v", m)
	}
	m2, err := callRaw(t, p, `{"protocol":"2.0","function":"sy","payload":"eHk="}`)
	assertOK(t, err, "v2 同步调用仍工作")
	if m2["ok"] != true || m2["attempts"] == nil {
		t.Fatalf("v2 同步结论异常: %v", m2)
	}
}
