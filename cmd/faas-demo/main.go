// Command faas-demo 在本地进程内演示函数计算内核的发布、回滚、
// 超时终止、并发上限、实例回收与失败重试链路。
//
// 运行：
//
//	go run ./cmd/faas-demo
//
// 演示使用纯内存存储；如需查看持久化快照，可在 newPlatform 处传入目录。
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	faas "github.com/highcumontoa/faas-orchestrator-go"
)

func main() {
	p, err := faas.NewPlatform("", faas.PoolConfig{
		IdleTTL:           400 * time.Millisecond,
		MaxIdlePerVersion: 3,
		ReapInterval:      200 * time.Millisecond, // 后台回收协程
	})
	must(err)
	defer p.Close()

	ctx := context.Background()

	section("1. 注册 / 发布 / 路由")
	must(p.RegisterFunction("hello"))
	v1, err := p.Publish("hello", faas.VersionConfig{
		HandlerName: "echo", Timeout: 500 * time.Millisecond,
		MaxConcurrency: 4, Retry: faas.RetryPolicy{MaxAttempts: 1},
	})
	must(err)
	v2, err := p.Publish("hello", faas.VersionConfig{
		HandlerName: "slow", Timeout: 500 * time.Millisecond,
		MaxConcurrency: 4, Retry: faas.RetryPolicy{MaxAttempts: 1},
	})
	must(err)
	fmt.Printf("发布历史：v%d(echo) -> v%d(slow)，当前活动 v%d\n", v1.Version, v2.Version, v2.Version)
	must(p.AddRoute("/hello", "hello", 0))
	showCall(ctx, p, `{"protocol":"2.0","route":"/hello","payload":"`+b64(`{"sleepMs":5}`)+`"}`,
		"正常路由调用（命中活动 v2 的 slow 处理器）")

	section("2. 版本回滚后新调用命中目标版本")
	_, err = p.Rollback("hello", 1)
	must(err)
	showCall(ctx, p, `{"protocol":"2.0","route":"/hello","payload":"`+b64(`"after rollback"`)+`"}`,
		"回滚到 v1 后的新调用必须命中 version=1")
	_, err = p.Rollback("hello", 99)
	showErr(err, "回滚到非法版本 v99（预期 not_found，被拒绝）")

	section("3. 硬超时终止实例，且不污染后续调用")
	must(p.RegisterFunction("sleeper"))
	_, err = p.Publish("sleeper", faas.VersionConfig{
		HandlerName: "slow", Timeout: 100 * time.Millisecond,
		MaxConcurrency: 4, Retry: faas.RetryPolicy{MaxAttempts: 1},
	})
	must(err)
	slowReq := `{"protocol":"2.0","function":"sleeper","payload":"` + b64(`{"sleepMs":300}`) + `"}`
	_, err = p.Call(ctx, []byte(slowReq))
	showErr(err, "sleepMs=300 > timeout=100ms（预期 timeout，实例被强杀）")
	fmt.Printf("超时后池状态: %+v（busy 必须为 0）\n", p.PoolStats())
	showCall(ctx, p, `{"protocol":"2.0","function":"hello","payload":"`+b64(`"still clean"`)+`"}`,
		"后续调用 v1 echo（预期在全新实例上成功）")

	section("4. 并发上限拒绝（不排队、不静默丢弃）")
	must(p.RegisterFunction("limited"))
	release := make(chan struct{})
	var once sync.Once
	must(p.RegisterHandler("blocker", faas.HandlerFunc(func(_ context.Context, _ faas.InvokeContext, _ []byte) ([]byte, error) {
		<-release
		return []byte("released"), nil
	})))
	_, err = p.Publish("limited", faas.VersionConfig{
		HandlerName: "blocker", Timeout: 5 * time.Second,
		MaxConcurrency: 2, Retry: faas.RetryPolicy{MaxAttempts: 1},
	})
	must(err)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = p.Call(ctx, []byte(`{"protocol":"2.0","function":"limited"}`)) }()
	}
	time.Sleep(100 * time.Millisecond) // 等 2 个占满
	_, err = p.Call(ctx, []byte(`{"protocol":"2.0","function":"limited"}`))
	showErr(err, "并发已达上限 2 时第 3 个调用（预期 concurrency_limited）")
	once.Do(func() { close(release) })
	wg.Wait()

	section("5. 失败分类与重试")
	must(p.RegisterFunction("flaky"))
	_, err = p.Publish("flaky", faas.VersionConfig{
		HandlerName: "flaky-timeout", Timeout: 100 * time.Millisecond,
		MaxConcurrency: 4,
		Retry:          faas.RetryPolicy{MaxAttempts: 3, Backoff: 10 * time.Millisecond},
	})
	must(err)
	flakyReq := `{"protocol":"2.0","function":"flaky","payload":"` +
		b64(`{"slowUntilAttempt":2,"slowMs":300,"fastMs":5}`) + `"}`
	showCall(ctx, p, flakyReq, "第 1 次超时被杀 -> 重试在新实例上第 2 次成功（attempts=2）")

	must(p.RegisterFunction("buggy"))
	_, err = p.Publish("buggy", faas.VersionConfig{
		HandlerName: "fail", Timeout: time.Second, MaxConcurrency: 2,
		Retry: faas.RetryPolicy{MaxAttempts: 3},
	})
	must(err)
	_, err = p.Call(ctx, []byte(`{"protocol":"2.0","function":"buggy","payload":"`+b64("x")+`"}`))
	showErr(err, "函数业务错误（function_error，不重试；与 timeout/platform 可区分）")

	section("6. 空闲实例回收")
	// hello@v1 前面累积过空闲实例，等待越过 IdleTTL 后后台回收。
	time.Sleep(900 * time.Millisecond)
	fmt.Printf("越过 IdleTTL(400ms) 后池状态: %+v（后台 reaper 周期 200ms）\n", p.PoolStats())
	n := p.ReapIdle()
	fmt.Printf("手动再回收一轮，终结实例数=%d\n", n)

	section("7. 协议版本兼容")
	showCall(ctx, p, `{"protocol":"1.0","function":"hello","payload":"`+b64(`"legacy client"`)+`"}`,
		"v1 旧客户端：缺失字段取确定默认值，跟随活动版本")
	_, err = p.Call(ctx, []byte(`{"protocol":"2.0","function":"hello","options":{"futureFlag":"1"}}`))
	showErr(err, "v2 携带未声明选项 futureFlag（预期 invalid，显式拒绝而非静默忽略）")

	section("8. 幂等：同键并发只产生一次副作用")
	must(p.RegisterFunction("orders"))
	_, err = p.Publish("orders", faas.VersionConfig{
		HandlerName: "idempotent-sideeffect", Timeout: time.Second,
		MaxConcurrency: 8, Retry: faas.RetryPolicy{MaxAttempts: 1},
	})
	must(err)
	idemReq := `{"protocol":"2.0","function":"orders","idempotencyKey":"order-777","payload":"` + b64(`{"item":"book"}`) + `"}`
	resps := make(chan string, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, cErr := p.Call(ctx, []byte(idemReq))
			if cErr != nil {
				fmt.Println("幂等并发异常:", cErr)
				return
			}
			resps <- string(out)
		}()
	}
	wg.Wait()
	close(resps)
	first := ""
	for r := range resps {
		if first == "" {
			first = r
		} else if r != first {
			fmt.Println("发现不一致响应（不应出现）")
			os.Exit(1)
		}
	}
	pretty, _ := prettyJSON(first)
	fmt.Printf("6 个并发同键调用得到完全一致的结论（副作用仅提交一次）:\n  %s\n", pretty)

	section("9. 异步任务：提交凭证 / 查询终态")
	must(p.RegisterFunction("jobs"))
	_, err = p.Publish("jobs", faas.VersionConfig{
		HandlerName: "echo", Timeout: time.Second,
		MaxConcurrency: 4, Retry: faas.RetryPolicy{MaxAttempts: 1},
	})
	must(err)
	ackRaw, err := p.SubmitAsyncRaw([]byte(`{"protocol":"2.0","mode":"async","function":"jobs",` +
		`"idempotencyKey":"job-1","payload":"` + b64(`"background job"`) + `"}`))
	must(err)
	fmt.Printf("异步提交立即拿到凭证: %s\n", mustOneString(ackRaw))
	ack := mustMap(ackRaw)
	pollAsync(ctx, p, ack["taskId"].(string))

	section("10. 异步：不配合取消也按截止时间强终态 + 回收")
	must(p.RegisterHandler("stubborn", faas.HandlerFunc(func(_ context.Context, _ faas.InvokeContext, _ []byte) ([]byte, error) {
		time.Sleep(5 * time.Second) // 刻意忽略 ctx
		return []byte("late"), nil
	})))
	must(p.RegisterFunction("stubborn"))
	_, err = p.Publish("stubborn", faas.VersionConfig{
		HandlerName: "stubborn", Timeout: 120 * time.Millisecond,
		MaxConcurrency: 2, Retry: faas.RetryPolicy{MaxAttempts: 1},
	})
	must(err)
	stubAck, err := p.SubmitAsyncRaw([]byte(`{"protocol":"2.0","mode":"async","function":"stubborn"}`))
	must(err)
	stubID := mustMap(stubAck)["taskId"].(string)
	fmt.Println("提交了一个睡 5s、完全忽略取消的任务，版本硬截止 120ms ...")
	pollAsync(ctx, p, stubID)
	fmt.Printf("截止后池状态: %+v（busy 必须为 0，实例被强杀不复用）\n", p.PoolStats())

	fmt.Println("\n全部演示链路结束。")
}

// mustOneString 把 JSON 报文压成单行便于打印。
func mustOneString(raw []byte) string {
	v, err := prettyJSON(string(raw))
	if err != nil {
		return string(raw)
	}
	return v
}

// mustMap 解析 JSON 报文为 map。
func mustMap(raw []byte) map[string]any {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		fmt.Fprintln(os.Stderr, "bad json:", err)
		os.Exit(1)
	}
	return m
}

// pollAsync 凭凭证轮询任务直到终态并打印每次观察到的状态。
func pollAsync(_ context.Context, p *faas.Platform, taskID string) {
	for i := 0; i < 200; i++ {
		out, err := p.QueryAsyncRaw([]byte(`{"protocol":"2.0","mode":"async","op":"query","taskId":"` + taskID + `"}`))
		if err != nil {
			fmt.Println("查询失败:", err)
			return
		}
		m := mustMap(out)
		state, _ := m["state"].(string)
		fmt.Printf("  查询 %s -> state=%s\n", taskID, state)
		if state == "succeeded" || state == "failed" || state == "timed_out" || state == "canceled" {
			pretty, _ := prettyJSON(string(out))
			fmt.Printf("  确定终态: %s\n", pretty)
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
}

func section(title string) {
	fmt.Printf("\n==== %s ====\n", title)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func showCall(ctx context.Context, p *faas.Platform, raw, note string) {
	out, err := p.Call(ctx, []byte(raw))
	if err != nil {
		fmt.Printf("[%s] 调用失败: %v\n", note, err)
		return
	}
	pretty, _ := prettyJSON(string(out))
	fmt.Printf("[%s]\n  请求: %s\n  响应: %s\n", note, raw, pretty)
}

func showErr(err error, note string) {
	if err == nil {
		fmt.Printf("[%s] 预期错误但调用成功\n", note)
		return
	}
	fmt.Printf("[%s]\n  %v\n", note, err)
}

func prettyJSON(s string) (string, error) {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return s, nil
	}
	b, err := json.Marshal(v)
	return string(b), err
}
