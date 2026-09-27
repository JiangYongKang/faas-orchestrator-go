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

	section("9. 异步任务：提交即得凭证，后台执行，凭凭证查询确定结论")
	must(p.RegisterFunction("jobs"))
	_, err = p.Publish("jobs", faas.VersionConfig{
		HandlerName: "slow", Timeout: 200 * time.Millisecond,
		MaxConcurrency: 2, Retry: faas.RetryPolicy{MaxAttempts: 1},
	})
	must(err)

	// 9.1 提交后立即返回任务凭证，调用方不阻塞。
	taskID := submitTask(ctx, p, `{"protocol":"2.0","op":"task.submit","function":"jobs","payload":"`+b64(`{"sleepMs":50}`)+`"}`,
		"提交 50ms 任务（立即返回凭证）")
	waitTask(ctx, p, taskID, "轮询查询直到终态（预期 succeeded）")

	// 9.2 超过单次执行截止时间：函数不配合也能在有限时间内看到 timed_out。
	timeoutID := submitTask(ctx, p, `{"protocol":"2.0","op":"task.submit","function":"jobs","payload":"`+b64(`{"sleepMs":800}`)+`"}`,
		"提交 800ms 任务（版本超时 200ms）")
	waitTask(ctx, p, timeoutID, "轮询查询直到终态（预期 timed_out，实例被强杀回收）")
	fmt.Printf("超时回收后池状态: %+v（busy 必须为 0）\n", p.PoolStats())

	// 9.3 调用方主动撤销：查询方在有限时间内看到 canceled。
	cancelID := submitTask(ctx, p, `{"protocol":"2.0","op":"task.submit","function":"jobs","payload":"`+b64(`{"sleepMs":800}`)+`"}`,
		"提交 800ms 任务（随后立即撤销）")
	out, err := p.Call(ctx, []byte(`{"protocol":"2.0","op":"task.cancel","taskId":"`+cancelID+`"}`))
	must(err)
	fmt.Printf("撤销响应: %s\n", string(out))
	waitTask(ctx, p, cancelID, "轮询查询直到终态（预期 canceled）")

	// 9.4 重复提交同一业务标识：返回同一凭证，副作用最多一次。
	must(p.RegisterFunction("async-orders"))
	_, err = p.Publish("async-orders", faas.VersionConfig{
		HandlerName: "idempotent-sideeffect", Timeout: time.Second,
		MaxConcurrency: 4, Retry: faas.RetryPolicy{MaxAttempts: 2, Backoff: 5 * time.Millisecond},
	})
	must(err)
	dupReq := `{"protocol":"2.0","op":"task.submit","function":"async-orders","idempotencyKey":"order-888","payload":"` + b64(`{"item":"book"}`) + `"}`
	id1 := submitTask(ctx, p, dupReq, "首次提交 order-888")
	id2 := submitTask(ctx, p, dupReq, "重复提交 order-888（预期同一凭证）")
	fmt.Printf("两次提交凭证一致: %v（%s）\n", id1 == id2, id1)
	waitTask(ctx, p, id1, "等待去重任务终态（副作用恰好一次）")

	// 9.5 策略允许多次尝试但缺幂等键：明确拒绝，不进入可能重复副作用的执行。
	_, err = p.Call(ctx, []byte(`{"protocol":"2.0","op":"task.submit","function":"async-orders"}`))
	showErr(err, "可重试策略下缺少 idempotencyKey（预期 invalid_request）")

	fmt.Println("\n全部演示链路结束。")
}

// submitTask 通过线上协议提交异步任务并打印凭证。
func submitTask(ctx context.Context, p *faas.Platform, raw, note string) string {
	out, err := p.Call(ctx, []byte(raw))
	if err != nil {
		fmt.Printf("[%s] 提交失败: %v\n", note, err)
		os.Exit(1)
	}
	var sub struct {
		TaskID string `json:"taskId"`
		Status string `json:"taskStatus"`
	}
	if err := json.Unmarshal(out, &sub); err != nil || sub.TaskID == "" {
		fmt.Printf("[%s] 提交响应异常: %s\n", note, string(out))
		os.Exit(1)
	}
	fmt.Printf("[%s]\n  凭证: %s（提交时状态 %s，调用方未阻塞）\n", note, sub.TaskID, sub.Status)
	return sub.TaskID
}

// waitTask 轮询查询直到任务进入终态并打印结论。
func waitTask(ctx context.Context, p *faas.Platform, taskID, note string) {
	for {
		out, err := p.Call(ctx, []byte(`{"protocol":"2.0","op":"task.query","taskId":"`+taskID+`"}`))
		if err != nil {
			fmt.Printf("[%s] 查询失败: %v\n", note, err)
			os.Exit(1)
		}
		var q struct {
			Status string `json:"taskStatus"`
			Class  string `json:"errorClass"`
		}
		_ = json.Unmarshal(out, &q)
		switch faas.TaskStatus(q.Status) {
		case faas.TaskSucceeded, faas.TaskFailed, faas.TaskTimedOut, faas.TaskCanceled:
			fmt.Printf("[%s]\n  终态: %s %s\n", note, q.Status, q.Class)
			return
		}
		time.Sleep(10 * time.Millisecond)
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
