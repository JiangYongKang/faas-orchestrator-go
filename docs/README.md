# 本地函数计算内核

该实现使用当前 Go 进程或本地子进程模拟函数运行时，使用内存元数据/实例池与本地 JSONL 文件模拟发布、回滚和调用终态存储。目标是在超时、并发上限、实例回收和版本切换并发发生时，仍然给出明确、可归类、可解释的结论。

## 1. 版本模型

- `Publish(FunctionSpec)` 每次追加一个不可变版本，版本号在同一函数内从 1 单调递增。
- 每次发布都会更新该函数的 `current` 指针；历史版本不会被覆盖，仍可显式调用。
- `Rollback(name, target, route)` 只移动 `current` 指针，不复制或重建历史，因此回滚后的**新当前调用**立即命中目标版本。
- 版本可以被标记为 `unavailable`。回滚或显式调用不可用版本会被拒绝，错误类型为 `unavailable`，不会回退到默认实现。
- 非法版本号、缺失版本、不可用版本分别返回：
  - `invalid_argument`：版本号为 0 或负数；
  - `not_found`：版本号不存在；
  - `unavailable`：版本存在但不可服务。
- 已发布规格中的处理器、超时、并发上限和重试策略会复制到版本快照，调用过程中不会被后续发布修改。

## 2. 路由模型

- 路由必须显式绑定到 `{function, version}`，版本号不能为 0。
- 系统没有默认路由、通配路由或“最新版本”隐式兜底。
- `SetRoute` 对同一 URI 绑定不同目标返回 `conflict`；绑定相同目标是幂等的。
- 已存在的路由需要使用 `ReplaceRoute` 或 `Rollback(..., route)` 显式迁移。
- 已固定到旧版本的路由不会因当前版本变化而漂移；因此版本发布、回滚与灰度路由可以区分：
  - 不传版本调用函数：使用函数当前指针；
  - 显式版本调用：使用指定版本；
  - 路由调用：使用路由固定的版本。

## 3. 调用协议

请求使用 `InvokeRequest`：

```go
InvokeRequest{
    Protocol:       ProtocolV2,       // 空值按 V1 处理
    Function:       "orders",        // 与 Route 二选一
    Version:        2,                // 0 表示当前版本
    Payload:        []byte("..."),
    IdempotencyKey: "order-123",
    Options:        CallOptions{...},
}
```

- 空 `Protocol` 是旧 V1 客户端缺字段的确定兼容规则：按 `faas.protocol/v1` 处理，而不是猜测最新协议。
- V1 不支持每次调用的重试覆盖，传 `Retryable` 或 `MaxAttempts` 返回 `unsupported_option`。
- V2 允许在函数发布规格内缩小超时、并发和最大尝试次数；超过发布规格的覆盖会被拒绝。
- 未知协议返回 `protocol_incompatible`。
- `UnsupportedFields` 中的任何未知选项都返回 `unsupported_option`，不会静默忽略。
- 同时给 `Route` 和 `Function`、两者都不给、负数版本等请求返回 `invalid_argument`。

## 4. 实例生命周期

每个函数版本维护独立实例池，状态只允许以下路径变化：

```text
新建 -> busy -> idle -> busy -> idle -> reclaimed
                 \                 ^
                  +-> draining ----+  (调用结束时直接 reclaimed)
```

- 冷启动：没有空闲实例时创建实例。
- 复用：成功完成且未被标记排空的空闲实例会被下一次同版本调用复用。
- 并发：`Acquire` 在同一个互斥临界区检查并增加 in-flight 计数；达到上限立即返回 `concurrency_limit`，不排队、不预留实例。
- 回收：`ReclaimIdleInstances` 只回收超过 `IdleTTL` 的空闲实例；busy 实例不会被回收。
- 排空：`DrainVersion` 立即回收空闲实例，并把 busy 实例标成 `draining`。
- 排空或回收中的实例不会再被 `Acquire` 选中；busy 实例结束时若已经是 `draining`，释放动作会把它精确回收一次。
- 超时、处理器 panic、处理器返回错误后，实例会被标记退役，释放时直接回收，不进入空闲池，避免污染后续调用。

## 5. 本地运行时与超时

支持两类处理器：

1. **Go 内联处理器**：`Handler HandlerFunc`，接收 `context.Context`。Go 无法从外部强杀 goroutine，因此处理器必须遵守 `ctx.Done()`；内核会取消上下文并立即终止本次调用的等待、释放并发槽、退役实例。
2. **本地子进程处理器**：`Command *LocalCommand`。内核每次尝试启动命令，stdin 接收 JSON 请求信封；超时后通过 `exec.CommandContext` 的 kill 钩子直接杀掉 OS 进程并等待其退出。

子进程输入格式：

```json
{"request_id":"req-...","attempt":1,"idempotency_key":"...","payload":"..."}
```

非零退出码归类为函数错误；无法启动进程归类为平台错误；超时归类为超时错误。

## 6. 失败分类与重试

错误类型由 `faas.ErrorKindOf(err)` 判定：

| 类型 | 含义 | 典型场景 |
| --- | --- | --- |
| `invalid_argument` | 请求或规格不合法 | 空函数名、V1 使用 V2 选项 |
| `not_found` | 目标不存在 | 未知函数、未知路由、历史中无该版本 |
| `conflict` | 目标状态冲突 | 路由重复绑定到不同版本、重复注册 |
| `unavailable` | 目标存在但不能服务 | 无已发布版本、版本被禁用 |
| `timeout` | 执行超时 | 内联上下文到期、子进程被杀 |
| `concurrency_limit` | 超过函数版本并发上限 | in-flight 已满 |
| `function_error` | 函数自身失败 | handler 返回错误/panic、命令非零退出 |
| `platform_error` | 平台内部失败 | 本地事件存储写盘失败、进程无法启动 |
| `retry_exhausted` | 多次尝试后仍失败 | 最后一次原因保存在错误链中 |
| `unsupported_option` | 明确不支持的选项 | 未知字段、超出发布规格 |
| `protocol_incompatible` | 协议版本不兼容 | 未知协议名 |

重试规则：

- 发布版本中的 `RetryPolicy.Retryable` 是权威配置；未列出的失败类默认不重试。
- `ShouldRetry` 在每次失败后检查失败类别和 `MaxAttempts`。
- 限流、超时等是否重试必须显式配置；业务函数错误默认不重试。
- 只要最大尝试次数大于 1，就必须提供 `IdempotencyKey`，否则请求在执行前被拒绝。
- 幂等键和尝试编号会传入 `InvocationContext` 与子进程 JSON，函数必须用该键保证自身外部副作用去重。
- 相同 `{key, function, version}` 的并发调用只执行一次，其他调用等待并共享同一成功或失败终态。平台不把没有幂等键的请求自动合并。

## 7. 本地存储审计

`OpenLocalStore(path)` 将事件追加到 JSON Lines 文件：

- `published`：函数、版本、状态和资源限制；
- `rollback`：函数和新的当前版本；
- `invoke`：终态、尝试次数和实例 ID。

存储为空时内核仍可纯内存运行。若显式提供存储但追加/同步失败，发布或调用返回 `platform_error`，不会吞掉写盘失败。

## 8. 并发一致性

- 注册表、路由表、实例池和幂等表分别通过锁保护状态转换。
- 版本解析得到完整快照后才执行函数，因此调用不会读到半个 `FunctionSpec`。
- 回滚与调用并发时，每个调用只会得到旧或新的完整版本；响应版本号与输出必须匹配。
- 版本切换不会强制中断已解析版本的进行中调用；调用结束前持有的是该版本快照。
- 实例池的 inflight 增减、busy/draining/reclaimed 状态变化和回收删除在同一个实例池锁下完成。
- `go test -race` 已用于验证实现与测试。

## 9. 验证方法

运行全量测试：

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```

重点测试文件：

- `faas/registry_test.go`：发布历史、回滚、非法/不可用版本；
- `faas/route_protocol_test.go`：未知路由、无默认路由、路由冲突、协议兼容；
- `faas/resource_test.go`：超时终止、并发上限、函数错误分类；
- `faas/instance_test.go`：实例复用、空闲回收、busy 保护；
- `faas/retry_idempotency_test.go`：不重试、限流重试、并发幂等；
- `faas/concurrency_test.go`：回滚并发一致性、排空后释放回收；
- `faas/runtime_test.go`：本地子进程超时强杀和非零退出；
- `faas/storage_test.go`：JSONL 审计记录与存储失败传播。

测试日志使用标准 `go test -v` 输出，每条决策日志都包含调用输入 `input=...` 和判定依据 `reason=...`。

运行本地演示：

```bash
go run ./cmd/faasdemo
```

演示会发布两个版本、绑定固定路由、回滚当前版本、验证固定路由不漂移，并展示并发上限错误和事件日志路径。
