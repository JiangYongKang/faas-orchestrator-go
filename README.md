# faas-orchestrator-go：本地函数计算平台内核

用**本地进程（实例状态机 + goroutine）与本地存储（原子 JSON 快照）**模拟函数计算平台内核，
聚焦在异常链路下结论的**一致性与可解释性**：并发调用、超时终止、实例回收、版本回滚、
失败重试、协议版本演进。

## 1. 快速开始

```bash
# 端到端演示（发布/回滚/超时/并发/回收/重试/幂等/协议兼容/异步凭证/异步截止强终态）
go run ./cmd/faas-demo

# 全量单元测试（以边界与异常路径为主，含异步状态机/去重/截止/回收）
go test ./... -count=1 -v

# 只跑异步链路
go test ./... -count=1 -v -run TestAsync

# 并发正确性（数据竞争检测，异步任务调度/取消/回收并发路径均覆盖）
go test ./... -race -count=1
```

测试日志统一以 `[判定依据]` 打印**调用输入**与平台的**判定理由**，便于核对每条异常路径。

## 2. 错误分类模型（结论可区分、可解释）

所有对外错误都是 `*faas.Error{Class, Reason, Cause}`，类别枚举（`errors.go`）：

| Class | 触发场景 | 是否重试 |
|---|---|---|
| `not_found` | 函数未注册、版本不存在、路由不存在、发布产物缺失 | 否 |
| `invalid_request` | 非法版本号、非法配置、非法协议字段/未知选项、非法路由参数 | 否 |
| `conflict` | 函数重复注册、路由绑定冲突、版本状态非法迁移 | 否 |
| `unavailable` | 无已发布版本、目标版本被 `disabled` / `retired` | 否 |
| `timeout` | 单次执行超过版本硬超时；调用方 ctx 取消 | 是 |
| `concurrency_limited` | 版本在执并发达到 `MaxConcurrency`（立即拒绝，不排队） | 是 |
| `function_error` | 函数处理器**主动返回**的业务错误（含无效载荷等） | 否 |
| `platform_error` | 处理器 panic（模拟进程崩溃）、实例在回收竞争中失效、存储故障 | 是 |
| `retry_exhausted` | 可重试错误**实际发生过重试**且策略耗尽后的终态包装（`Cause` 保留末次根因） | 终态 |

关键区分原则：

- `MaxAttempts=1`（根本没有重试）时，错误保留原始分类（如 `timeout`），**不会**被包装成
  `retry_exhausted` 而掩盖根因。
- 处理器 `panic` 与处理器 `return error` 严格分离：前者是平台侧进程崩溃（`platform_error`，
  实例被销毁、可重试），后者是业务失败（`function_error`，实例可复用、不重试）。
- `not_found` 与 `unavailable` 分离："从未存在"与"存在但当前不可用"是两种不同结论。

## 3. 版本与路由模型

### 3.1 注册 / 发布 / 回滚（`registry.go`）

- `Register(name)`：函数名唯一，重复注册返回 `conflict`。
- `Publish(cfg)`：版本号**单调递增**（1,2,3…），版本配置一经发布**不可变**；
  新版本自动成为活动版本。发布配置严格校验（`handler_name` 非空、`timeout>0`、
  `max_concurrency>=1`、重试参数合法），非法配置返回 `invalid_request`，不接受静默默认。
- 每次 publish / rollback / 状态变更都向**不可变历史** `History` 追加 `PublishRecord`。
- `Rollback(name, v)` 只切换活动指针，不复制/重建版本：
  - 版本号 `<1` → `invalid_request`；版本不存在 → `not_found`；
  - 目标 `disabled` / `retired` → `unavailable`（原因文本可区分下线与废弃）；
  - 目标已经是活动版本 → `invalid_request`；
  - 成功后**新调用立即命中目标版本**（指针在写锁内原子切换，无半更新）。
- 版本状态机：`active ⇄ disabled`、任意活跃态 → `retired`（终态，永久不可再启用/回滚）。
  下线活动指针所指版本本身被允许（模拟"停服"），此后跟随指针的调用得到 `unavailable`，
  而不是静默落到其他版本。

### 3.2 路由（`router.go`）

- 路由项：`{path, function, versionPin}`。`versionPin=0` 跟随函数当前活动版本；
  `versionPin>0` 固定钉选某版本（钉选目标在调用时仍校验可用性）。
- 未知路径返回 `not_found`，**没有任何默认兜底实现**。
- 对同一路径绑定**不同目标**返回 `conflict`，原绑定保持不变；完全相同的重复登记幂等成功。
- 路由解析发生在调用最前端，因此"未知路由"与"函数存在但无可用版本"分别报
  `not_found(route)` 与 `unavailable`。

## 4. 调用协议的版本兼容（`protocol.go`）

线上报文为 JSON，按 `protocol` 字段区分代际：

- **v1（`"1.0"`）**：只有 `route|function` 与 `payload`。缺失字段（版本钉选、超时、
  幂等键、选项）在解码归一化时取**唯一权威的确定默认值**（0/""/nil），
  不会因默认取值变化导致结论漂移。
- **v2（`"2.0"`）**：增加 `version`、`timeoutMs`、`idempotencyKey`、`options`。
  - `timeoutMs` 只能**收紧**不能放宽版本配置超时，超出即 `invalid_request`；
  - `options` 走白名单（当前仅 `trace=on|off`），任何未知选项 / 非法值都被
    **明确拒绝而非静默忽略**；
  - `route` 与 `function` 互斥，路由调用不允许携带 `version` 钉选。
- 响应编码同样按代际归一化：v1 响应不出现 `attempts`/`instanceId` 等新字段，
  但 `ok/errorClass/errorReason` 在两个版本中都稳定存在；**失败响应也携带 `attempts`**，
  使重试次数在结论中可判定。
- 未知协议版本、缺失 `protocol`、非法 JSON 一律 `invalid_request`。

## 5. 实例生命周期（`instance.go` / `pool.go`）

### 5.1 实例状态机

```
            Acquire(复用/冷启动即服务)
   idle ───────────────────────────────▶ busy
    ▲                                    │  │
    │ Release(正常归还)                  │  │ 超时 / panic / ctx取消
    │                                    ▼  ▼
    └─────────────── busy(带回收标记) ─ draining ─ terminate ─▶ terminated
```

- `idle → busy` 仅在池锁内通过；`draining` / `terminated` 实例**永远不可能**被 Acquire 命中。
- 忙碌中收到回收请求：实例保持 `busy` 执行完手头调用（期间拒绝新调用），
  Release 时转入 `draining` 并**立即终结**，绝不回到空闲链。
- `terminate()` 只允许在 `idle/draining` 下执行且**只成功一次**；重复回收返回 false，
  杜绝"同一实例被重复回收"。
- 超时路径走 `forceAbort()`：取消执行 ctx 并直接置 `terminated`，实例从 busy 表摘除、
  **不回池**。因此被超时/崩溃污染的实例不可能服务后续调用。

### 5.2 池与并发闸门

- 池按 `(function, version)` 维度维护 `idle` 列表与 `busy` 表；`inflight` 是该版本
  在执调用数。`Acquire` 在池锁内先判闸门再复用空闲实例；冷启动在锁外装载处理器，
  重新取锁后**二次校验闸门**，防止并发冷启动双超上限。
- 达到 `MaxConcurrency` 立即返回 `concurrency_limited`：不排队、不阻塞、不静默丢弃。
- 正常结束 `Release` 回 LIFO 空闲链（优先复用热实例）；超过 `MaxIdlePerVersion`
  的空闲实例当场收缩终结。
- 回收策略：后台 reaper 按 `ReapInterval` 扫描，空闲超过 `IdleTTL` 的实例被终结
  （`ReapIdle(now)` 也可手动触发，测试用它做确定性验证）；
  `DrainVersion` 在版本下线/回滚时可强制回收该版本全部实例，且对 busy 实例安全、幂等。
- 实例 ID 全局单调，便于在日志中区分"复用热实例"与"冷启动新实例"。

## 6. 失败与重试规则（`invoker.go`）

单次调用 `invokeOnce`：

1. 解析目标（路由 → 函数 → 版本，含可用性门禁）；
2. 计算有效超时（版本配置，v2 可选收紧）；
3. `Acquire` 实例（并发拒绝在此发生）；挂载超时取消句柄；
4. 独立 goroutine 执行处理器，平台侧用 `context.WithTimeout` 与 `time.Timer` 双保险；
5. 结论处理：
   - 定时器先到 → `Abort` 强杀实例 → `timeout`（等待被取消的处理器退出，无 goroutine 泄漏）；
   - 处理器 panic → recover → `platform_error`，实例 `Abort` 不回池；
   - 处理器返回 error 且执行 ctx 已到期 → 仍归 `timeout` 并强杀；
   - 处理器主动 error → `function_error`，实例正常归还；
   - 成功 → 响应携带 `version / instanceId / attempts`，实例归还复用。

`invokeWithRetry`：

- 仅 `timeout / concurrency_limited / platform_error` 进入重试；
  `function_error` 与所有确定性请求错误一次即返回。
- 退避使用 `sleepContext`，调用方 ctx 取消可中断退避。
- 重试在**全新尝试**上进行：超时实例已销毁，因此天然不会在被污染实例上重试。
- 仅当 `attempt > 1` 且仍失败时才包装 `retry_exhausted`，`Cause` 链保留末次根因，
  可用 `errors.As/Unwrap` 还原真实类别。

### 6.1 幂等与"无重复副作用"

- v2 请求可携带 `idempotencyKey`：平台对同键调用做**进行中合并 + 结论缓存**——
  首个调用执行，其余同键并发调用阻塞等待同一结果，响应逐字节一致。
- 处理器侧通过 `InvokeContext.IdempotencyKey` 对外部副作用去重
  （内置 `idempotent-sideeffect` 处理器演示），即使触发平台重试，同键副作用也只提交一次。
- 结论先回填、后 `close(done)`：channel 关闭构成 happens-before 屏障，等待者读到的
  结果必然可见（该语义有 `-race` 下的并发测试守护）。

## 7. 异步任务调用链路（`task.go` / `async.go` / `async_protocol.go`）

在同步主链路之上新增一条**提交即返回凭证、后台推进、可反复查询**的异步链路。
它复用既有的函数注册发布、版本路由、实例池与失败重试能力，但结论模型与副作用语义更强。

### 7.1 任务状态机（单向推进）

```
              调度器获得并发槽位与实例
   queued ───────────────────────────────▶ running
      │                                       │  ├────────────▶ succeeded（成功，终态）
      │                                       │  ├────────────▶ failed（函数失败/重试耗尽/启动失败，终态）
      │                                       │  ├────────────▶ timed_out（硬截止，终态）
      └──────────────▶ canceled（终态）        │  └────────────▶ canceled（调用方撤销/关闭，终态）
                   （排队中撤销，attempts=0）
```

- 合法迁移只有 `queued -> running | failed | canceled` 与
  `running -> succeeded | failed | timed_out | canceled`，由 `transitionTask` 集中校验。
- **四个终态彼此严格可区分**：成功 / 函数自身失败（`errorClass=function_error`）/
  硬截止超时（`timed_out`，`errorClass=timeout`）/ 被取消（`canceled`，
  `errorClass=timeout`，原因文本区分排队撤销、运行撤销、退避撤销、关闭撤销）。
  实际发生过重试才出现 `retry_exhausted`，挂在 `failed` 下且原因保留末次根因。
- 状态与结论在 `AsyncManager` 单把锁内**一次性写入**；`settle` 是终态唯一提交点，
  任务一旦终态，迟到的函数返回（哪怕是成功）一律丢弃，**不回退、不跳变、无半更新**。
- 同一凭证反复查询返回**同一份确定结论**（成功载荷、尝试次数、实例 ID 逐字节稳定）。
  未知凭证返回 `not_found`。

### 7.2 提交、去重与"最多一次副作用"

- 提交（`SubmitAsync` / 报文 `mode=async`）**立即**返回稳定任务凭证（`taskId`），
  不阻塞等待执行；随后 `op=query` 查询、`op=cancel` 撤销。
- **业务操作标识去重**（`idempotencyKey`，去重域 = 函数名 + 键）：
  - 同一业务操作标识无论是**并发提交、串行重复提交**，都合并为**同一个任务**，
    所有提交方拿到同一凭证、看到同一结论，处理器**只被实际执行一次**。
  - 同键复用到**不同已发布版本**返回 `conflict`，不允许跨版本混用去重凭证。
- **无去重手段不得重试**：若目标版本的重试策略允许一次提交内多次尝试
  （`Retry.MaxAttempts > 1`），而提交方**没有提供业务操作标识**，该提交被
  **明确拒绝（`invalid_request`）**，绝不进入可能重复产生副作用的执行。
- 一次提交内部的多次尝试共享同一个 `InvokeContext.IdempotencyKey`，
  处理器据此把外部副作用提交压到一次（平台保证"并发/重复提交只执行一次"，
  处理器与平台协作保证"一次提交内重试只提交一次副作用"，见
  `sideeffect-counter` 处理器与对应测试）。

### 7.3 截止时间、取消与实例回收的硬保证

- **硬截止**：每次尝试使用独立的 `context.WithTimeout(版本超时)`（v2 `timeoutMs`
  只能收紧不能放宽，与同步一致）。截止到达时平台**立即** `Abort` 强杀执行实例并推进终态，
  **不等待函数是否配合取消**；因此即便处理器刻意忽略 `ctx`，查询方也在
  接近 deadline 的有限时间内看到 `timed_out`/`canceled`。
- **主动撤销**：排队任务立即 `canceled`（`attempts=0`，从未产生执行）；
  运行中任务在取消临界区内即被置为终态并强杀实例，迟到的 goroutine 退出只做清理、
  不能再改结论；重试退避期取消会回收已冷启动的待重试实例。
- **实例不泄漏、不复用**：截止/取消/`panic` 路径的实例走与同步相同的
  `forceAbort + Abort`，从 busy 摘除、不回空闲链，后续任务只能拿到干净新实例；
  全部任务终态后池内 `busy` 恒为 0。
- **并发上限与排队**：后台任务与同步调用**共用同一实例池**，统一受版本
  `MaxConcurrency` 约束。拿不到槽位的任务进入 FIFO 队列；当"在执 + 排队"达到
  `MaxConcurrency + MaxQueuedPerVersion` 时，新提交立即得到
  **`concurrency_limited` 的可区分拒绝**（排队中与被拒绝两种结论可区分），
  **不静默丢弃、不静默降级为同步**。调度通过"池锁内回调 + 管理器锁"原子完成
  `queued -> running` 与实例占用，不存在"占了名额却未计入在执"的窗口。
- **关闭语义**：`Platform.Close` 停止调度，把所有在执/排队任务推进到 `canceled`
  并回收实例；终态结论在关闭后仍可查询。

> 关于"终态之后继续产生副作用"：Go 无法强杀一个不响应 `ctx` 的用户 goroutine。
> 平台保证的是——截止/取消时实例立即被判定终止并从池中摘除、**绝不复用**，
> 任务结论立即终态，且迟到结果被丢弃；被强杀实例的任何残留执行都处于隔离上下文，
> 其输出不会被采纳、其实例不会再服务任何任务。

### 7.4 协议与版本兼容范围

异步能力**只在 v2 协议上开放**，通过显式 `"mode":"async"` 选择，与同步报文互斥：

| 操作 | 报文 | 成功返回 |
|---|---|---|
| 提交 | `{"protocol":"2.0","mode":"async","route":"/p" | "function":"f", "version"?, "timeoutMs"?, "idempotencyKey"?, "options"?}` | `{ok:true, taskId, state:"queued"}` |
| 查询 | `{"protocol":"2.0","mode":"async","op":"query","taskId":"..."}` | `{ok, state, attempts, instanceId, payload?, errorClass?, errorReason?}` |
| 撤销 | `{"protocol":"2.0","mode":"async","op":"cancel","taskId":"..."}` | 同上（终态任务返回原终态） |

- **v1 客户端不支持异步**：`protocol:"1.0"` 的异步请求被**明确拒绝**，
  平台绝不静默降级为同步执行；旧版同步调用的行为与字段（v1 响应无 `attempts` 等）
  完全保持不变。
- v2 但缺少 `mode=async`（即同步报文形态）打到异步入口、未知顶层字段、未知 `options`、
  非法 `op`、缺失 `taskId`、`route/function` 互斥冲突等，一律 `invalid_request`，
  不静默忽略未知选项。
- 同步与异步是**两个独立入口**：`Call`/`Invoke` 仍是同步，
  `SubmitAsync*`/`QueryAsync*`/`CancelAsync*` 是异步，互不隐式转换。

## 8. 并发一致性

- 注册表（`sync.RWMutex`，指针切换在写锁内原子完成）、路由表（`sync.RWMutex`）、
  实例池（单把池互斥锁保护所有记账结构）、实例自身（独立互斥锁保护状态机）、
  幂等表（独立互斥锁 + channel 屏障）各自封界，不存在跨结构无锁读写。
- 异步管理器额外持有一把锁保护任务表 / 去重表 / 队列 / 在执计数，任务状态与结论
  在同一临界区一次性提交；跨池与管理器的调度回调固定锁序为
  `pool.mu -> AsyncManager.mu`，取消/关闭路径则先释管理器锁再动池，不存在锁环。
- 版本切换、实例回收与调用并发发生时：调用在解析瞬间拿到目标版本的**快照克隆**，
  实例的 busy/draining/terminated 转换与池记账在同一把池锁内完成，无半更新窗口。
  并发查询 / 取消 / 回收同时作用于同一任务时，先到的终态获胜、其余结论被丢弃，
  结果自洽（见 `TestAsync_ConcurrentQueryCancelAndReclaimStayConsistent`）。
- `go test ./... -race` 全部通过。

## 9. 本地持久化（`storage.go`）

- 元数据（函数、全部版本、历史、路由）以单个 JSON 快照保存在 `<dir>/faas-state.json`。
- 写入 = 写同目录临时文件 → `Sync` → `rename`，崩溃或并发下不会读到半更新文件；
  每次 publish / rollback / 状态变更 / 路由变更后立即落盘。
- 实例与幂等缓存属于易失运行态，不持久化（重启后冷启动重建）；异步任务记录、
  去重表与队列同样是进程内运行态，不进入快照，元数据重启后完整恢复：
  回滚目标、历史版本与 rollback 记录均保留，新调用仍命中重启前的活动版本
  （见 `TestPlatform_PersistAndRestoreAcrossRestart`）。
- `storeDir` 传空字符串即为纯内存模式。

## 10. 代码地图

| 文件 | 职责 |
|---|---|
| `errors.go` | 错误分类、`*Error`、原因/因果链工具 |
| `model.go` | 函数、版本、配置、重试策略、发布记录、路由等领域类型 |
| `registry.go` | 注册/发布/回滚/版本状态机/调用目标解析 |
| `router.go` | 路由绑定、冲突检测、未知路由拒绝 |
| `protocol.go` | v1/v2 报文解码归一化、严格选项校验、分版本响应编码 |
| `runtime.go` | 处理器接口与内置模拟处理器（echo/slow/fail/panic/flaky/幂等副作用/不配合取消/副作用计数） |
| `instance.go` | 实例状态机、panic 恢复、强杀与回收原语 |
| `pool.go` | 冷启动/复用、并发闸门、空闲收缩、TTL 回收、Drain、后台 reaper、锁内启动 `AcquireForRun` |
| `invoker.go` | 同步调用编排、超时控制、错误归类、退避重试、幂等合并 |
| `task.go` | 异步任务状态机、状态取值与单向迁移校验、任务快照 |
| `async.go` | 异步管理器：提交/业务去重/排队调度/硬截止/取消强终态/重试/实例回收/关闭 |
| `async_protocol.go` | 异步 v2 报文严格解码（提交/查询/撤销）与凭证、状态响应编码 |
| `storage.go` | 原子 JSON 快照持久化与恢复 |
| `platform.go` | 门面装配：接线组件、变更落盘、同步与异步协议入口 |
| `cmd/faas-demo` | 可本地运行的十条链路演示（含异步凭证/查询与截止强终态） |

## 11. 测试覆盖（边界与异常路径）

- 注册/发布：空名、重复注册、未知函数发布、超时/并发/处理器非法配置。
- 回滚：0/负版本号、不存在版本、回滚当前版本、disabled/retired 目标拒绝、
  回滚后命中验证、历史可追溯；活动版本下线后跟随调用 `unavailable`。
- 路由：空路径、冲突绑定（含 pin 变化）、幂等登记、未知路径、删除后解析。
- 协议：v1 默认值确定性、八类非法报文拒绝、未知选项拒绝、v1 响应不含新字段。
- 实例：重复终结、busy 回收不接新调用、TTL 回收、Drain 幂等、
  50 并发 × 上限 8 的扩缩自洽（无残留 busy、空闲受上限约束）。
- 调用：超时强杀不污染、业务错误不重试、panic 归类平台错误并重试、
  首次超时重试成功且落在新实例、同键 8 并发副作用仅一次且响应一致、
  回滚穿插在 32 个并发调用中指针原子切换。
- 异步任务：
  - 提交即得稳定凭证，反复查询结论逐字节稳定；未知凭证 `not_found`。
  - 成功 / 函数失败 / 硬截止 `timed_out` / `canceled` 四类终态可区分；终态后取消不回退。
  - **并发与重复提交只产生一次副作用**（首次 16 路并发合并、24 次串并发同凭证、
    invocations=1、committed 唯一）；允许重试却缺业务标识的提交被明确拒绝。
  - 一次提交内多次尝试共享幂等键：3 次尝试（2 次超时被杀）副作用仍只提交一次。
  - **函数完全不配合取消**：硬截止 ~80ms 内必得 `timed_out`，运行中撤销快速得
    `canceled`，重试退避期取消快速终态；被杀实例 busy 归零、不影响后续任务。
  - 排队：并发上限 1 + 排队 2 占满后第 4 个提交 `concurrency_limited`，
    槽位恢复后排队任务 FIFO 成功；排队中取消立即 `canceled` 且永不复活。
  - 并发查询 / 取消 / 回收同时发生：12 任务终态只可能是 timed_out/canceled、无跳变、busy=0。
  - 协议：v1 异步请求被拒（不降级同步）、缺 mode / 未知选项 / 未知字段 / 非法 op 拒绝、
    `timeoutMs` 只能收紧；同步 v1/v2 既有用例不回归。
  - 关闭：在执与排队任务全部 `canceled`、实例回收，终态仍可查。
- 持久化：原子文件无临时残留、重启恢复活动版本与回滚历史、恢复后非法回滚仍被拒。
