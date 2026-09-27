# faas-orchestrator-go：本地函数计算平台内核

用**本地进程（实例状态机 + goroutine）与本地存储（原子 JSON 快照）**模拟函数计算平台内核，
聚焦在异常链路下结论的**一致性与可解释性**：并发调用、超时终止、实例回收、版本回滚、
失败重试、协议版本演进、异步任务（提交凭证 / 后台执行 / 结果查询 / 撤销）。

## 1. 快速开始

```bash
# 端到端演示（发布/回滚/超时/并发/回收/重试/幂等/协议兼容/异步任务）
go run ./cmd/faas-demo

# 全量单元测试（以边界与异常路径为主）
go test ./... -count=1 -v

# 并发正确性（数据竞争检测）
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
| `canceled` | 异步任务被调用方撤销或平台关停（仅出现在任务终态结论中） | 终态 |

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

## 7. 异步任务链路（`task.go` / `taskmanager.go`）

耗时较长、会产生外部业务动作的调用不应阻塞调用方。异步链路在同步调用能力之上
提供：**提交即得稳定任务凭证 → 后台推进执行 → 凭凭证反复查询同一确定结论**。

### 7.1 状态机（单向推进，终态不可逆）

```
                 撤销/平台关停
                ┌─────────────┐
                ▼             │
queued ──获取到实例──▶ running ──┬─ 成功 ───────────────▶ succeeded
  │                            ├─ 函数业务失败/平台错误耗尽 ─▶ failed
  │                            └─ 超时且重试耗尽 ────────▶ timed_out
  └── 撤销/关停 ──▶ canceled ◀── 撤销（运行中）
```

- 合法迁移只有 `queued → running → {succeeded|failed|timed_out}` 与
  `{queued|running} → canceled`；**不允许回退、跳变或半更新**。
  所有迁移都在 `TaskManager.mu` 内完成，终态由 `finishLocked` 单次落定，
  重复落定被忽略——因此并发查询、取消与回收同时发生时结论自洽。
- `queued` 是**可观测的排队结论**：任务已受理但尚未拿到版本并发槽位
  （首次成功获取实例时才推进到 `running`）。
- 同一凭证在终态后的任意次查询返回**逐字段一致**的快照
  （`TaskSnapshot` 是不可变视图，查询返回拷贝）。

### 7.2 提交校验与"不静默降级"

- 异步提交复用同步链路的目标解析与超时校验：未知函数/版本/路由、
  `timeoutMs` 超出版本配置、未知 `options` 一律按既有分类拒绝。
- 线上协议（v2）通过 `op` 字段选择操作：`task.submit` / `task.query` /
  `task.cancel`，缺省 `op=invoke` 时行为与旧版完全一致。
  - `task.query` / `task.cancel` 只允许携带 `taskId`，混入任何调用字段
    （route/function/payload/timeoutMs/options…）都是 `invalid_request`；
  - 未知 `op` 显式拒绝，**不会**被静默当作同步调用执行；
  - 规范化入口 `Platform.Invoke` 收到任务操作时同样显式拒绝，
    异步语义只能走 `SubmitTask` / `QueryTask` / `CancelTask`。
- 在途任务数（queued+running）达到 `TaskManagerConfig.MaxQueued` 时，
  提交返回 `concurrency_limited`——**显式拒绝而非静默丢弃**。

### 7.3 去重规则（副作用最多一次）

- `idempotencyKey` 即业务操作标识。同一标识的**并发提交与重复提交**
  在管理器锁内合并：只有首个提交创建任务，其余提交方拿到**同一个任务凭证**，
  因此所有提交方看到同一个确定结论，业务副作用最多发生一次。
- 一次提交内部因可重试失败（timeout / concurrency / platform）发生多次尝试时，
  平台把同一 `idempotencyKey` 透传给每次尝试（`InvokeContext.IdempotencyKey`），
  由函数侧对副作用去重（内置 `idempotent-sideeffect` 处理器演示）。
- 因此：**版本重试策略允许多次尝试（`MaxAttempts>1`）而提交方未提供
  `idempotencyKey` 时，提交被 `invalid_request` 明确拒绝**——
  不允许进入可能重复产生副作用的执行。`MaxAttempts<=1` 时键可选。

### 7.4 截止时间与回收的硬保证

- 每次尝试仍受版本单次执行超时约束（v2 可收紧）。到达截止时间：
  实例被 `Abort` 强杀（**不回池、不再服务后续任务**），任务按 timeout 归类；
  重试耗尽后落定 `timed_out` 终态。
- 与同步路径的关键差异：异步路径**不等待未配合取消的处理器退出**
  （`invokeOnceDetached`）。函数无视 `ctx` 取消时，任务依然在有限时间内
  到达 `timed_out` / `canceled` 终态；处理器迟到的返回被直接丢弃，
  不会改写终态（终态之后平台侧不再产生任何结论变更）。
- 调用方 `CancelTask`：queued 任务立即落定 `canceled`；running 任务取消
  执行上下文并强杀实例，执行协程在有限时间内落定 `canceled`；
  已终态任务幂等返回当前结论。终态后实例已被回收，后续任务在新实例上执行。
- 后台任务仍受每个版本的 `MaxConcurrency` 约束：槽位不足时任务停留在
  可观测的 `queued` 状态轮询等待（不消耗重试次数、不静默丢弃），
  被撤销时等待立即中断。

### 7.5 协议与版本兼容范围

- 异步任务是 **v2 协议能力**；v1 报文没有 `op` 字段，永远是同步调用，
  行为与结论和此前版本完全一致（既有用例守护）。
- v2 同步调用（缺省 `op`）的字段校验、响应编码与此前版本一致；
  新增字段仅 `op` / `taskId`，且只在对应操作下合法。
- 任务响应（`taskId` / `taskStatus` / `attempts` / `payload` / `errorClass`）
  只对 v2 编码；`ok` 表示本次操作是否成功，任务自身结论由
  `taskStatus` + `errorClass` 透出（succeeded/failed/timed_out/canceled 可区分）。
- 任务记录是易失运行态，与实例、幂等缓存一样**不持久化**；
  重启后在途任务不恢复（元数据不受影响）。

## 8. 并发一致性

- 注册表（`sync.RWMutex`，指针切换在写锁内原子完成）、路由表（`sync.RWMutex`）、
  实例池（单把池互斥锁保护所有记账结构）、实例自身（独立互斥锁保护状态机）、
  幂等表（独立互斥锁 + channel 屏障）、任务管理器（单把互斥锁保护任务状态机、
  去重表与在途计数）各自封界，不存在跨结构无锁读写。
- 版本切换、实例回收与调用并发发生时：调用在解析瞬间拿到目标版本的**快照克隆**，
  实例的 busy/draining/terminated 转换与池记账在同一把池锁内完成，无半更新窗口。
- `go test ./... -race` 全部通过。

## 9. 本地持久化（`storage.go`）

- 元数据（函数、全部版本、历史、路由）以单个 JSON 快照保存在 `<dir>/faas-state.json`。
- 写入 = 写同目录临时文件 → `Sync` → `rename`，崩溃或并发下不会读到半更新文件；
  每次 publish / rollback / 状态变更 / 路由变更后立即落盘。
- 实例与幂等缓存属于易失运行态，不持久化（重启后冷启动重建），元数据重启后完整恢复：
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
| `runtime.go` | 处理器接口与内置模拟处理器（echo/slow/fail/panic/flaky/幂等副作用） |
| `instance.go` | 实例状态机、panic 恢复、强杀与回收原语 |
| `pool.go` | 冷启动/复用、并发闸门、空闲收缩、TTL 回收、Drain、后台 reaper |
| `invoker.go` | 调用编排、超时控制、错误归类、退避重试、幂等合并、异步分离式尝试 |
| `task.go` | 异步任务状态机（queued/running/四种终态）与不可变快照 |
| `taskmanager.go` | 任务受理与校验、幂等合并、后台执行、排队/拒绝、撤销与关停 |
| `storage.go` | 原子 JSON 快照持久化与恢复 |
| `platform.go` | 门面装配：接线组件、变更落盘、协议入口、任务门面方法 |
| `cmd/faas-demo` | 可本地运行的九条异常链路演示（含异步任务全链路） |

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
- 持久化：原子文件无临时残留、重启恢复活动版本与回滚历史、恢复后非法回滚仍被拒。
- 异步任务：提交即得凭证且终态后重复查询逐字段一致、函数业务失败归类 failed、
  函数不配合取消时超时/撤销仍在有限时间内落定终态且实例被回收（后续任务不受影响）、
  排队任务可观测可撤销、16 路并发重复提交同一业务标识副作用恰好一次且凭证一致、
  可重试策略缺幂等键显式拒绝、首次超时重试在新实例上成功（attempts=2）、
  超时重试耗尽落定 timed_out、在途超上限显式拒绝 concurrency_limited、
  线上协议 submit/query/cancel 闭环与五类非法组合拒绝、v1 同步行为兼容、
  8 路并发查询与并发撤销下终态结论自洽（-race 守护）。
