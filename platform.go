package faas

import (
	"context"
	"time"
)

// Platform 是内核对外门面：把注册、发布、回滚、路由、调用、持久化、
// 实例回收聚合为一致的入口，并保证元数据变更后快照同步。
type Platform struct {
	registry *Registry
	router   *Router
	runtime  *Runtime
	pool     *InstancePool
	invoker  *Invoker
	async    *AsyncManager
	store    *Store // 可为 nil（纯内存模式）
}

// NewPlatform 装配一个平台实例；storeDir 为空表示纯内存运行，
// 非空则启动时载入快照、每次元数据变更后原子落盘。
// 异步任务链路使用 DefaultAsyncConfig；需要自定义排队容量/调度周期时
// 使用 NewPlatformWithAsyncConfig。
func NewPlatform(storeDir string, poolCfg PoolConfig) (*Platform, error) {
	return NewPlatformWithAsyncConfig(storeDir, poolCfg, DefaultAsyncConfig())
}

// NewPlatformWithAsyncConfig 与 NewPlatform 相同，但允许指定异步链路配置。
func NewPlatformWithAsyncConfig(storeDir string, poolCfg PoolConfig, asyncCfg AsyncConfig) (*Platform, error) {
	reg := NewRegistry()
	router := NewRouter()
	runtime := NewRuntime()

	var store *Store
	if storeDir != "" {
		s, err := NewStore(storeDir)
		if err != nil {
			return nil, err
		}
		state, exists, err := s.Load()
		if err != nil {
			return nil, err
		}
		if exists {
			reg.loadAll(state.Functions)
			router.loadAll(state.Routes)
		}
		store = s
	}

	pool := NewInstancePool(runtime, poolCfg)
	inv := NewInvoker(reg, router, pool)
	asyncMgr := NewAsyncManager(reg, router, pool, asyncCfg)
	return &Platform{
		registry: reg,
		router:   router,
		runtime:  runtime,
		pool:     pool,
		invoker:  inv,
		async:    asyncMgr,
		store:    store,
	}, nil
}

// RegisterHandler 在运行时中装载自定义处理器（模拟上传发布产物）。
func (p *Platform) RegisterHandler(name string, h Handler) error {
	return p.runtime.Register(name, h)
}

// persist 把当前元数据快照原子落盘；纯内存模式为空操作。
func (p *Platform) persist() error {
	if p.store == nil {
		return nil
	}
	names := p.registry.FunctionNames()
	funcs := make([]*Function, 0, len(names))
	for _, n := range names {
		fn, err := p.registry.Snapshot(n)
		if err != nil {
			return err
		}
		funcs = append(funcs, fn)
	}
	return p.store.Save(funcs, p.router.List())
}

// RegisterFunction 注册函数。
func (p *Platform) RegisterFunction(name string) error {
	if err := p.registry.Register(name); err != nil {
		return err
	}
	return p.persist()
}

// Publish 发布新版本并落盘。
func (p *Platform) Publish(name string, cfg VersionConfig) (*FunctionVersion, error) {
	v, err := p.registry.Publish(name, cfg)
	if err != nil {
		return nil, err
	}
	if err := p.persist(); err != nil {
		return nil, err
	}
	return v, nil
}

// Rollback 回滚到指定版本并落盘；回滚后新调用必须命中目标版本。
func (p *Platform) Rollback(name string, version int) (*FunctionVersion, error) {
	v, err := p.registry.Rollback(name, version)
	if err != nil {
		return nil, err
	}
	if err := p.persist(); err != nil {
		return nil, err
	}
	return v, nil
}

// SetVersionState 调整版本可用性并落盘。
func (p *Platform) SetVersionState(name string, version int, state VersionState) error {
	if err := p.registry.SetVersionState(name, version, state); err != nil {
		return err
	}
	return p.persist()
}

// AddRoute 绑定路由并落盘；冲突显式拒绝。
func (p *Platform) AddRoute(path, function string, versionPin int) error {
	if err := p.router.AddRoute(Route{Path: path, FunctionName: function, VersionPin: versionPin}); err != nil {
		return err
	}
	return p.persist()
}

// Call 以原始协议报文发起调用（内部完成版本兼容解码），
// 返回按请求协议版本编码的响应报文；平台错误同样被编码进报文，
// 但为了便于程序化判定，非成功时同时返回 error。
func (p *Platform) Call(ctx context.Context, raw []byte) ([]byte, error) {
	version, req, err := DecodeRequest(raw)
	if err != nil {
		// 解码失败时调用方协议版本未知，统一用 v2 错误报文返回可区分原因。
		encoded, _ := EncodeResponse(ProtoV2, nil, err)
		return encoded, err
	}
	resp, callErr := p.invoker.Invoke(ctx, req)
	encoded, encErr := EncodeResponse(version, resp, callErr)
	if encErr != nil {
		return nil, encErr
	}
	return encoded, callErr
}

// Invoke 以规范化请求发起调用。
func (p *Platform) Invoke(ctx context.Context, req *InvokeRequest) (*InvokeResponse, error) {
	return p.invoker.Invoke(ctx, req)
}

// ReapIdle 手动触发一轮空闲实例回收。
func (p *Platform) ReapIdle() int {
	return p.pool.ReapIdle(time.Now())
}

// DrainVersion 触发某版本实例回收（回滚/下线后调用可强制清池）。
func (p *Platform) DrainVersion(name string, version int) {
	p.pool.DrainVersion(name, version)
}

// PoolStats 返回实例池诊断信息。
func (p *Platform) PoolStats() PoolStats {
	return p.pool.Stats()
}

// Close 停止后台回收并释放实例。
func (p *Platform) Close() {
	p.async.Stop()
	p.pool.Stop()
}

// ---- 异步任务链路 ----

// SubmitAsync 以规范化请求提交异步任务，立即返回稳定的任务凭证。
func (p *Platform) SubmitAsync(req *InvokeRequest) (string, error) {
	return p.async.Submit(req)
}

// QueryAsync 凭凭证查询任务结论；反复查询始终得到同一份确定结论。
func (p *Platform) QueryAsync(taskID string) (*TaskSnapshot, error) {
	return p.async.Query(taskID)
}

// CancelAsync 主动撤销任务；终态任务保持原终态并返回该终态快照。
func (p *Platform) CancelAsync(taskID string) (*TaskSnapshot, error) {
	return p.async.Cancel(taskID)
}

// SubmitAsyncRaw 以原始协议报文提交异步任务，返回按协议编码的凭证报文。
// 不支持的协议 / 非法字段组合会被明确拒绝，绝不静默降级为同步执行。
func (p *Platform) SubmitAsyncRaw(raw []byte) ([]byte, error) {
	req, err := DecodeAsyncSubmit(raw)
	if err != nil {
		encoded, _ := EncodeAsyncAck("", "", err)
		return encoded, err
	}
	taskID, subErr := p.async.Submit(req)
	snap, _ := p.async.Query(taskID)
	var state TaskState
	if snap != nil {
		state = snap.State
	}
	encoded, encErr := EncodeAsyncAck(taskID, state, subErr)
	if encErr != nil {
		return nil, encErr
	}
	return encoded, subErr
}

// QueryAsyncRaw 以原始协议报文查询任务结论。
func (p *Platform) QueryAsyncRaw(raw []byte) ([]byte, error) {
	op, taskID, err := DecodeAsyncOp(raw)
	if err != nil {
		encoded, _ := EncodeAsyncStatus(op, nil, err)
		return encoded, err
	}
	snap, qErr := p.async.Query(taskID)
	return EncodeAsyncStatus(op, snap, qErr)
}

// CancelAsyncRaw 以原始协议报文撤销任务并返回当前确定结论。
func (p *Platform) CancelAsyncRaw(raw []byte) ([]byte, error) {
	op, taskID, err := DecodeAsyncOp(raw)
	if err != nil {
		encoded, _ := EncodeAsyncStatus(op, nil, err)
		return encoded, err
	}
	snap, cErr := p.async.Cancel(taskID)
	return EncodeAsyncStatus(op, snap, cErr)
}
