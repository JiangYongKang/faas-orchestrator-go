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
	tasks    *TaskManager
	store    *Store // 可为 nil（纯内存模式）
}

// NewPlatform 装配一个平台实例；storeDir 为空表示纯内存运行，
// 非空则启动时载入快照、每次元数据变更后原子落盘。
func NewPlatform(storeDir string, poolCfg PoolConfig) (*Platform, error) {
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
	return &Platform{
		registry: reg,
		router:   router,
		runtime:  runtime,
		pool:     pool,
		invoker:  inv,
		tasks:    NewTaskManager(inv, DefaultTaskManagerConfig()),
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
// v2 报文可通过 op 字段选择异步任务操作（task.submit / task.query /
// task.cancel）；缺省 op=invoke 时行为与旧版完全一致。
func (p *Platform) Call(ctx context.Context, raw []byte) ([]byte, error) {
	version, req, err := DecodeRequest(raw)
	if err != nil {
		// 解码失败时调用方协议版本未知，统一用 v2 错误报文返回可区分原因。
		encoded, _ := EncodeResponse(ProtoV2, nil, err)
		return encoded, err
	}
	switch req.Op {
	case "", OpInvoke:
		resp, callErr := p.invoker.Invoke(ctx, req)
		encoded, encErr := EncodeResponse(version, resp, callErr)
		if encErr != nil {
			return nil, encErr
		}
		return encoded, callErr
	case OpTaskSubmit:
		snap, callErr := p.tasks.Submit(req)
		return encodeTaskCall(version, snap, callErr)
	case OpTaskQuery:
		snap, callErr := p.tasks.Query(req.TaskID)
		return encodeTaskCall(version, snap, callErr)
	case OpTaskCancel:
		snap, callErr := p.tasks.Cancel(req.TaskID)
		return encodeTaskCall(version, snap, callErr)
	default:
		// DecodeRequest 已拒绝未知 op，此处防御兜底。
		err := newError(ErrClassInvalid, "unsupported op: "+string(req.Op))
		encoded, _ := EncodeResponse(ProtoV2, nil, err)
		return encoded, err
	}
}

// encodeTaskCall 统一编码任务操作响应。
func encodeTaskCall(version ProtocolVersion, snap *TaskSnapshot, callErr error) ([]byte, error) {
	encoded, encErr := EncodeTaskResponse(version, snap, callErr)
	if encErr != nil {
		return nil, encErr
	}
	return encoded, callErr
}

// Invoke 以规范化请求发起同步调用。
// 携带任务操作（task.*）的请求在此被明确拒绝，绝不静默降级为同步执行；
// 异步语义请使用 SubmitTask / QueryTask / CancelTask。
func (p *Platform) Invoke(ctx context.Context, req *InvokeRequest) (*InvokeResponse, error) {
	if req.Op != "" && req.Op != OpInvoke {
		return nil, newError(ErrClassInvalid,
			"Invoke only accepts op=invoke; use SubmitTask/QueryTask/CancelTask for "+string(req.Op))
	}
	return p.invoker.Invoke(ctx, req)
}

// SubmitTask 以规范化请求提交异步任务，立即返回任务凭证。
func (p *Platform) SubmitTask(req *InvokeRequest) (*TaskSnapshot, error) {
	return p.tasks.Submit(req)
}

// QueryTask 按任务凭证查询确定结论。
func (p *Platform) QueryTask(taskID string) (*TaskSnapshot, error) {
	return p.tasks.Query(taskID)
}

// CancelTask 撤销一次异步任务提交。
func (p *Platform) CancelTask(taskID string) (*TaskSnapshot, error) {
	return p.tasks.Cancel(taskID)
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

// Close 停止后台回收并释放实例；在途异步任务先被取消并落定终态。
func (p *Platform) Close() {
	p.tasks.Stop()
	p.pool.Stop()
}
