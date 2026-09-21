package faas

import (
	"sync"
	"time"
)

// Registry 负责函数元数据、发布历史与活动版本指针的一致性管理。
// 所有方法均并发安全；版本配置一经发布不可变，仅活动指针与版本状态可变。
type Registry struct {
	mu        sync.RWMutex
	functions map[string]*Function
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{functions: make(map[string]*Function)}
}

// Register 注册一个尚未发布任何版本的函数；重复注册被拒绝。
func (r *Registry) Register(name string) error {
	if name == "" {
		return newError(ErrClassInvalid, "function name must not be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.functions[name]; ok {
		return newError(ErrClassConflict, "function already registered: "+name)
	}
	r.functions[name] = &Function{
		Name:          name,
		Versions:      make(map[int]*FunctionVersion),
		ActiveVersion: 0,
		nextVersion:   0,
	}
	return nil
}

// validateConfig 对发布配置做严格校验：非法值必须被拒绝，不接受静默默认。
func validateConfig(cfg VersionConfig) error {
	if cfg.HandlerName == "" {
		return newError(ErrClassInvalid, "handler_name must not be empty")
	}
	if cfg.Timeout <= 0 {
		return newError(ErrClassInvalid, "timeout must be positive")
	}
	if cfg.MaxConcurrency < 1 {
		return newError(ErrClassInvalid, "max_concurrency must be >= 1")
	}
	if cfg.Retry.MaxAttempts < 1 {
		return newError(ErrClassInvalid, "retry.max_attempts must be >= 1")
	}
	if cfg.Retry.Backoff < 0 {
		return newError(ErrClassInvalid, "retry.backoff must not be negative")
	}
	return nil
}

// Publish 为函数发布一个新版本，版本号单调递增；发布后自动成为活动版本。
func (r *Registry) Publish(name string, cfg VersionConfig) (*FunctionVersion, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	fn, ok := r.functions[name]
	if !ok {
		return nil, newError(ErrClassNotFound, "function not registered: "+name)
	}
	fn.nextVersion++
	v := &FunctionVersion{
		FunctionName: name,
		Version:      fn.nextVersion,
		Config:       cfg,
		State:        VersionActive,
		PublishedAt:  time.Now(),
	}
	fn.Versions[v.Version] = v
	prev := fn.ActiveVersion
	fn.ActiveVersion = v.Version
	fn.History = append(fn.History, PublishRecord{
		Kind: "publish", Version: v.Version, FromVersion: prev,
		OccurredAt: v.PublishedAt, Note: "published handler " + cfg.HandlerName,
	})
	return v.Clone(), nil
}

// Rollback 把活动指针切换到历史上已发布且当前可用的目标版本，
// 并在历史中追加 rollback 记录。目标版本非法 / 不可用将被拒绝。
func (r *Registry) Rollback(name string, version int) (*FunctionVersion, error) {
	if version < 1 {
		return nil, newError(ErrClassInvalid, "version number must be >= 1")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	fn, ok := r.functions[name]
	if !ok {
		return nil, newError(ErrClassNotFound, "function not registered: "+name)
	}
	v, ok := fn.Versions[version]
	if !ok {
		return nil, newError(ErrClassNotFound, "version does not exist")
	}
	if v.State == VersionRetired {
		return nil, newError(ErrClassUnavailable, "target version is retired and can never be reactivated")
	}
	if v.State == VersionDisabled {
		return nil, newError(ErrClassUnavailable, "target version is disabled; enable it before rollback")
	}
	if fn.ActiveVersion == version {
		return nil, newError(ErrClassInvalid, "target version is already the active version")
	}
	prev := fn.ActiveVersion
	fn.ActiveVersion = version
	fn.History = append(fn.History, PublishRecord{
		Kind: "rollback", Version: version, FromVersion: prev,
		OccurredAt: time.Now(), Note: "rollback active pointer",
	})
	return v.Clone(), nil
}

// SetVersionState 修改版本可用性状态，非法迁移被拒绝。
func (r *Registry) SetVersionState(name string, version int, state VersionState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	fn, ok := r.functions[name]
	if !ok {
		return newError(ErrClassNotFound, "function not registered: "+name)
	}
	v, ok := fn.Versions[version]
	if !ok {
		return newError(ErrClassNotFound, "version does not exist")
	}
	switch state {
	case VersionActive, VersionDisabled, VersionRetired:
	default:
		return newError(ErrClassInvalid, "unsupported version state: "+string(state))
	}
	if v.State == state {
		return newError(ErrClassConflict, "version already in state: "+string(state))
	}
	if v.State == VersionRetired {
		return newError(ErrClassConflict, "retired version can not change state")
	}
	if state == VersionActive && v.State != VersionDisabled {
		return newError(ErrClassConflict, "only disabled version can be enabled")
	}
	prev := v.State
	v.State = state
	fn.History = append(fn.History, PublishRecord{
		Kind: string(state), Version: version, FromVersion: fn.ActiveVersion,
		OccurredAt: time.Now(), Note: "state " + string(prev) + " -> " + string(state),
	})
	return nil
}

// ResolveVersion 返回某函数当前活动版本指针指向的版本记录（不校验可调用性）。
func (r *Registry) ResolveVersion(name string) (*FunctionVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.functions[name]
	if !ok {
		return nil, newError(ErrClassNotFound, "function not registered: "+name)
	}
	if fn.ActiveVersion == 0 {
		return nil, newError(ErrClassUnavailable, "function has no published version")
	}
	return fn.Versions[fn.ActiveVersion].Clone(), nil
}

// GetVersion 返回指定版本记录；不存在返回 not_found。
func (r *Registry) GetVersion(name string, version int) (*FunctionVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.functions[name]
	if !ok {
		return nil, newError(ErrClassNotFound, "function not registered: "+name)
	}
	v, ok := fn.Versions[version]
	if !ok {
		return nil, newError(ErrClassNotFound, "version does not exist")
	}
	return v.Clone(), nil
}

// ResolveForCall 解析一次调用实际命中的版本：
// pin>0 时校验固定版本且要求其可用；pin==0 时取活动版本并要求其可用。
func (r *Registry) ResolveForCall(name string, pin int) (*FunctionVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.functions[name]
	if !ok {
		return nil, newError(ErrClassNotFound, "function not registered: "+name)
	}
	var v *FunctionVersion
	if pin > 0 {
		var ok bool
		v, ok = fn.Versions[pin]
		if !ok {
			return nil, newError(ErrClassNotFound, "pinned version does not exist")
		}
	} else {
		if fn.ActiveVersion == 0 {
			return nil, newError(ErrClassUnavailable, "function has no published version")
		}
		v = fn.Versions[fn.ActiveVersion]
	}
	switch v.State {
	case VersionActive:
		return v.Clone(), nil
	case VersionDisabled:
		return nil, newError(ErrClassUnavailable, "target version is disabled")
	case VersionRetired:
		return nil, newError(ErrClassUnavailable, "target version is retired")
	default:
		return nil, newError(ErrClassPlatform, "unknown version state")
	}
}

// Snapshot 返回某函数的深拷贝视图（含历史），供存储持久化与诊断使用。
func (r *Registry) Snapshot(name string) (*Function, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.functions[name]
	if !ok {
		return nil, newError(ErrClassNotFound, "function not registered: "+name)
	}
	return cloneFunction(fn), nil
}

// FunctionNames 返回全部已注册函数名。
func (r *Registry) FunctionNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.functions))
	for name := range r.functions {
		names = append(names, name)
	}
	return names
}

// cloneFunction 在调用方已持锁的前提下深拷贝函数聚合。
func cloneFunction(fn *Function) *Function {
	cp := &Function{
		Name:          fn.Name,
		Versions:      make(map[int]*FunctionVersion, len(fn.Versions)),
		History:       append([]PublishRecord(nil), fn.History...),
		ActiveVersion: fn.ActiveVersion,
		nextVersion:   fn.nextVersion,
	}
	for k, v := range fn.Versions {
		cp.Versions[k] = v.Clone()
	}
	return cp
}

// loadAll 供存储层在启动时恢复状态（加写锁整体载入）。
func (r *Registry) loadAll(funcs []*Function) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.functions = make(map[string]*Function, len(funcs))
	for _, fn := range funcs {
		r.functions[fn.Name] = cloneFunction(fn)
	}
}
