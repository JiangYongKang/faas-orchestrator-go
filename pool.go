package faas

import (
	"fmt"
	"sync"
	"time"
)

// PoolConfig 控制实例池的回收策略。
type PoolConfig struct {
	// IdleTTL 实例空闲超过该时长即被回收。
	IdleTTL time.Duration
	// MaxIdlePerVersion 每个版本最多保留的空闲实例数，超出的空闲实例立即回收。
	MaxIdlePerVersion int
	// ReapInterval 后台扫描间隔；<=0 表示不启动后台回收（测试可手动 ReapIdle）。
	ReapInterval time.Duration
}

// DefaultPoolConfig 返回默认回收策略。
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		IdleTTL:           30 * time.Second,
		MaxIdlePerVersion: 2,
		ReapInterval:      5 * time.Second,
	}
}

// versionKey 标识一个版本的实例子池。
type versionKey struct {
	function string
	version  int
}

// InstancePool 管理全部实例，保证并发扩缩状态自洽：
// 同一实例不会被重复回收、不会被两个调用同时获得、回收中的实例不接新调用。
type InstancePool struct {
	cfg     PoolConfig
	runtime *Runtime

	mu     sync.Mutex
	pools  map[versionKey]*versionPool
	all    map[string]*Instance
	idGen  uint64
	stopCh chan struct{}
	stop   bool
	wg     sync.WaitGroup
}

// versionPool 是单版本维度的池与并发闸门（所有字段仅在 InstancePool.mu 下访问）。
type versionPool struct {
	idle     []*Instance
	busy     map[string]*Instance
	inflight int
}

// NewInstancePool 创建实例池并按需启动后台回收协程。
func NewInstancePool(rt *Runtime, cfg PoolConfig) *InstancePool {
	if cfg.IdleTTL <= 0 {
		cfg.IdleTTL = DefaultPoolConfig().IdleTTL
	}
	if cfg.MaxIdlePerVersion < 0 {
		cfg.MaxIdlePerVersion = 0
	}
	p := &InstancePool{
		cfg:     cfg,
		runtime: rt,
		pools:   make(map[versionKey]*versionPool),
		all:     make(map[string]*Instance),
		stopCh:  make(chan struct{}),
	}
	if cfg.ReapInterval > 0 {
		p.wg.Add(1)
		go p.reaperLoop()
	}
	return p
}

func (p *InstancePool) vpLocked(k versionKey) *versionPool {
	vp, ok := p.pools[k]
	if !ok {
		vp = &versionPool{busy: make(map[string]*Instance)}
		p.pools[k] = vp
	}
	return vp
}

// Acquire 获取一个可执行调用的实例：优先复用空闲实例，否则冷启动。
// 若在执并发已达 MaxConcurrency，返回 ErrClassConcurrency（不排队、不静默丢弃）。
func (p *InstancePool) Acquire(v *FunctionVersion) (*Instance, error) {
	k := versionKey{v.FunctionName, v.Version}

	p.mu.Lock()
	if p.stop {
		p.mu.Unlock()
		return nil, newError(ErrClassPlatform, "instance pool is stopped")
	}
	vp := p.vpLocked(k)
	if vp.inflight >= v.Config.MaxConcurrency {
		p.mu.Unlock()
		return nil, newError(ErrClassConcurrency,
			fmt.Sprintf("version %s@v%d reached max concurrency %d",
				v.FunctionName, v.Version, v.Config.MaxConcurrency))
	}

	// 复用：从空闲链表尾部取最新实例。
	if n := len(vp.idle); n > 0 {
		in := vp.idle[n-1]
		vp.idle = vp.idle[:n-1]
		// 状态转换在池锁内完成；draining/terminated 实例绝不会出现在 idle 中。
		in.mu.Lock()
		if in.state != InstanceIdle {
			in.mu.Unlock()
			p.mu.Unlock()
			return nil, wrapError(ErrClassPlatform, "idle list held non-idle instance", nil)
		}
		in.state = InstanceBusy
		in.mu.Unlock()
		vp.busy[in.ID] = in
		vp.inflight++
		p.mu.Unlock()
		return in, nil
	}
	p.mu.Unlock()

	// 冷启动在锁外完成（模拟进程拉起；处理器查找失败要给出可解释错误）。
	handler, err := p.runtime.lookup(v.Config.HandlerName)
	if err != nil {
		return nil, err // not_found：发布产物缺失
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stop {
		return nil, newError(ErrClassPlatform, "instance pool is stopped")
	}
	// 拿到锁后重新校验并发闸门（防止并发冷启动双超）。
	vp = p.vpLocked(k)
	if vp.inflight >= v.Config.MaxConcurrency {
		return nil, newError(ErrClassConcurrency,
			fmt.Sprintf("version %s@v%d reached max concurrency %d",
				v.FunctionName, v.Version, v.Config.MaxConcurrency))
	}
	p.idGen++
	in := &Instance{
		ID:           fmt.Sprintf("inst-%d", p.idGen),
		FunctionName: v.FunctionName,
		Version:      v.Version,
		handler:      handler,
		state:        InstanceBusy, // 冷启动即服务本次调用
		createdAt:    time.Now(),
	}
	p.all[in.ID] = in
	vp.busy[in.ID] = in
	vp.inflight++
	return in, nil
}

// AcquireForRun 在一次池锁临界区内完成"判定闸门 -> 取空闲或冷启动 -> 标记忙碌"，
// 并在锁内调用 start，保证调用方的记账与实例状态迁移原子可见、不存在
// "已占并发名额但尚未计入在执"的窗口（异步调度用它与任务状态机联动）。
// 冷启动在锁外装载处理器后，会在锁内做二次闸门校验，防止并发冷启动双超上限。
func (p *InstancePool) AcquireForRun(v *FunctionVersion, start func(*Instance) bool) (*Instance, error) {
	k := versionKey{v.FunctionName, v.Version}

	p.mu.Lock()
	if p.stop {
		p.mu.Unlock()
		return nil, newError(ErrClassPlatform, "instance pool is stopped")
	}
	vp := p.vpLocked(k)
	if vp.inflight >= v.Config.MaxConcurrency {
		p.mu.Unlock()
		return nil, newError(ErrClassConcurrency,
			fmt.Sprintf("version %s@v%d reached max concurrency %d",
				v.FunctionName, v.Version, v.Config.MaxConcurrency))
	}
	if n := len(vp.idle); n > 0 {
		in := vp.idle[n-1]
		vp.idle = vp.idle[:n-1]
		in.mu.Lock()
		if in.state != InstanceIdle {
			in.mu.Unlock()
			p.mu.Unlock()
			return nil, wrapError(ErrClassPlatform, "idle list held non-idle instance", nil)
		}
		in.state = InstanceBusy
		in.mu.Unlock()
		vp.busy[in.ID] = in
		vp.inflight++
		keep := start(in)
		p.mu.Unlock()
		if !keep {
			p.Abort(in)
			return nil, newError(ErrClassPlatform, "start callback rejected the acquired instance")
		}
		return in, nil
	}
	p.mu.Unlock()

	// 冷启动装载处理器在锁外。
	handler, err := p.runtime.lookup(v.Config.HandlerName)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stop {
		return nil, newError(ErrClassPlatform, "instance pool is stopped")
	}
	vp = p.vpLocked(k)
	if vp.inflight >= v.Config.MaxConcurrency {
		return nil, newError(ErrClassConcurrency,
			fmt.Sprintf("version %s@v%d reached max concurrency %d",
				v.FunctionName, v.Version, v.Config.MaxConcurrency))
	}
	p.idGen++
	in := &Instance{
		ID:           fmt.Sprintf("inst-%d", p.idGen),
		FunctionName: v.FunctionName,
		Version:      v.Version,
		handler:      handler,
		state:        InstanceBusy,
		createdAt:    time.Now(),
	}
	keep := start(in)
	if !keep {
		return nil, newError(ErrClassPlatform, "start callback rejected the cold-start instance")
	}
	p.all[in.ID] = in
	vp.busy[in.ID] = in
	vp.inflight++
	return in, nil
}

// Release 正常归还实例：实例恢复 idle 并可被复用；
// 若 busy 期间已收到回收请求，则终结而不入空闲链表。
func (p *InstancePool) Release(in *Instance) {
	k := versionKey{in.FunctionName, in.Version}
	p.mu.Lock()
	defer p.mu.Unlock()
	vp := p.vpLocked(k)
	if _, ok := vp.busy[in.ID]; !ok {
		// 异常路径（如已被 Abort 摘除）：幂等忽略，绝不重复记账。
		return
	}
	delete(vp.busy, in.ID)
	vp.inflight--

	ok := in.finishCall()
	if !ok {
		// 状态机拒绝（理论上不应发生），保守终结并从全量表摘除。
		in.terminate()
		delete(p.all, in.ID)
		return
	}
	if in.State() == InstanceDraining {
		// 忙碌期间被标记回收：执行完毕立刻终结。
		in.terminate()
		delete(p.all, in.ID)
		return
	}
	vp.idle = append(vp.idle, in)
	p.evictExcessIdleLocked(vp)
}

// Abort 超时 / 崩溃路径：实例已被强杀，从 busy 摘除并计入终结，不回池。
func (p *InstancePool) Abort(in *Instance) {
	k := versionKey{in.FunctionName, in.Version}
	p.mu.Lock()
	defer p.mu.Unlock()
	vp := p.vpLocked(k)
	if _, ok := vp.busy[in.ID]; !ok {
		// 已被其他路径摘除：保证 forceAbort 幂等后直接返回。
		in.forceAbort()
		return
	}
	delete(vp.busy, in.ID)
	vp.inflight--
	in.forceAbort()
	delete(p.all, in.ID)
}

// evictExcessIdleLocked 终结超过 MaxIdlePerVersion 的空闲实例（在池锁内）。
func (p *InstancePool) evictExcessIdleLocked(vp *versionPool) {
	if p.cfg.MaxIdlePerVersion <= 0 {
		return
	}
	for len(vp.idle) > p.cfg.MaxIdlePerVersion {
		oldest := vp.idle[0]
		vp.idle = vp.idle[1:]
		if oldest.terminate() {
			delete(p.all, oldest.ID)
		}
	}
}

// DrainVersion 回收某版本的全部实例：空闲实例立即终结，忙碌实例标记回收中。
// 幂等：重复回收同一实例安全；回收中的实例不会再被 Acquire 命中。
func (p *InstancePool) DrainVersion(name string, version int) {
	k := versionKey{name, version}
	p.mu.Lock()
	defer p.mu.Unlock()
	vp, ok := p.pools[k]
	if !ok {
		return
	}
	remaining := vp.idle[:0]
	for _, in := range vp.idle {
		// 空闲实例：标记 draining 后立即终结。
		in.markDraining()
		if in.terminate() {
			delete(p.all, in.ID)
		}
	}
	vp.idle = remaining // 全部清空
	for _, in := range vp.busy {
		// 忙碌实例：登记回收，当前调用结束后由 Release 终结。
		in.markDraining()
	}
}

// ReapIdle 执行一轮空闲回收：终结空闲超过 IdleTTL 的实例，
// 并对超过保留上限的版本做收缩。返回被终结的实例数。
func (p *InstancePool) ReapIdle(now time.Time) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	reaped := 0
	for _, vp := range p.pools {
		kept := vp.idle[:0]
		for _, in := range vp.idle {
			if now.Sub(in.LastUsedAt()) > p.cfg.IdleTTL {
				in.markDraining()
				if in.terminate() {
					delete(p.all, in.ID)
					reaped++
				}
				continue
			}
			kept = append(kept, in)
		}
		vp.idle = kept
		p.evictExcessIdleLocked(vp)
	}
	return reaped
}

func (p *InstancePool) reaperLoop() {
	defer p.wg.Done()
	t := time.NewTicker(p.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case now := <-t.C:
			p.ReapIdle(now)
		}
	}
}

// Stop 停止后台回收并终结全部实例（忙碌实例强杀，空闲实例终结）。
func (p *InstancePool) Stop() {
	p.mu.Lock()
	if p.stop {
		p.mu.Unlock()
		p.wg.Wait()
		return
	}
	p.stop = true
	close(p.stopCh)
	for _, vp := range p.pools {
		for _, in := range vp.idle {
			in.terminate()
		}
		vp.idle = nil
		for _, in := range vp.busy {
			in.forceAbort()
		}
		vp.busy = map[string]*Instance{}
		vp.inflight = 0
	}
	p.all = map[string]*Instance{}
	p.mu.Unlock()
	p.wg.Wait()
}

// Stats 是实例池的诊断快照。
type PoolStats struct {
	Idle       int
	Busy       int
	Draining   int
	Terminated int
}

// Stats 返回当前池状态快照。
func (p *InstancePool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	var st PoolStats
	for _, in := range p.all {
		switch in.State() {
		case InstanceIdle:
			st.Idle++
		case InstanceBusy:
			st.Busy++
		case InstanceDraining:
			st.Draining++
		case InstanceTerminated:
			st.Terminated++
		}
	}
	return st
}
