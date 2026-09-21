package faas

import (
	"sort"
	"sync"
)

// Router 维护路由路径到 (函数, 版本钉选) 的绑定。
// 不做任何默认兜底：未知路径、冲突绑定一律显式报错。
type Router struct {
	mu     sync.RWMutex
	routes map[string]Route
}

// NewRouter 创建空路由器。
func NewRouter() *Router {
	return &Router{routes: make(map[string]Route)}
}

// AddRoute 新增或幂等更新一条路由；
// 若路径已绑定到不同目标则返回 conflict，不静默覆盖。
// 相同目标的重复登记视为幂等成功。
func (rt *Router) AddRoute(r Route) error {
	if r.Path == "" {
		return newError(ErrClassInvalid, "route path must not be empty")
	}
	if r.FunctionName == "" {
		return newError(ErrClassInvalid, "route target function must not be empty")
	}
	if r.VersionPin < 0 {
		return newError(ErrClassInvalid, "version pin must be >= 0")
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if exist, ok := rt.routes[r.Path]; ok {
		if exist.FunctionName != r.FunctionName || exist.VersionPin != r.VersionPin {
			return newError(ErrClassConflict,
				"route conflict: path already bound to a different target")
		}
		return nil // 幂等登记
	}
	rt.routes[r.Path] = r
	return nil
}

// Resolve 按路径解析路由；未知路径返回 not_found，绝不落到默认实现。
func (rt *Router) Resolve(path string) (Route, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	r, ok := rt.routes[path]
	if !ok {
		return Route{}, newError(ErrClassNotFound, "no route for path: "+path)
	}
	return r, nil
}

// RemoveRoute 删除一条路由；不存在返回 not_found。
func (rt *Router) RemoveRoute(path string) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if _, ok := rt.routes[path]; !ok {
		return newError(ErrClassNotFound, "no route for path: "+path)
	}
	delete(rt.routes, path)
	return nil
}

// List 返回全部路由的副本（按路径排序，保证输出稳定）。
func (rt *Router) List() []Route {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	out := make([]Route, 0, len(rt.routes))
	for _, r := range rt.routes {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// loadAll 供存储层启动恢复。
func (rt *Router) loadAll(routes []Route) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.routes = make(map[string]Route, len(routes))
	for _, r := range routes {
		rt.routes[r.Path] = r
	}
}
