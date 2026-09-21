package faas

import "sync"

type RouteTarget struct {
	Function string
	Version  VersionNumber
}

// Router stores explicit, version-pinned routes. There is deliberately no
// default route or wildcard, and overwriting a different target is rejected.
type Router struct {
	mu     sync.RWMutex
	routes map[string]RouteTarget
}

func NewRouter() *Router {
	return &Router{routes: make(map[string]RouteTarget)}
}

func validateTarget(route string, target RouteTarget) error {
	if route == "" {
		return invalidArgument("route is empty")
	}
	if target.Function == "" {
		return invalidArgument("route %q target function is empty", route)
	}
	if target.Version == NoVersion {
		return invalidArgument("route %q target version is zero; pin a published version", route)
	}
	return nil
}

// Set binds an unused route. Binding the same route to an identical target is
// idempotent; binding it to another target returns KindConflict.
func (r *Router) Set(route string, target RouteTarget) error {
	if err := validateTarget(route, target); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.routes[route]; ok && existing != target {
		return conflict("route %q already targets %s@%d", route, existing.Function, existing.Version)
	}
	r.routes[route] = target
	return nil
}

// Replace changes an explicit route. It still refuses empty targets, ensuring
// callers cannot accidentally remove routing by supplying a zero value.
func (r *Router) Replace(route string, target RouteTarget) error {
	if err := validateTarget(route, target); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.routes[route]; !ok {
		return notFound("route %q does not exist", route)
	}
	r.routes[route] = target
	return nil
}

func (r *Router) Resolve(route string) (RouteTarget, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	target, ok := r.routes[route]
	if !ok {
		return RouteTarget{}, notFound("route %q is not registered", route)
	}
	return target, nil
}

func (r *Router) Remove(route string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.routes[route]; !ok {
		return notFound("route %q does not exist", route)
	}
	delete(r.routes, route)
	return nil
}
