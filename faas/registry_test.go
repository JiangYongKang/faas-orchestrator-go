package faas

import (
	"context"
	"testing"
)

func handlerReturning(value string) HandlerFunc {
	return func(_ context.Context, _ *InvocationContext) ([]byte, error) {
		return []byte(value), nil
	}
}

func TestPublishKeepsHistoryAndRollbackSelectsTarget(t *testing.T) {
	k := testKernel(t, Config{})
	v1 := mustPublish(t, k, specReturning("orders", "v1", handlerReturning("old")))
	v2 := mustPublish(t, k, specReturning("orders", "v2", handlerReturning("new")))
	if v1.Number != 1 || v2.Number != 2 {
		t.Fatalf("versions = %d,%d, want 1,2", v1.Number, v2.Number)
	}

	resp, err := k.Invoke(InvokeRequest{Function: "orders", Payload: []byte("before-rollback")})
	if err != nil {
		t.Fatalf("invoke current: %v", err)
	}
	if string(resp.Payload) != "new" || resp.Version != 2 {
		t.Fatalf("current result = %q@v%d, want new@v2", resp.Payload, resp.Version)
	}

	rolled, err := k.Rollback("orders", 1, "")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if rolled.Number != 1 {
		t.Fatalf("rolled version = %d, want 1", rolled.Number)
	}
	resp, err = k.Invoke(InvokeRequest{Function: "orders", Payload: []byte("after-rollback")})
	if err != nil {
		t.Fatalf("invoke after rollback: %v", err)
	}
	if string(resp.Payload) != "old" || resp.Version != 1 {
		t.Fatalf("after rollback result = %q@v%d, want old@v1", resp.Payload, resp.Version)
	}
}

func TestRollbackRejectsMissingAndUnavailableVersions(t *testing.T) {
	k := testKernel(t, Config{})
	mustPublish(t, k, specReturning("billing", "v1", handlerReturning("one")))
	mustPublish(t, k, specReturning("billing", "v2", handlerReturning("two")))

	if _, err := k.Rollback("billing", 0, ""); ErrorKindOf(err) != KindInvalidArgument {
		t.Fatalf("zero version kind = %s, want invalid_argument", ErrorKindOf(err))
	}
	if _, err := k.Rollback("billing", 99, ""); ErrorKindOf(err) != KindNotFound {
		t.Fatalf("missing version kind = %s, want not_found", ErrorKindOf(err))
	}
	if err := k.SetVersionAvailable("billing", 1, false); err != nil {
		t.Fatalf("disable v1: %v", err)
	}
	if _, err := k.Rollback("billing", 1, ""); ErrorKindOf(err) != KindUnavailable {
		t.Fatalf("unavailable rollback kind = %s, want unavailable; err=%v", ErrorKindOf(err), err)
	}
}

func TestExplicitVersionCallRejectsUnavailableVersion(t *testing.T) {
	k := testKernel(t, Config{})
	mustPublish(t, k, specReturning("users", "v1", handlerReturning("one")))
	if err := k.SetVersionAvailable("users", 1, false); err != nil {
		t.Fatalf("disable version: %v", err)
	}
	_, err := k.Invoke(InvokeRequest{Function: "users", Version: 1, Payload: []byte("disabled-version")})
	if ErrorKindOf(err) != KindUnavailable {
		t.Fatalf("kind = %s, want unavailable", ErrorKindOf(err))
	}
}

func specReturning(name, marker string, handler HandlerFunc) FunctionSpec {
	_ = marker
	return FunctionSpec{
		Name:           name,
		TimeoutMillis:  100,
		MaxConcurrency: 2,
		Handler:        handler,
		Retries:        RetryPolicy{MaxAttempts: 1},
	}
}

func TestRollbackWithMissingRouteDoesNotMoveCurrent(t *testing.T) {
	k := testKernel(t, Config{})
	mustPublish(t, k, specReturning("atomic-rollback", "v1", handlerReturning("old")))
	mustPublish(t, k, specReturning("atomic-rollback", "v2", handlerReturning("new")))
	_, err := k.Rollback("atomic-rollback", 1, "/missing-rollback-route")
	if ErrorKindOf(err) != KindNotFound {
		t.Fatalf("rollback missing route kind=%s want not_found; err=%v", ErrorKindOf(err), err)
	}
	resp, err := k.Invoke(InvokeRequest{Function: "atomic-rollback", Payload: []byte("current-unchanged")})
	if err != nil {
		t.Fatalf("invoke after failed rollback: %v", err)
	}
	if resp.Version != 2 || string(resp.Payload) != "new" {
		t.Fatalf("current after failed rollback = %q@v%d, want new@v2", resp.Payload, resp.Version)
	}
}

func TestRollbackCanRepointPinnedRouteAtomically(t *testing.T) {
	k := testKernel(t, Config{})
	mustPublish(t, k, specReturning("route-back", "v1", handlerReturning("old")))
	mustPublish(t, k, specReturning("route-back", "v2", handlerReturning("new")))
	if err := k.SetRoute("/back", RouteTarget{Function: "route-back", Version: 2}); err != nil {
		t.Fatalf("set route: %v", err)
	}
	if _, err := k.Rollback("route-back", 1, "/back"); err != nil {
		t.Fatalf("rollback and repoint: %v", err)
	}
	resp, err := k.Invoke(InvokeRequest{Route: "/back", Payload: []byte("route-rollback")})
	if err != nil {
		t.Fatalf("invoke repointed route: %v", err)
	}
	if resp.Version != 1 || string(resp.Payload) != "old" {
		t.Fatalf("route result=%q@v%d, want old@v1", resp.Payload, resp.Version)
	}
}
