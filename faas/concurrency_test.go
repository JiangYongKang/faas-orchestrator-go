package faas

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestConcurrentCallsStayConsistentAcrossRollback(t *testing.T) {
	k := testKernel(t, Config{})
	mustPublish(t, k, FunctionSpec{
		Name:           "switching",
		TimeoutMillis:  200,
		MaxConcurrency: 64,
		Handler:        func(context.Context, *InvocationContext) ([]byte, error) { return []byte("v1"), nil },
	})
	mustPublish(t, k, FunctionSpec{
		Name:           "switching",
		TimeoutMillis:  200,
		MaxConcurrency: 64,
		Handler:        func(context.Context, *InvocationContext) ([]byte, error) { return []byte("v2"), nil },
	})

	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	results := make(chan string, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i == n/2 {
				if _, err := k.Rollback("switching", 1, ""); err != nil {
					errs <- err
					return
				}
			}
			resp, err := k.Invoke(InvokeRequest{Function: "switching", Payload: []byte("concurrent-switch")})
			if err != nil {
				errs <- err
				return
			}
			if got := string(resp.Payload); got == "v1" && resp.Version == 1 || got == "v2" && resp.Version == 2 {
				results <- got
				return
			}
			errs <- &Error{Kind: KindPlatformError, Reason: "half-updated response"}
		}(i)
	}
	wg.Wait()
	close(errs)
	close(results)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent call: %v", err)
		}
	}
}

func TestDrainVersionReclaimsBusyInstanceOnRelease(t *testing.T) {
	k := testKernel(t, Config{})
	entered := make(chan struct{})
	release := make(chan struct{})
	mustPublish(t, k, FunctionSpec{
		Name:           "drain-me",
		TimeoutMillis:  500,
		MaxConcurrency: 1,
		Handler: func(context.Context, *InvocationContext) ([]byte, error) {
			close(entered)
			<-release
			return []byte("ok"), nil
		},
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = k.Invoke(InvokeRequest{Function: "drain-me", Payload: []byte("draining")})
	}()
	<-entered
	time.Sleep(2 * time.Millisecond)
	affected := k.DrainVersion("drain-me", 1)
	if affected != 1 {
		t.Fatalf("drain affected=%d want 1 busy instance", affected)
	}
	for _, inst := range k.pool.Snapshot() {
		if inst.Function == "drain-me" && inst.State != InstanceDraining {
			t.Fatalf("during drain state=%s want draining", inst.State)
		}
	}
	close(release)
	<-done
	for _, inst := range k.pool.Snapshot() {
		if inst.Function == "drain-me" && inst.State != InstanceReclaimed {
			t.Fatalf("after release state=%s want reclaimed", inst.State)
		}
	}
}

func TestRollbackRouteMigrationIsAtomicForReaders(t *testing.T) {
	k := testKernel(t, Config{})
	v1 := mustPublish(t, k, FunctionSpec{
		Name:           "route-atomic",
		TimeoutMillis:  100,
		MaxConcurrency: 64,
		Handler:        func(context.Context, *InvocationContext) ([]byte, error) { return []byte("v1"), nil },
	})
	mustPublish(t, k, FunctionSpec{
		Name:           "route-atomic",
		TimeoutMillis:  100,
		MaxConcurrency: 64,
		Handler:        func(context.Context, *InvocationContext) ([]byte, error) { return []byte("v2"), nil },
	})
	if err := k.SetRoute("/r", RouteTarget{Function: "route-atomic", Version: 2}); err != nil {
		t.Fatalf("set route: %v", err)
	}

	const n = 30
	var wg sync.WaitGroup
	errs := make(chan error, n+1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := k.Rollback("route-atomic", v1.Number, "/r"); err != nil {
			errs <- err
		}
	}()
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			target, err := k.router.Resolve("/r")
			if err != nil {
				errs <- err
				return
			}
			if target.Function != "route-atomic" || target.Version != 1 && target.Version != 2 {
				errs <- &Error{Kind: KindPlatformError, Reason: "route read returned invalid target"}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent route read/rollback: %v", err)
	}
}
