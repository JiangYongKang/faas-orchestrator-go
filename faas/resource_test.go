package faas

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTimeoutTerminatesInstanceAndDoesNotPolluteNextCall(t *testing.T) {
	k := testKernel(t, Config{})
	mustPublish(t, k, FunctionSpec{
		Name:           "slow",
		TimeoutMillis:  20,
		MaxConcurrency: 2,
		Handler: func(ctx context.Context, call *InvocationContext) ([]byte, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(200 * time.Millisecond):
				return []byte("late"), nil
			}
		},
	})
	before := k.pool.ColdStarts()
	_, err := k.Invoke(InvokeRequest{Function: "slow", Payload: []byte("will-timeout")})
	if ErrorKindOf(err) != KindTimeout {
		t.Fatalf("timeout kind = %s, want timeout; err=%v", ErrorKindOf(err), err)
	}
	for _, inst := range k.pool.Snapshot() {
		if inst.Function == "slow" && inst.State != InstanceReclaimed {
			t.Fatalf("timed out instance %s state=%s, want reclaimed", inst.ID, inst.State)
		}
	}
	mustPublish(t, k, FunctionSpec{
		Name:           "slow",
		TimeoutMillis:  50,
		MaxConcurrency: 2,
		Handler: func(_ context.Context, call *InvocationContext) ([]byte, error) {
			return []byte("fast:" + string(call.Payload)), nil
		},
	})
	resp, err := k.Invoke(InvokeRequest{Function: "slow", Payload: []byte("after-timeout")})
	if err != nil {
		t.Fatalf("new current version after timeout: %v", err)
	}
	if string(resp.Payload) != "fast:after-timeout" || resp.Version != 2 {
		t.Fatalf("resp=%q@v%d, want fast:after-timeout@v2", resp.Payload, resp.Version)
	}
	if got := k.pool.ColdStarts() - before; got < 2 {
		t.Fatalf("cold starts after timeout and new call = %d, want at least 2", got)
	}
}

func TestConcurrencyLimitRejectsInsteadOfQueuing(t *testing.T) {
	k := testKernel(t, Config{})
	entered := make(chan struct{})
	release := make(chan struct{})
	mustPublish(t, k, FunctionSpec{
		Name:           "limited",
		TimeoutMillis:  500,
		MaxConcurrency: 1,
		Handler: func(ctx context.Context, call *InvocationContext) ([]byte, error) {
			close(entered)
			select {
			case <-release:
				return []byte("done"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	})
	firstDone := make(chan struct{})
	var firstErr error
	go func() {
		defer close(firstDone)
		_, firstErr = k.Invoke(InvokeRequest{Function: "limited", Payload: []byte("first")})
	}()
	<-entered
	_, err := k.Invoke(InvokeRequest{Function: "limited", Payload: []byte("second")})
	if ErrorKindOf(err) != KindConcurrencyLimit {
		t.Fatalf("second call kind = %s, want concurrency_limit; err=%v", ErrorKindOf(err), err)
	}
	close(release)
	<-firstDone
	if firstErr != nil {
		t.Fatalf("first call: %v", firstErr)
	}
}

func TestFunctionErrorIsDistinctFromPlatformError(t *testing.T) {
	k := testKernel(t, Config{})
	mustPublish(t, k, FunctionSpec{
		Name:           "fails",
		TimeoutMillis:  50,
		MaxConcurrency: 1,
		Handler: func(context.Context, *InvocationContext) ([]byte, error) {
			return nil, errors.New("business invariant rejected")
		},
	})
	_, err := k.Invoke(InvokeRequest{Function: "fails", Payload: []byte("bad-business-input")})
	if ErrorKindOf(err) != KindFunctionError {
		t.Fatalf("kind = %s, want function_error", ErrorKindOf(err))
	}
}

func TestHandlerPanicIsFunctionErrorAndRetiresInstance(t *testing.T) {
	k := testKernel(t, Config{})
	mustPublish(t, k, FunctionSpec{
		Name:           "panics",
		TimeoutMillis:  50,
		MaxConcurrency: 1,
		Handler: func(context.Context, *InvocationContext) ([]byte, error) {
			panic("boom")
		},
	})
	_, err := k.Invoke(InvokeRequest{Function: "panics", Payload: []byte("panic-input")})
	if ErrorKindOf(err) != KindFunctionError {
		t.Fatalf("panic kind=%s want function_error; err=%v", ErrorKindOf(err), err)
	}
	for _, inst := range k.pool.Snapshot() {
		if inst.Function == "panics" && inst.State != InstanceReclaimed {
			t.Fatalf("panic instance %s state=%s want reclaimed", inst.ID, inst.State)
		}
	}
}
