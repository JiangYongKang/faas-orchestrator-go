package faas

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestFunctionErrorIsNotRetriedByDefault(t *testing.T) {
	k := testKernel(t, Config{})
	var attempts atomic.Int32
	mustPublish(t, k, FunctionSpec{
		Name:           "no-retry-business",
		TimeoutMillis:  50,
		MaxConcurrency: 2,
		Retries:        RetryPolicy{MaxAttempts: 3},
		Handler: func(context.Context, *InvocationContext) ([]byte, error) {
			attempts.Add(1)
			return nil, context.DeadlineExceeded // still wrapped as function error
		},
	})
	_, err := k.Invoke(InvokeRequest{Protocol: ProtocolV2, Function: "no-retry-business", IdempotencyKey: "biz-1", Payload: []byte("business-fail")})
	if ErrorKindOf(err) != KindFunctionError {
		t.Fatalf("kind=%s want function_error", ErrorKindOf(err))
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts=%d want 1 non-retried business failure", attempts.Load())
	}
}

func TestThrottledFailureRetriesWithinSameCall(t *testing.T) {
	k := testKernel(t, Config{RetryWait: func(int) { time.Sleep(25 * time.Millisecond) }})
	var workAttempts atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	mustPublish(t, k, FunctionSpec{
		Name:           "retry-throttle",
		TimeoutMillis:  500,
		MaxConcurrency: 1,
		Retries: RetryPolicy{MaxAttempts: 3, Retryable: map[FailureClass]bool{
			FailureThrottled: true,
		}},
		Handler: func(ctx context.Context, _ *InvocationContext) ([]byte, error) {
			n := workAttempts.Add(1)
			if n == 1 {
				close(entered)
				select {
				case <-release:
					return []byte("holder-done"), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return []byte("recovered"), nil
		},
	})

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		_, _ = k.Invoke(InvokeRequest{Function: "retry-throttle", IdempotencyKey: "holder", Payload: []byte("holder")})
	}()
	<-entered
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(release)
	}()
	resp, err := k.Invoke(InvokeRequest{Protocol: ProtocolV2, Function: "retry-throttle", IdempotencyKey: "retry-1", Payload: []byte("throttled-then-retry")})
	if err != nil {
		t.Fatalf("retry call: %v", err)
	}
	<-firstDone
	if string(resp.Payload) != "recovered" || resp.Attempt != 2 {
		t.Fatalf("resp=%q attempt=%d, want recovered attempt 2", resp.Payload, resp.Attempt)
	}
}

func TestConcurrentIdempotentCallsProduceOneSideEffect(t *testing.T) {
	k := testKernel(t, Config{})
	var sideEffects atomic.Int32
	mustPublish(t, k, FunctionSpec{
		Name:           "idempotent",
		TimeoutMillis:  200,
		MaxConcurrency: 4,
		Handler: func(context.Context, *InvocationContext) ([]byte, error) {
			sideEffects.Add(1)
			time.Sleep(20 * time.Millisecond)
			return []byte("ok"), nil
		},
	})
	const n = 8
	errs := make(chan error, n)
	for range n {
		go func() {
			_, err := k.Invoke(InvokeRequest{Function: "idempotent", IdempotencyKey: "same-key", Payload: []byte("same-input")})
			errs <- err
		}()
	}
	for range n {
		if err := <-errs; err != nil {
			t.Fatalf("duplicate call: %v", err)
		}
	}
	if sideEffects.Load() != 1 {
		t.Fatalf("side effects=%d want 1", sideEffects.Load())
	}
}

func TestRetryablePolicyRequiresIdempotencyKey(t *testing.T) {
	k := testKernel(t, Config{})
	mustPublish(t, k, FunctionSpec{
		Name:           "needs-key",
		TimeoutMillis:  50,
		MaxConcurrency: 1,
		Handler:        func(context.Context, *InvocationContext) ([]byte, error) { return nil, context.DeadlineExceeded },
		Retries: RetryPolicy{MaxAttempts: 2, Retryable: map[FailureClass]bool{
			FailureTimeout: true,
		}},
	})
	_, err := k.Invoke(InvokeRequest{Function: "needs-key", Payload: []byte("no-key")})
	if ErrorKindOf(err) != KindInvalidArgument {
		t.Fatalf("kind=%s want invalid_argument", ErrorKindOf(err))
	}
}
