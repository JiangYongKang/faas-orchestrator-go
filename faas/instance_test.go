package faas

import (
	"context"
	"testing"
	"time"
)

func TestInstanceReuseThenIdleReclaim(t *testing.T) {
	k := testKernel(t, Config{IdleTTL: 15 * time.Millisecond})
	mustPublish(t, k, echoSpec("reuse"))
	req := InvokeRequest{Function: "reuse", Payload: []byte("first")}
	first, err := k.Invoke(req)
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	starts := k.pool.ColdStarts()
	second, err := k.Invoke(req)
	if err != nil {
		t.Fatalf("second invoke: %v", err)
	}
	if second.Version != first.Version || k.pool.ColdStarts() != starts {
		t.Fatalf("idle instance was not reused: starts before=%d after=%d", starts, k.pool.ColdStarts())
	}
	if n := k.ReclaimIdleInstances(); n != 0 {
		t.Fatalf("fresh idle reclaimed=%d, want 0", n)
	}
	time.Sleep(30 * time.Millisecond)
	if n := k.ReclaimIdleInstances(); n != 1 {
		t.Fatalf("expired idle reclaimed=%d, want 1", n)
	}
	if n := k.ReclaimIdleInstances(); n != 0 {
		t.Fatalf("second reclaim=%d, want 0 duplicate reclaim", n)
	}
	_, err = k.Invoke(req)
	if err != nil {
		t.Fatalf("invoke after reclaim: %v", err)
	}
	if k.pool.ColdStarts() <= starts {
		t.Fatalf("cold starts did not increase after reclaim")
	}
}

func TestReclaimSkipsBusyInstances(t *testing.T) {
	k := testKernel(t, Config{IdleTTL: 0})
	entered := make(chan struct{})
	release := make(chan struct{})
	mustPublish(t, k, FunctionSpec{
		Name:           "busy-reclaim",
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
		_, _ = k.Invoke(InvokeRequest{Function: "busy-reclaim", Payload: []byte("busy")})
	}()
	<-entered
	time.Sleep(2 * time.Millisecond)
	if n := k.ReclaimIdleInstances(); n != 0 {
		t.Fatalf("reclaim busy=%d, want 0", n)
	}
	for _, inst := range k.pool.Snapshot() {
		if inst.Function == "busy-reclaim" && inst.State != InstanceBusy {
			t.Fatalf("busy instance state=%s, want busy", inst.State)
		}
	}
	close(release)
	<-done
}
