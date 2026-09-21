package faas

import (
	"context"
	"sync"
	"testing"
)

type testLogger struct {
	t   *testing.T
	mu  sync.Mutex
	all []DecisionLog
}

func newTestLogger(t *testing.T) *testLogger {
	t.Helper()
	return &testLogger{t: t}
}

func (l *testLogger) Log(entry DecisionLog) {
	l.mu.Lock()
	l.all = append(l.all, entry)
	l.mu.Unlock()
	l.t.Logf("decision=%s request=%s input=%q reason=%s", entry.Decision, entry.RequestID, string(entry.Input), entry.Reason)
}

func (l *testLogger) contains(decision string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, entry := range l.all {
		if entry.Decision == decision {
			return true
		}
	}
	return false
}

func testKernel(t *testing.T, config Config) *Kernel {
	t.Helper()
	if config.Logger == nil {
		config.Logger = newTestLogger(t)
	}
	if config.IdleTTL == 0 {
		config.IdleTTL = 1
	}
	k := New(config)
	t.Cleanup(func() {
		if store, ok := k.store.(*LocalStore); ok {
			_ = store.Close()
		}
	})
	return k
}

func mustPublish(t *testing.T, k *Kernel, spec FunctionSpec) PublishedVersion {
	t.Helper()
	version, err := k.Publish(spec)
	if err != nil {
		t.Fatalf("publish %s: %v", spec.Name, err)
	}
	return version
}

func echoSpec(name string) FunctionSpec {
	return FunctionSpec{
		Name:           name,
		TimeoutMillis:  100,
		MaxConcurrency: 2,
		Handler: func(_ context.Context, call *InvocationContext) ([]byte, error) {
			return append([]byte(nil), call.Payload...), nil
		},
	}
}
