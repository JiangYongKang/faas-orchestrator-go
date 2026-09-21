package faas

import "testing"

func TestLocalProcessTimeoutKillsCommand(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	k := testKernel(t, Config{})
	mustPublish(t, k, FunctionSpec{
		Name:           "proc-slow",
		TimeoutMillis:  20,
		MaxConcurrency: 1,
		Command: &LocalCommand{
			Path: "/bin/sh",
			Args: []string{"-c", "cat >/dev/null; sleep 1"},
		},
	})
	_, err := k.Invoke(InvokeRequest{Function: "proc-slow", Payload: []byte("process-timeout")})
	if ErrorKindOf(err) != KindTimeout {
		t.Fatalf("kind=%s want timeout; err=%v", ErrorKindOf(err), err)
	}
	for _, inst := range k.pool.Snapshot() {
		if inst.Function == "proc-slow" && inst.State != InstanceReclaimed {
			t.Fatalf("process timeout instance=%s state=%s, want reclaimed", inst.ID, inst.State)
		}
	}
}

func TestLocalProcessFunctionExitIsFunctionError(t *testing.T) {
	k := testKernel(t, Config{})
	mustPublish(t, k, FunctionSpec{
		Name:           "proc-fail",
		TimeoutMillis:  100,
		MaxConcurrency: 1,
		Command: &LocalCommand{
			Path: "/bin/sh",
			Args: []string{"-c", "printf deliberate-business-failure >&2; exit 7"},
		},
	})
	_, err := k.Invoke(InvokeRequest{Function: "proc-fail", Payload: []byte("process-fails")})
	if ErrorKindOf(err) != KindFunctionError {
		t.Fatalf("kind=%s want function_error; err=%v", ErrorKindOf(err), err)
	}
}

func TestLocalProcessReceivesIdempotencyKey(t *testing.T) {
	k := testKernel(t, Config{})
	mustPublish(t, k, FunctionSpec{
		Name:           "proc-key",
		TimeoutMillis:  100,
		MaxConcurrency: 1,
		Command: &LocalCommand{
			Path: "/bin/sh",
			Args: []string{"-c", "grep -q '\"idempotency_key\":\"proc-key-1\"' && printf accepted"},
		},
	})
	resp, err := k.Invoke(InvokeRequest{
		Function:       "proc-key",
		IdempotencyKey: "proc-key-1",
		Payload:        []byte("check-key"),
	})
	if err != nil {
		t.Fatalf("invoke local process: %v", err)
	}
	if string(resp.Payload) != "accepted" {
		t.Fatalf("payload=%q want accepted", resp.Payload)
	}
}
