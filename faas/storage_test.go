package faas

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalStoreRecordsPublishRollbackAndInvocation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	store, err := OpenLocalStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	k := testKernel(t, Config{Store: store})
	v1 := mustPublish(t, k, echoSpec("audited"))
	mustPublish(t, k, FunctionSpec{
		Name:           "audited",
		TimeoutMillis:  100,
		MaxConcurrency: 1,
		Handler:        echoSpec("ignored").Handler,
	})
	if _, err := k.Rollback("audited", v1.Number, ""); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, err := k.Invoke(InvokeRequest{Function: "audited", Payload: []byte("audit-input")}); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open event file: %v", err)
	}
	defer f.Close()
	kinds := map[EventKind]int{}
	scanner := bufio.NewScanner(f)
	sawPublishedV1 := false
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var event StoredEvent
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("decode event: %v", err)
		}
		kinds[event.Kind]++
		if event.Kind == EventPublished && event.Version == 1 {
			sawPublishedV1 = true
		}
	}
	if !sawPublishedV1 {
		t.Fatalf("published v1 event was not recorded")
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	for kind, want := range map[EventKind]int{EventPublished: 2, EventRollback: 1, EventInvoke: 1} {
		if kinds[kind] != want {
			t.Fatalf("%s events=%d want %d, all=%v", kind, kinds[kind], want, kinds)
		}
	}
}

type failingStore struct{}

func (failingStore) Append(StoredEvent) error {
	return &Error{Kind: KindPlatformError, Reason: "simulated disk failure"}
}

func TestStorageFailureIsReportedAsPlatformError(t *testing.T) {
	failingKernel := testKernel(t, Config{Store: failingStore{}})
	if _, err := failingKernel.Publish(echoSpec("storage-fails")); ErrorKindOf(err) != KindPlatformError {
		t.Fatalf("publish storage kind=%s want platform_error; err=%v", ErrorKindOf(err), err)
	}

	k := testKernel(t, Config{})
	mustPublish(t, k, echoSpec("storage-invoke-fails"))
	k.store = failingStore{}
	_, err := k.Invoke(InvokeRequest{Function: "storage-invoke-fails", Payload: []byte("disk-fail")})
	if ErrorKindOf(err) != KindPlatformError {
		t.Fatalf("invoke storage kind=%s want platform_error; err=%v", ErrorKindOf(err), err)
	}
}

func TestRollbackStorageFailureRestoresCurrentPointer(t *testing.T) {
	k := testKernel(t, Config{})
	v1 := mustPublish(t, k, FunctionSpec{
		Name:           "rollback-storage",
		TimeoutMillis:  100,
		MaxConcurrency: 1,
		Handler:        handlerReturning("old"),
	})
	mustPublish(t, k, FunctionSpec{
		Name:           "rollback-storage",
		TimeoutMillis:  100,
		MaxConcurrency: 1,
		Handler:        handlerReturning("new"),
	})
	k.store = failingStore{}
	_, err := k.Rollback("rollback-storage", v1.Number, "")
	if ErrorKindOf(err) != KindPlatformError {
		t.Fatalf("rollback storage kind=%s want platform_error; err=%v", ErrorKindOf(err), err)
	}
	k.store = nil
	resp, err := k.Invoke(InvokeRequest{Function: "rollback-storage", Payload: []byte("restored-current")})
	if err != nil {
		t.Fatalf("invoke after failed rollback storage: %v", err)
	}
	if resp.Version != 2 || string(resp.Payload) != "new" {
		t.Fatalf("current = %q@v%d, want new@v2", resp.Payload, resp.Version)
	}
}
