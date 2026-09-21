package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/highcumontoa/faas-orchestrator-go/faas"
)

type stdoutLogger struct{}

func (stdoutLogger) Log(entry faas.DecisionLog) {
	fmt.Printf("[%s] input=%q reason=%s\n", entry.Decision, string(entry.Input), entry.Reason)
}

func main() {
	path := filepath.Join(os.TempDir(), "faas-demo-events.jsonl")
	store, err := faas.OpenLocalStore(path)
	must(err)
	defer store.Close()

	k := faas.New(faas.Config{
		DefaultTimeoutMillis:  100,
		DefaultMaxConcurrency: 2,
		IdleTTL:               time.Millisecond,
		Logger:                stdoutLogger{},
		Store:                 store,
	})

	v1, err := k.Publish(faas.FunctionSpec{
		Name:           "demo",
		TimeoutMillis:  50,
		MaxConcurrency: 1,
		Handler: func(_ context.Context, call *faas.InvocationContext) ([]byte, error) {
			return []byte("old:" + string(call.Payload)), nil
		},
	})
	must(err)
	v2, err := k.Publish(faas.FunctionSpec{
		Name:           "demo",
		TimeoutMillis:  50,
		MaxConcurrency: 1,
		Handler: func(_ context.Context, call *faas.InvocationContext) ([]byte, error) {
			return []byte("new:" + string(call.Payload)), nil
		},
	})
	must(err)
	must(k.SetRoute("/demo", faas.RouteTarget{Function: "demo", Version: v2.Number}))

	current, err := k.Invoke(faas.InvokeRequest{Protocol: faas.ProtocolV2, Function: "demo", Payload: []byte("current")})
	must(err)
	fmt.Printf("before rollback: v%d => %s\n", current.Version, current.Payload)

	routed, err := k.Invoke(faas.InvokeRequest{Protocol: faas.ProtocolV2, Route: "/demo", Payload: []byte("route")})
	must(err)
	fmt.Printf("pinned route before rollback: v%d => %s\n", routed.Version, routed.Payload)

	_, err = k.Rollback("demo", v1.Number, "")
	must(err)
	after, err := k.Invoke(faas.InvokeRequest{Protocol: faas.ProtocolV2, Function: "demo", Payload: []byte("after")})
	must(err)
	fmt.Printf("current after rollback: v%d => %s\n", after.Version, after.Payload)

	_, err = k.Invoke(faas.InvokeRequest{Protocol: faas.ProtocolV2, Route: "/demo", Payload: []byte("still-pinned")})
	must(err)
	fmt.Println("pinned route stayed on v2")

	var wg sync.WaitGroup
	entered := make(chan struct{})
	release := make(chan struct{})
	_, err = k.Publish(faas.FunctionSpec{
		Name:           "limited",
		TimeoutMillis:  200,
		MaxConcurrency: 1,
		Handler: func(ctx context.Context, _ *faas.InvocationContext) ([]byte, error) {
			close(entered)
			select {
			case <-release:
				return []byte("ok"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	})
	must(err)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = k.Invoke(faas.InvokeRequest{Protocol: faas.ProtocolV2, Function: "limited", Payload: []byte("first")})
	}()
	<-entered
	_, err = k.Invoke(faas.InvokeRequest{Protocol: faas.ProtocolV2, Function: "limited", Payload: []byte("second")})
	fmt.Printf("expected concurrency error kind=%s reason=%v\n", faas.ErrorKindOf(err), err)
	close(release)
	wg.Wait()

	fmt.Printf("event log: %s\n", path)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
