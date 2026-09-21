package faas

import "testing"

func TestRoutingNeverUsesDefault(t *testing.T) {
	k := testKernel(t, Config{})
	v := mustPublish(t, k, echoSpec("payments"))

	if _, err := k.Invoke(InvokeRequest{Route: "/missing", Payload: []byte("unknown-route")}); ErrorKindOf(err) != KindNotFound {
		t.Fatalf("unknown route kind = %s, want not_found", ErrorKindOf(err))
	}
	if err := k.SetRoute("/pay", RouteTarget{Function: "payments", Version: v.Number}); err != nil {
		t.Fatalf("set route: %v", err)
	}
	if err := k.SetRoute("/pay", RouteTarget{Function: "payments", Version: 99}); ErrorKindOf(err) != KindConflict {
		t.Fatalf("same route different target kind = %s, want conflict", ErrorKindOf(err))
	}
	if err := k.SetRoute("/zero", RouteTarget{Function: "payments"}); ErrorKindOf(err) != KindInvalidArgument {
		t.Fatalf("zero target kind = %s, want invalid_argument", ErrorKindOf(err))
	}
	resp, err := k.Invoke(InvokeRequest{Protocol: ProtocolV2, Route: "/pay", Payload: []byte("routed")})
	if err != nil {
		t.Fatalf("routed invoke: %v", err)
	}
	if resp.Version != v.Number || string(resp.Payload) != "routed" {
		t.Fatalf("resp = %q@v%d", resp.Payload, resp.Version)
	}
}

func TestFunctionWithoutPublishedVersionIsUnavailable(t *testing.T) {
	k := testKernel(t, Config{})
	if err := k.Register(echoSpec("drafts")); err != nil {
		t.Fatalf("register: %v", err)
	}
	_, err := k.Invoke(InvokeRequest{Function: "drafts", Payload: []byte("no-version")})
	if ErrorKindOf(err) != KindUnavailable {
		t.Fatalf("kind = %s, want unavailable", ErrorKindOf(err))
	}
}

func TestProtocolCompatibilityAndUnsupportedOptions(t *testing.T) {
	k := testKernel(t, Config{})
	v := mustPublish(t, k, echoSpec("proto"))
	if err := k.SetRoute("/proto", RouteTarget{Function: "proto", Version: v.Number}); err != nil {
		t.Fatalf("route: %v", err)
	}

	if _, err := k.Invoke(InvokeRequest{Protocol: "bad", Route: "/proto"}); ErrorKindOf(err) != KindProtocolIncompatible {
		t.Fatalf("bad protocol kind = %s, want protocol_incompatible", ErrorKindOf(err))
	}
	v1Req := InvokeRequest{Protocol: ProtocolV1, Route: "/proto", Options: CallOptions{MaxAttempts: 2}}
	if _, err := k.Invoke(v1Req); ErrorKindOf(err) != KindUnsupportedOption {
		t.Fatalf("v1 retry override kind = %s, want unsupported_option", ErrorKindOf(err))
	}
	bigTimeout := InvokeRequest{Protocol: ProtocolV2, Route: "/proto", Payload: []byte("too-long"), Options: CallOptions{TimeoutMillis: 99999}}
	if _, err := k.Invoke(bigTimeout); ErrorKindOf(err) != KindUnsupportedOption {
		t.Fatalf("timeout override kind = %s, want unsupported_option", ErrorKindOf(err))
	}
	unknown := InvokeRequest{Protocol: ProtocolV2, Route: "/proto", Payload: []byte("unknown-option"), Options: CallOptions{UnsupportedFields: map[string]any{"x": 1}}}
	if _, err := k.Invoke(unknown); ErrorKindOf(err) != KindUnsupportedOption {
		t.Fatalf("unknown option kind = %s, want unsupported_option", ErrorKindOf(err))
	}
}

func TestSetRouteNeverBindsUnavailableTarget(t *testing.T) {
	k := testKernel(t, Config{})
	v := mustPublish(t, k, echoSpec("target-disable"))
	if err := k.SetVersionAvailable("target-disable", v.Number, false); err != nil {
		t.Fatalf("disable target: %v", err)
	}
	err := k.SetRoute("/never-bound", RouteTarget{Function: "target-disable", Version: v.Number})
	if ErrorKindOf(err) != KindUnavailable {
		t.Fatalf("set disabled route kind=%s want unavailable; err=%v", ErrorKindOf(err), err)
	}
	if _, err := k.router.Resolve("/never-bound"); err == nil {
		t.Fatalf("invalid target left a half-bound route")
	}
}

func TestUnknownFunctionIsNotFound(t *testing.T) {
	k := testKernel(t, Config{})
	_, err := k.Invoke(InvokeRequest{Function: "missing-function", Payload: []byte("unknown-function")})
	if ErrorKindOf(err) != KindNotFound {
		t.Fatalf("unknown function kind=%s want not_found; err=%v", ErrorKindOf(err), err)
	}
}
