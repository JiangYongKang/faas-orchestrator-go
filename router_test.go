package faas

import "testing"

func TestRouter_ConflictAndUnknown(t *testing.T) {
	rt := NewRouter()

	assertClass(t, rt.AddRoute(Route{Path: "", FunctionName: "fn"}), ErrClassInvalid)
	assertClass(t, rt.AddRoute(Route{Path: "/a", FunctionName: ""}), ErrClassInvalid)
	logCase(t, "输入空 path / 空 function → invalid")

	assertOK(t, rt.AddRoute(Route{Path: "/a", FunctionName: "fnA", VersionPin: 0}), "绑定 /a->fnA")

	// 冲突：同路径不同目标，必须拒绝而不是静默覆盖。
	err := rt.AddRoute(Route{Path: "/a", FunctionName: "fnB", VersionPin: 0})
	assertClass(t, err, ErrClassConflict)
	got, err := rt.Resolve("/a")
	assertOK(t, err, "冲突后解析 /a")
	if got.FunctionName != "fnA" {
		t.Fatalf("冲突绑定不得覆盖原路由，got %s", got.FunctionName)
	}
	logCase(t, "输入冲突绑定 /a->fnB → conflict，原绑定 fnA 不变")

	// 幂等：完全相同的绑定重复登记成功。
	assertOK(t, rt.AddRoute(Route{Path: "/a", FunctionName: "fnA", VersionPin: 0}), "幂等重复绑定")

	// 钉选版本变化也算冲突。
	err = rt.AddRoute(Route{Path: "/a", FunctionName: "fnA", VersionPin: 3})
	assertClass(t, err, ErrClassConflict)
	logCase(t, "输入同函数但 pin 0→3 → conflict")

	// 未知路径不得静默落到默认实现。
	_, err = rt.Resolve("/nope")
	assertClass(t, err, ErrClassNotFound)
	logCase(t, "输入未知路径 /nope → not_found，无默认兜底")

	// 删除后再次解析必须 not_found。
	assertOK(t, rt.RemoveRoute("/a"), "删除 /a")
	_, err = rt.Resolve("/a")
	assertClass(t, err, ErrClassNotFound)
	assertClass(t, rt.RemoveRoute("/a"), ErrClassNotFound)
}
