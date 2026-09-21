package faas

import "testing"

func TestRegisterAndPublish_HappyPathAndDuplicates(t *testing.T) {
	r := NewRegistry()

	assertClass(t, r.Register(""), ErrClassInvalid)
	logCase(t, "输入 name=\"\" → 拒绝空函数名")

	assertOK(t, r.Register("fn1"), "注册 fn1")
	assertClass(t, r.Register("fn1"), ErrClassConflict)
	logCase(t, "输入重复注册 fn1 → conflict，不覆盖已有元数据")

	assertClass(t, asErr(r.Publish("missing", testCfg("echo"))), ErrClassNotFound)
	logCase(t, "输入未知函数 missing → not_found")

	bad := testCfg("echo")
	bad.Timeout = 0
	assertClass(t, asErr(r.Publish("fn1", bad)), ErrClassInvalid)
	logCase(t, "输入 timeout=0 → invalid，不接受静默默认值")

	bad = testCfg("echo")
	bad.MaxConcurrency = 0
	assertClass(t, asErr(r.Publish("fn1", bad)), ErrClassInvalid)

	bad = testCfg("")
	assertClass(t, asErr(r.Publish("fn1", bad)), ErrClassInvalid)
	logCase(t, "输入 handler=\"\" → invalid")

	v1, err := r.Publish("fn1", testCfg("echo"))
	assertOK(t, err, "发布 v1")
	if v1.Version != 1 {
		t.Fatalf("首个版本号应为 1, got %d", v1.Version)
	}
	v2, err := r.Publish("fn1", testCfg("slow"))
	assertOK(t, err, "发布 v2")
	if v2.Version != 2 {
		t.Fatalf("版本号应单调递增, got %d", v2.Version)
	}

	got, err := r.ResolveVersion("fn1")
	assertOK(t, err, "解析活动版本")
	if got.Version != 2 {
		t.Fatalf("发布后活动版本应为 2, got %d", got.Version)
	}
	logCase(t, "输入连续发布 v1,v2 → 活动指针=%d，判定依据 ResolveVersion", got.Version)
}

func TestRollback_IllegalAndUnavailableTargets(t *testing.T) {
	r := NewRegistry()
	_ = r.Register("fn")
	v1, _ := r.Publish("fn", testCfg("echo"))
	_, _ = r.Publish("fn", testCfg("slow"))

	cases := []struct {
		name    string
		version int
		want    ErrorClass
		input   string
	}{
		{"版本号为0", 0, ErrClassInvalid, "version=0"},
		{"负版本号", -3, ErrClassInvalid, "version=-3"},
		{"不存在的版本", 99, ErrClassNotFound, "version=99"},
		{"回滚到当前活动版本", 2, ErrClassInvalid, "version=2(active)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			logCase(t, "输入 %s", c.input)
			_, err := r.Rollback("fn", c.version)
			assertClass(t, err, c.want)
		})
	}

	// 下线 v1 后回滚必须被拒绝（指向不可用版本）。
	assertOK(t, r.SetVersionState("fn", v1.Version, VersionDisabled), "下线 v1")
	_, err := r.Rollback("fn", v1.Version)
	assertClass(t, err, ErrClassUnavailable)
	logCase(t, "输入 rollback 到 disabled v1 → unavailable，原因与 not_found 可区分")

	// 重新启用后回滚成功，且新解析命中目标版本。
	assertOK(t, r.SetVersionState("fn", v1.Version, VersionActive), "重新启用 v1")
	got, err := r.Rollback("fn", v1.Version)
	assertOK(t, err, "回滚到 v1")
	if got.Version != 1 {
		t.Fatalf("回滚后应返回 v1, got v%d", got.Version)
	}
	cur, _ := r.ResolveVersion("fn")
	if cur.Version != 1 {
		t.Fatalf("回滚后新调用解析应命中 v1, got v%d", cur.Version)
	}
	logCase(t, "输入回滚 v2→v1 → ResolveVersion=v1，判定依据：活动指针已切换")

	// retired 版本永久不可回滚。
	assertOK(t, r.SetVersionState("fn", 2, VersionRetired), "废弃 v2")
	_, err = r.Rollback("fn", 2)
	assertClass(t, err, ErrClassUnavailable)
	logCase(t, "输入 rollback 到 retired v2 → unavailable(永久)")

	// 历史中应有 publish/publish/rollback 等可追溯记录。
	snap, _ := r.Snapshot("fn")
	logCase(t, "历史记录数=%d（判定依据：发布与回滚均追加不可变历史）", len(snap.History))
	if len(snap.History) < 4 {
		t.Fatalf("历史记录应至少包含 2 publish + disable/enable + rollback, got %d", len(snap.History))
	}
}

func TestResolveForCall_StateGate(t *testing.T) {
	r := NewRegistry()
	_ = r.Register("fn")
	v1, _ := r.Publish("fn", testCfg("echo"))
	_, _ = r.Publish("fn", testCfg("slow"))

	// 钉选未发布版本。
	_, err := r.ResolveForCall("fn", 77)
	assertClass(t, err, ErrClassNotFound)

	// 钉选已下线版本。
	assertOK(t, r.SetVersionState("fn", v1.Version, VersionDisabled), "下线 v1")
	_, err = r.ResolveForCall("fn", v1.Version)
	assertClass(t, err, ErrClassUnavailable)
	logCase(t, "输入 pin=v1(disabled) → unavailable，不允许静默改调活动版本")

	// 活动版本被下线后，跟随活动指针的调用也必须失败。
	assertOK(t, r.SetVersionState("fn", 2, VersionDisabled), "下线活动 v2")
	_, err = r.ResolveForCall("fn", 0)
	assertClass(t, err, ErrClassUnavailable)
	logCase(t, "输入 pin=0 且活动 v2 disabled → unavailable，无静默兜底")
}
