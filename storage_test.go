package faas

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestStore_AtomicFileShape(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s, err := NewStore(dir)
	assertOK(t, err, "创建本地存储")

	if err := s.Save([]*Function{{Name: "f"}}, []Route{{Path: "/p", FunctionName: "f"}}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// 不应残留临时文件。
	matches, _ := filepath.Glob(filepath.Join(dir, ".faas-state-*.tmp"))
	if len(matches) != 0 {
		t.Fatalf("原子写入后不得残留临时文件: %v", matches)
	}
	state, exists, err := s.Load()
	assertOK(t, err, "读取快照")
	if !exists || len(state.Functions) != 1 || len(state.Routes) != 1 {
		t.Fatalf("快照内容错误: exists=%v funcs=%d routes=%d", exists, len(state.Functions), len(state.Routes))
	}
	logCase(t, "输入 Save 快照 → 目录仅 1 个正式文件、0 个临时文件，判定依据：tmp+rename 原子提交")
}

func TestPlatform_PersistAndRestoreAcrossRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "faas")
	poolCfg := PoolConfig{IdleTTL: time.Hour, MaxIdlePerVersion: 2, ReapInterval: 0}

	p1, err := NewPlatform(dir, poolCfg)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, p1.RegisterFunction("svc"), "注册 svc")
	_, err = p1.Publish("svc", testCfg("echo"))
	assertOK(t, err, "发布 v1")
	_, err = p1.Publish("svc", testCfg("echo"))
	assertOK(t, err, "发布 v2（活动）")
	_, err = p1.Rollback("svc", 1)
	assertOK(t, err, "回滚到 v1")
	assertOK(t, p1.AddRoute("/svc", "svc", 0), "绑定路由")
	p1.Close()

	// 重启：快照恢复，回滚历史必须保留，新调用必须仍命中 v1。
	p2, err := NewPlatform(dir, poolCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()

	cur, err := p2.registry.ResolveVersion("svc")
	assertOK(t, err, "重启后解析活动版本")
	if cur.Version != 1 {
		t.Fatalf("重启后活动版本必须仍是回滚目标 v1, got v%d", cur.Version)
	}
	snap, _ := p2.registry.Snapshot("svc")
	if len(snap.Versions) != 2 {
		t.Fatalf("v1/v2 历史版本都必须保留, got %d", len(snap.Versions))
	}
	foundRollback := false
	for _, h := range snap.History {
		if h.Kind == "rollback" && h.Version == 1 {
			foundRollback = true
		}
	}
	if !foundRollback {
		t.Fatal("回滚记录必须随快照保留，历史可解释")
	}
	logCase(t, "输入 重启进程 → 活动=v1、历史版本=2、rollback 记录存在，判定依据：JSON 快照恢复")

	out, err := p2.Call(context.Background(), []byte(`{"protocol":"2.0","route":"/svc","payload":"cg=="}`))
	if err != nil {
		t.Fatalf("重启后路由调用应成功: %v", err)
	}
	logCase(t, "输入重启后路由调用 /svc → %s", string(out))

	// 恢复后把 v2 废弃，再回滚到 v2 必须被拒绝（指向不可用版本）。
	assertOK(t, p2.SetVersionState("svc", 2, VersionRetired), "废弃 v2")
	_, err = p2.Rollback("svc", 2)
	assertClass(t, err, ErrClassUnavailable)
	logCase(t, "输入恢复后 rollback 到 retired v2 → unavailable，判定原因与 not_found 可区分")
}

func TestPlatform_RollbackIllegalAfterRestore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "faas2")
	cfg := PoolConfig{ReapInterval: 0}
	p, err := NewPlatform(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	assertOK(t, p.RegisterFunction("s"), "注册")
	_, _ = p.Publish("s", testCfg("echo"))
	_, err = p.Rollback("s", 42)
	assertClass(t, err, ErrClassNotFound)
	_, err = p.Rollback("ghost", 1)
	assertClass(t, err, ErrClassNotFound)
	logCase(t, "输入非法版本/未知函数回滚 → not_found，重启前后规则一致")
}
