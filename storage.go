package faas

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Store 以本地 JSON 快照模拟平台持久化：函数元数据 / 版本历史 / 路由。
// 写入采用"临时文件 + 原子改名"，保证并发与崩溃下不会出现半更新快照。
type Store struct {
	dir string
}

type persistedState struct {
	Functions []*Function `json:"functions"`
	Routes    []Route     `json:"routes"`
}

// NewStore 在目录 dir 下创建本地存储（目录不存在则创建）。
func NewStore(dir string) (*Store, error) {
	if dir == "" {
		return nil, newError(ErrClassInvalid, "store directory must not be empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, wrapError(ErrClassPlatform, "cannot create store directory", err)
	}
	return &Store{dir: dir}, nil
}

// snapshotPath 返回快照文件路径。
func (s *Store) snapshotPath() string {
	return filepath.Join(s.dir, "faas-state.json")
}

// Save 原子写入函数与路由快照。
func (s *Store) Save(funcs []*Function, routes []Route) error {
	state := persistedState{Functions: funcs, Routes: routes}
	if err := writeJSONAtomic(s.snapshotPath(), &state); err != nil {
		return wrapError(ErrClassPlatform, "failed persisting platform snapshot", err)
	}
	return nil
}

// Load 读取快照；文件不存在时返回空状态与 exists=false。
func (s *Store) Load() (persistedState, bool, error) {
	raw, err := os.ReadFile(s.snapshotPath())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return persistedState{}, false, nil
		}
		return persistedState{}, false, wrapError(ErrClassPlatform, "failed reading snapshot", err)
	}
	var state persistedState
	if err := json.Unmarshal(raw, &state); err != nil {
		return persistedState{}, false, wrapError(ErrClassPlatform, "corrupt snapshot", err)
	}
	if state.Functions == nil {
		state.Functions = []*Function{}
	}
	if state.Routes == nil {
		state.Routes = []Route{}
	}
	return state, true, nil
}

// writeJSONAtomic 先写同目录临时文件再 rename，杜绝半更新文件。
// 0o600 权限与 0o644 的最终文件通过 chmod 对齐。
func writeJSONAtomic(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".faas-state-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // rename 成功后删除不存在的临时文件是无副作用的
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
