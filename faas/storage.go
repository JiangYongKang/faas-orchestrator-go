package faas

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type EventKind string

const (
	EventPublished EventKind = "published"
	EventRollback  EventKind = "rollback"
	EventInvoke    EventKind = "invoke"
)

type StoredEvent struct {
	Kind     EventKind      `json:"kind"`
	Function string         `json:"function,omitempty"`
	Version  VersionNumber  `json:"version,omitempty"`
	Key      string         `json:"idempotency_key,omitempty"`
	Status   string         `json:"status,omitempty"`
	Detail   map[string]any `json:"detail,omitempty"`
}

type EventStore interface {
	Append(StoredEvent) error
}

// LocalStore serializes events to a local JSON Lines file. fsync is used for
// every append so publication and terminal invocation records are observable
// after normal process exits in local development.
type LocalStore struct {
	mu sync.Mutex
	f  *os.File
}

func OpenLocalStore(path string) (*LocalStore, error) {
	if path == "" {
		return &LocalStore{}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, platformError("create event store directory: %v", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, platformError("open event store: %v", err)
	}
	return &LocalStore{f: f}, nil
}

func (s *LocalStore) Append(event StoredEvent) error {
	if s == nil || s.f == nil {
		return nil
	}
	data, err := json.Marshal(event)
	if err != nil {
		return platformError("marshal event: %v", err)
	}
	data = append(data, '\n')
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.f.Write(data); err != nil {
		return platformError("write event: %v", err)
	}
	if err := s.f.Sync(); err != nil {
		return platformError("sync event: %v", err)
	}
	return nil
}

func (s *LocalStore) Close() error {
	if s == nil || s.f == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.f.Close()
	s.f = nil
	return err
}
