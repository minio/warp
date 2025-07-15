// pkg/state/state.go
package state

import (
	"sync"
	"time"
)

type StateManager struct {
	ttl time.Duration
	mu  sync.RWMutex
	m   map[string]entry
}

type entry struct {
	target string
	expire time.Time
}

func New(ttl time.Duration) *StateManager {
	return &StateManager{
		ttl: ttl,
		m:   make(map[string]entry),
	}
}

// LookupOrSet returns the cached target for objectID if still valid,
// otherwise stores `target` and returns it.
func (s *StateManager) LookupOrSet(objID, target string) string {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.m[objID]; ok && e.expire.After(now) {
		return e.target
	}
	s.m[objID] = entry{target: target, expire: now.Add(s.ttl)}
	return target
}

