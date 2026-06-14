/*
 * Warp (C) 2019-2026 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package control

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Store persists the control-plane state. This file-backed implementation keeps
// everything in a single JSON document, which is plenty for a single-user, local
// coordinator. The Store API is small enough to swap for SQLite later without
// touching callers.
type Store struct {
	mu   sync.RWMutex
	path string
	data storeData
}

type storeData struct {
	Scenarios map[string]*Scenario `json:"scenarios"`
	Targets   map[string]*Target   `json:"targets"`
	Clients   map[string]*Client   `json:"clients"`
	Runs      map[string]*Run      `json:"runs"`
}

// NewStore opens (or creates) a store backed by file at path.
func NewStore(path string) (*Store, error) {
	s := &Store{
		path: path,
		data: storeData{
			Scenarios: map[string]*Scenario{},
			Targets:   map[string]*Target{},
			Clients:   map[string]*Client{},
			Runs:      map[string]*Run{},
		},
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, s.flush()
		}
		return nil, err
	}
	if err := json.Unmarshal(buf, &s.data); err != nil {
		return nil, fmt.Errorf("reading store %q: %w", path, err)
	}
	return s, nil
}

// flush writes the in-memory state to disk. Callers must hold the write lock.
func (s *Store) flush() error {
	buf, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// --- Scenarios ---

func (s *Store) ListScenarios() []*Scenario {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Scenario, 0, len(s.data.Scenarios))
	for _, v := range s.data.Scenarios {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *Store) GetScenario(id string) (*Scenario, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data.Scenarios[id]
	return v, ok
}

func (s *Store) SaveScenario(sc *Scenario) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if sc.ID == "" {
		sc.ID = newID()
		sc.CreatedAt = now
	}
	sc.UpdatedAt = now
	s.data.Scenarios[sc.ID] = sc
	return s.flush()
}

func (s *Store) DeleteScenario(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data.Scenarios, id)
	return s.flush()
}

// --- Targets ---

func (s *Store) ListTargets() []*Target {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Target, 0, len(s.data.Targets))
	for _, v := range s.data.Targets {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *Store) GetTarget(id string) (*Target, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data.Targets[id]
	return v, ok
}

func (s *Store) SaveTarget(t *Target) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.ID == "" {
		t.ID = newID()
		t.CreatedAt = time.Now()
	}
	s.data.Targets[t.ID] = t
	return s.flush()
}

func (s *Store) DeleteTarget(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data.Targets, id)
	return s.flush()
}

// --- Clients ---

func (s *Store) ListClients() []*Client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Client, 0, len(s.data.Clients))
	for _, v := range s.data.Clients {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *Store) GetClient(id string) (*Client, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data.Clients[id]
	return v, ok
}

func (s *Store) SaveClient(c *Client) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.ID == "" {
		c.ID = newID()
		c.CreatedAt = time.Now()
	}
	s.data.Clients[c.ID] = c
	return s.flush()
}

func (s *Store) DeleteClient(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data.Clients, id)
	return s.flush()
}

// --- Runs ---

func (s *Store) ListRuns() []*Run {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Run, 0, len(s.data.Runs))
	for _, v := range s.data.Runs {
		out = append(out, v)
	}
	// Newest first.
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return out
}

func (s *Store) GetRun(id string) (*Run, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data.Runs[id]
	return v, ok
}

func (s *Store) SaveRun(r *Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.ID == "" {
		r.ID = newID()
	}
	s.data.Runs[r.ID] = r
	return s.flush()
}
