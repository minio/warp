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

// Package control implements the warp benchmark control plane: a long-lived
// coordinator that stores benchmark scenarios, manages a pool of load-generating
// clients, runs scenarios across them in distributed mode, and serves a web UI.
package control

import "time"

// ScenarioSpec is the parameter set that defines a benchmark. It maps directly
// onto warp's command + flags, so the same definition the CLI understands drives
// distributed runs without a second config format.
type ScenarioSpec struct {
	// Method is the warp benchmark command: get, put, mixed, delete, stat, list.
	Method string `json:"method"`
	// ObjSize is the object size, e.g. "256KiB", "10MiB".
	ObjSize string `json:"obj_size"`
	// Objects to pre-create (used by get/stat/delete/list).
	Objects int `json:"objects"`
	// Duration of the benchmark stage, e.g. "30s", "5m".
	Duration string `json:"duration"`
	// Concurrent operation threads per client.
	Concurrent int `json:"concurrent"`

	// Mixed operation distribution (only used when Method == "mixed").
	GetDistrib    int `json:"get_distrib,omitempty"`
	PutDistrib    int `json:"put_distrib,omitempty"`
	StatDistrib   int `json:"stat_distrib,omitempty"`
	DeleteDistrib int `json:"delete_distrib,omitempty"`

	// ExtraFlags are passed through verbatim as --key value to warp.
	ExtraFlags map[string]string `json:"extra_flags,omitempty"`
}

// Scenario is a named, reusable benchmark definition. It holds no credentials so
// it stays portable.
type Scenario struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	Spec        ScenarioSpec `json:"spec"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
}

// Target is an S3 endpoint plus credentials. Kept separate from scenarios so
// secrets never live in a portable scenario definition.
type Target struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Endpoint  string    `json:"endpoint"` // host[:port], no scheme
	Region    string    `json:"region,omitempty"`
	TLS       bool      `json:"tls"`
	Bucket    string    `json:"bucket"`
	AccessKey string    `json:"access_key"`
	SecretKey string    `json:"secret_key"`
	CreatedAt time.Time `json:"created_at"`

	// Result of the most recent connectivity check (runtime/diagnostic).
	Status      string    `json:"status,omitempty"` // reachable | unreachable | unknown
	LastChecked time.Time `json:"last_checked,omitempty"`
}

// Client is an entry in the load-generator pool. Address points at a running
// `warp client` listener. Status fields are runtime-only (not persisted).
type Client struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Address   string    `json:"address"` // host:port
	CreatedAt time.Time `json:"created_at"`

	Status   string    `json:"status,omitempty"` // reachable | unreachable | unknown
	LastSeen time.Time `json:"last_seen,omitempty"`
}

// RunStatus is the lifecycle state of a run.
type RunStatus string

const (
	RunQueued   RunStatus = "queued"
	RunRunning  RunStatus = "running"
	RunDone     RunStatus = "done"
	RunFailed   RunStatus = "failed"
	RunAborted  RunStatus = "aborted"
	RunDegraded RunStatus = "degraded" // finished, but a client dropped mid-run
)

// Run is a single execution of a scenario against a target on a set of clients.
// It snapshots the spec/target/client set so history stays honest if the source
// scenario is later edited.
type Run struct {
	ID           string       `json:"id"`
	ScenarioID   string       `json:"scenario_id"`
	ScenarioName string       `json:"scenario_name"`
	TargetID     string       `json:"target_id"`
	TargetName   string       `json:"target_name"`
	ClientAddrs  []string     `json:"client_addrs"`
	Spec         ScenarioSpec `json:"spec"` // snapshot at launch
	Status       RunStatus    `json:"status"`
	StartedAt    time.Time    `json:"started_at"`
	EndedAt      time.Time    `json:"ended_at,omitempty"`
	ResultFile   string       `json:"result_file,omitempty"`
	LogFile      string       `json:"log_file,omitempty"`
	Error        string       `json:"error,omitempty"`
}
