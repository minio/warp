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
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/wui"
)

// Service is the control-plane HTTP server: it exposes CRUD for scenarios,
// targets and the client pool, launches runs, and serves the web UI.
type Service struct {
	store   *Store
	exec    Executor
	dataDir string
	runsDir string
	runMu   sync.Mutex // serializes runs; only one benchmark may run at a time

	resultMu    sync.Mutex
	resultCache map[string]*aggregate.Realtime // run ID -> parsed result (immutable once done)
}

// NewService creates a control-plane service backed by store, using exec to run
// benchmarks. dataDir holds per-run artifacts.
func NewService(store *Store, exec Executor, dataDir string) *Service {
	return &Service{
		store:       store,
		exec:        exec,
		dataDir:     dataDir,
		runsDir:     filepath.Join(dataDir, "runs"),
		resultCache: map[string]*aggregate.Realtime{},
	}
}

// Handler returns the HTTP handler for the service.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/scenarios", s.listScenarios)
	mux.HandleFunc("POST /api/scenarios", s.createScenario)
	mux.HandleFunc("GET /api/scenarios/{id}", s.getScenario)
	mux.HandleFunc("PUT /api/scenarios/{id}", s.updateScenario)
	mux.HandleFunc("DELETE /api/scenarios/{id}", s.deleteScenario)
	mux.HandleFunc("POST /api/scenarios/{id}/run", s.runScenario)

	mux.HandleFunc("GET /api/targets", s.listTargets)
	mux.HandleFunc("POST /api/targets", s.createTarget)
	mux.HandleFunc("DELETE /api/targets/{id}", s.deleteTarget)
	mux.HandleFunc("POST /api/targets/{id}/check", s.checkTarget)

	mux.HandleFunc("GET /api/clients", s.listClients)
	mux.HandleFunc("POST /api/clients", s.createClient)
	mux.HandleFunc("DELETE /api/clients/{id}", s.deleteClient)
	mux.HandleFunc("POST /api/clients/{id}/check", s.checkClient)

	mux.HandleFunc("GET /api/runs", s.listRuns)
	mux.HandleFunc("GET /api/runs/{id}", s.getRun)

	// Reuse the wui dashboard + compare views, served through the control plane's
	// own port (so they work behind a reverse proxy). The pages fetch their data
	// from run-scoped endpoints below; more specific patterns win over /dash/.
	mux.HandleFunc("GET /dash/api/data", s.dashData)
	mux.HandleFunc("GET /dash/api/compare", s.dashCompare)
	mux.Handle("/dash/", http.StripPrefix("/dash/", http.FileServerFS(wui.StaticFS())))

	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic("control: static assets: " + err.Error())
	}
	mux.Handle("/", http.FileServerFS(staticFS))
	return recoverMiddleware(mux)
}

// recoverMiddleware turns a panic in any handler into a 500 response instead of
// dropping the connection (which surfaces in the browser as "Failed to fetch").
func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("control: panic serving %s: %v\n%s", r.URL.Path, rec, debug.Stack())
				writeErr(w, http.StatusInternalServerError, fmt.Sprintf("internal error: %v", rec))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// --- JSON helpers ---

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}

// --- Scenarios ---

func (s *Service) listScenarios(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.store.ListScenarios())
}

func (s *Service) createScenario(w http.ResponseWriter, r *http.Request) {
	var sc Scenario
	if err := readJSON(r, &sc); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	sc.ID = "" // force create
	if err := s.store.SaveScenario(&sc); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, &sc)
}

func (s *Service) getScenario(w http.ResponseWriter, r *http.Request) {
	sc, ok := s.store.GetScenario(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, "scenario not found")
		return
	}
	writeJSON(w, http.StatusOK, sc)
}

func (s *Service) updateScenario(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	existing, ok := s.store.GetScenario(id)
	if !ok {
		writeErr(w, http.StatusNotFound, "scenario not found")
		return
	}
	var sc Scenario
	if err := readJSON(r, &sc); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	sc.ID = id
	sc.CreatedAt = existing.CreatedAt
	if err := s.store.SaveScenario(&sc); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, &sc)
}

func (s *Service) deleteScenario(w http.ResponseWriter, r *http.Request) {
	if err := s.store.DeleteScenario(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- Targets ---

func (s *Service) listTargets(w http.ResponseWriter, _ *http.Request) {
	// Redact secrets in the listing.
	targets := s.store.ListTargets()
	out := make([]Target, len(targets))
	for i, t := range targets {
		out[i] = *t
		out[i].SecretKey = ""
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Service) createTarget(w http.ResponseWriter, r *http.Request) {
	var t Target
	if err := readJSON(r, &t); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	t.ID = ""
	if err := s.store.SaveTarget(&t); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Respond with a redacted copy; never mutate the stored struct (the store
	// holds this pointer, so blanking it here would wipe the in-memory secret).
	resp := t
	resp.SecretKey = ""
	writeJSON(w, http.StatusCreated, &resp)
}

func (s *Service) deleteTarget(w http.ResponseWriter, r *http.Request) {
	if err := s.store.DeleteTarget(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// checkTarget validates the target's S3 connection: endpoint, credentials and
// bucket accessibility.
func (s *Service) checkTarget(w http.ResponseWriter, r *http.Request) {
	t, ok := s.store.GetTarget(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, "target not found")
		return
	}
	err := checkTarget(r.Context(), t)
	t.Status = "reachable"
	if err != nil {
		t.Status = "unreachable"
	}
	t.LastChecked = time.Now()
	_ = s.store.SaveTarget(t)

	resp := map[string]any{"ok": err == nil, "status": t.Status}
	if err != nil {
		resp["error"] = err.Error()
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- Clients ---

func (s *Service) listClients(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.store.ListClients())
}

func (s *Service) createClient(w http.ResponseWriter, r *http.Request) {
	var c Client
	if err := readJSON(r, &c); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	c.ID = ""
	c.Status = "unknown"
	if err := s.store.SaveClient(&c); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, &c)
}

func (s *Service) deleteClient(w http.ResponseWriter, r *http.Request) {
	if err := s.store.DeleteClient(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// checkClient probes a pool client for reachability. A full implementation would
// speak the warp websocket protocol and capture the client version; a TCP dial is
// enough to tell reachable from unreachable for now.
//
// TODO(milestone-2): perform a websocket handshake and record the client version,
// flagging server/client version mismatches before a run is allowed.
func (s *Service) checkClient(w http.ResponseWriter, r *http.Request) {
	c, ok := s.store.GetClient(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, "client not found")
		return
	}
	c.Status = "unreachable"
	conn, err := net.DialTimeout("tcp", c.Address, 3*time.Second)
	if err == nil {
		_ = conn.Close()
		c.Status = "reachable"
		c.LastSeen = time.Now()
	}
	if err := s.store.SaveClient(c); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, c)
}

// --- Runs ---

func (s *Service) listRuns(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.store.ListRuns())
}

func (s *Service) getRun(w http.ResponseWriter, r *http.Request) {
	run, ok := s.store.GetRun(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, "run not found")
		return
	}
	writeJSON(w, http.StatusOK, run)
}

// viewRun loads a finished run's result and serves it in a wui dashboard,
// returning the URL to open. The dashboard is reused if already running.
// resultFor loads and caches a run's parsed result. Results are immutable once a
// run finishes, so caching by run ID is safe.
func (s *Service) resultFor(runID string) (*aggregate.Realtime, error) {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	if rt, ok := s.resultCache[runID]; ok {
		return rt, nil
	}
	run, ok := s.store.GetRun(runID)
	if !ok {
		return nil, errClientNotFound(runID) // generic "not found"
	}
	if run.ResultFile == "" {
		return nil, errNoResult
	}
	rt, err := loadResult(run.ResultFile)
	if err != nil {
		return nil, err
	}
	s.applyClientNames(rt) // show friendly client names instead of addresses
	s.resultCache[runID] = rt
	return rt, nil
}

// dashData serves a single run's result in the wui dashboard's expected envelope.
func (s *Service) dashData(w http.ResponseWriter, r *http.Request) {
	rt, err := s.resultFor(r.URL.Query().Get("run"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"auto_update": false, "data": rt})
}

// dashCompare serves a before/after comparison of two runs for the compare view.
func (s *Service) dashCompare(w http.ResponseWriter, r *http.Request) {
	beforeID := r.URL.Query().Get("before")
	afterID := r.URL.Query().Get("after")
	before, err := s.resultFor(beforeID)
	if err != nil {
		writeErr(w, http.StatusNotFound, "before: "+err.Error())
		return
	}
	after, err := s.resultFor(afterID)
	if err != nil {
		writeErr(w, http.StatusNotFound, "after: "+err.Error())
		return
	}
	data := wui.BuildCompareData(before, after, s.runLabel(beforeID), s.runLabel(afterID), "")
	writeJSON(w, http.StatusOK, data)
}

// runLabel produces a human label for a run used in the compare view.
func (s *Service) runLabel(runID string) string {
	if run, ok := s.store.GetRun(runID); ok {
		// testname-target-time, e.g. "smoke-arvan-2026-06-13_10-04-05"
		return fmt.Sprintf("%s-%s-%s", run.ScenarioName, run.TargetName, run.StartedAt.Format("2006-01-02_15-04-05"))
	}
	return runID
}

type runRequest struct {
	TargetID  string   `json:"target_id"`
	ClientIDs []string `json:"client_ids"`
}

func (s *Service) runScenario(w http.ResponseWriter, r *http.Request) {
	sc, ok := s.store.GetScenario(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, "scenario not found")
		return
	}
	var req runRequest
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	target, ok := s.store.GetTarget(req.TargetID)
	if !ok {
		writeErr(w, http.StatusBadRequest, "target not found")
		return
	}
	addrs, err := s.resolveClients(req.ClientIDs)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	// Only one benchmark may run at a time: benchmarks saturate the target, so
	// concurrent runs would corrupt each other's numbers.
	if !s.runMu.TryLock() {
		writeErr(w, http.StatusConflict, "another run is already in progress")
		return
	}

	run := &Run{
		ScenarioID:   sc.ID,
		ScenarioName: sc.Name,
		TargetID:     target.ID,
		TargetName:   target.Name,
		ClientAddrs:  addrs,
		Spec:         sc.Spec, // snapshot
		Status:       RunRunning,
		StartedAt:    time.Now(),
	}
	if err := s.store.SaveRun(run); err != nil {
		s.runMu.Unlock()
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	go s.executeRun(run, target)
	writeJSON(w, http.StatusAccepted, run)
}

// resolveClients turns client IDs into addresses; an empty list means all clients.
func (s *Service) resolveClients(ids []string) ([]string, error) {
	if len(ids) == 0 {
		var addrs []string
		for _, c := range s.store.ListClients() {
			addrs = append(addrs, c.Address)
		}
		if len(addrs) == 0 {
			return nil, errNoClients
		}
		return addrs, nil
	}
	var addrs []string
	for _, id := range ids {
		c, ok := s.store.GetClient(id)
		if !ok {
			return nil, errClientNotFound(id)
		}
		addrs = append(addrs, c.Address)
	}
	return addrs, nil
}

// executeRun runs a benchmark to completion and records the outcome.
func (s *Service) executeRun(run *Run, target *Target) {
	defer s.runMu.Unlock()

	runDir := filepath.Join(s.runsDir, run.ID)
	if err := mkdir(runDir); err != nil {
		s.finishRun(run, RunFailed, "", err.Error())
		return
	}

	ctx := context.Background()
	resultFile, err := s.exec.Run(ctx, run, target, runDir)
	if err != nil {
		s.finishRun(run, RunFailed, "", err.Error())
		return
	}
	s.finishRun(run, RunDone, resultFile, "")
}

func (s *Service) finishRun(run *Run, status RunStatus, resultFile, errMsg string) {
	run.Status = status
	run.EndedAt = time.Now()
	run.ResultFile = resultFile
	run.Error = errMsg
	_ = s.store.SaveRun(run)
}
