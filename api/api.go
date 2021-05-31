/*
 * Warp (C) 2019-2020 MinIO, Inc.
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

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"
)

// BenchmarkStatus contains information when a benchmark is running.
type BenchmarkStatus = struct {
	// Text string describing the state of the benchmark run.
	// Updated continuously.
	LastStatus string `json:"last_status"`

	// Any non-fatal error during the run.
	Error string `json:"error"`

	// Will be true when benchmark has finished and data is ready.
	DataReady bool `json:"data_ready"`

	// Base filename of the
	Filename string `json:"filename,omitempty"`
}

// Operations contains raw benchmark operations.
// Usually very verbose.
type Operations struct {
	Operations bench.Operations `json:"operations"`
}

// Server contains the state of the running server.
type Server struct {
	status  BenchmarkStatus
	ops     bench.Operations
	agrr    *aggregate.Aggregated
	aggrDur time.Duration
	server  *http.Server
	cmdLine string

	// Shutting down
	ctx    context.Context
	cancel context.CancelFunc

	// lock for Server
	mu sync.Mutex
	// Parent loggers
	infoln  func(data ...interface{})
	errorln func(data ...interface{})
}

// OperationsReady can be used to send benchmark data to the server.
func (s *Server) OperationsReady(ops bench.Operations, filename, cmdLine string) {
	s.mu.Lock()
	s.status.DataReady = ops != nil
	s.ops = ops
	s.status.Filename = filename
	s.cmdLine = cmdLine
	s.mu.Unlock()
}

// SetLnLoggers can be used to set upstream loggers.
// When logging to the servers these will be called.
func (s *Server) SetLnLoggers(info, err func(data ...interface{})) {
	s.mu.Lock()
	s.infoln = info
	s.errorln = err
	s.mu.Unlock()
}

// Done can be called to block until a server is closed.
// If no server is started it will return at once.
func (s *Server) Done() {
	if s.server == nil {
		return
	}
	// Wait until killed.
	<-s.ctx.Done()
}

// InfoLn allows to log data to the server.
// The server will update its status and send message upstream if set.
func (s *Server) InfoLn(data ...interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.infoln != nil {
		s.infoln(data...)
	}
	s.status.LastStatus = strings.TrimSpace(fmt.Sprint(data...))
}

// InfoQuietln can be used to log data to the internal status only
// and not forward it to the upstream logger.
func (s *Server) InfoQuietln(data ...interface{}) {
	s.mu.Lock()
	s.status.LastStatus = strings.TrimSpace(fmt.Sprintln(data...))
	s.mu.Unlock()
}

// Errorln allows to store a non-fatal error.
func (s *Server) Errorln(data ...interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.errorln != nil {
		s.errorln(data...)
	}
	s.status.Error = strings.TrimSpace(fmt.Sprintln(data...))
}

// handleStatus handles GET `/v1/status` requests.
func (s *Server) handleStatus(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	st := s.status
	s.mu.Unlock()
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(b)
}

// handleAggregated handles GET `/v1/aggregated` requests with optional "segment" parameter.
func (s *Server) handleAggregated(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	segmentParam, ok := req.URL.Query()["segment"]
	if !ok || len(segmentParam) == 0 {
		segmentParam = []string{"1s"}
	}
	segmentDur, err := time.ParseDuration(segmentParam[0])
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	durFn := func(total time.Duration) time.Duration {
		return segmentDur
	}
	s.mu.Lock()
	if s.ops == nil {
		s.mu.Unlock()
		w.WriteHeader(404)
		return
	}
	if s.agrr == nil || s.aggrDur != segmentDur {
		aggr := aggregate.Aggregate(s.ops, aggregate.Options{
			DurFunc: durFn,
			SkipDur: 0,
		})
		s.agrr = &aggr
		s.aggrDur = segmentDur
	}
	// Copy
	aggregated := *s.agrr
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	b, err := json.MarshalIndent(aggregated, "", "  ")
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(b)
}

// handleDownloadZst handles GET `/v1/operations` requests and returns the operations
// as an archive that can be used by warp.
// If no data is present "No Content" status will be returned.
func (s *Server) handleDownloadZst(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	ops := s.ops
	fn := s.status.Filename
	s.mu.Unlock()
	if len(ops) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.csv.zst"`, fn))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(200)

	enc, err := zstd.NewWriter(w)
	if err != nil {
		s.Errorln(err)
		return
	}
	defer enc.Close()

	err = ops.CSV(enc, s.cmdLine)
	if err != nil {
		s.Errorln(err)
		return
	}
}

// handleDownloadJSON handles GET `/v1/operations` requests and returns the operations as JSON.
func (s *Server) handleDownloadJSON(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	ops := s.ops
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	enc := json.NewEncoder(w)
	enc.Encode(ops)
}

// handleStop handles requests to `/v1/stop`, stops the service.
func (s *Server) handleStop(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodDelete {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Write([]byte(`bye...`))
	s.server.Close()
}

// NewBenchmarkMonitor creates a new Server.
func NewBenchmarkMonitor(listenAddr string) *Server {
	s := &Server{}
	if listenAddr == "" {
		return s
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stop", s.handleStop)
	mux.HandleFunc("/v1/status", s.handleStatus)
	mux.HandleFunc("/v1/aggregated", s.handleAggregated)
	mux.HandleFunc("/v1/operations/json", s.handleDownloadJSON)
	mux.HandleFunc("/v1/operations", s.handleDownloadZst)

	s.server = &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		TLSConfig:         nil,
		ReadTimeout:       time.Minute,
		ReadHeaderTimeout: time.Second,
		WriteTimeout:      time.Minute,
		IdleTimeout:       time.Minute,
		MaxHeaderBytes:    0,
		TLSNextProto:      nil,
		ConnState:         nil,
		ErrorLog:          nil,
		BaseContext:       nil,
		ConnContext:       nil,
	}
	go func() {
		defer s.cancel()
		console.Infoln("opening server on", listenAddr)
		s.Errorln(s.server.ListenAndServe())
	}()
	return s
}
