package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"
)

type BenchmarkStatus = struct {
	LastStatus string `json:"last_status"`
	Error      string `json:"error"`
	DataReady  bool   `json:"data_ready"`
}

type Operations struct {
	Operations bench.Operations `json:"operations"`
}

type Aggregated struct {
	Type       string                `json:"type"`
	Single     []aggregate.Operation `json:"single,omitempty"`
	Mixed      []aggregate.Operation `json:"mixed,omitempty"`
	segmentDur time.Duration
}

type Server struct {
	status   BenchmarkStatus
	ops      bench.Operations
	filename string
	agrr     *Aggregated
	server   *http.Server

	// Shutting down
	ctx    context.Context
	cancel context.CancelFunc

	// lock for Server
	mu sync.Mutex
	// Parent loggers
	infoln  func(data ...interface{})
	errorln func(data ...interface{})
}

func (s *Server) OperationsReady(ops bench.Operations, filename string) {
	s.mu.Lock()
	s.status.DataReady = ops != nil
	s.ops = ops
	s.filename = filename
	s.mu.Unlock()
}

func (s *Server) SetLnLoggers(info, err func(data ...interface{})) {
	s.mu.Lock()
	s.infoln = info
	s.errorln = err
	s.mu.Unlock()
}

func (s *Server) Done() {
	if s.server == nil {
		return
	}
	// Wait until killed.
	<-s.ctx.Done()
}

func (s *Server) InfoLn(data ...interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.infoln != nil {
		s.infoln(data...)
	}
	s.status.LastStatus = fmt.Sprintln(data...)
}

func (s *Server) ErrorLn(data ...interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.errorln != nil {
		s.errorln(data...)
	}
	s.status.Error = fmt.Sprintln(data...)
}

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

	s.mu.Lock()
	if s.agrr == nil || s.agrr.segmentDur != segmentDur {
		if len(s.ops) > 0 {
			aggr := Aggregated{}
			if s.ops.IsMultiOp() {
				aggr.Type = "mixed"
			} else {
				aggr.Type = "single"
				aggr.Single = aggregate.SingleOp(s.ops, segmentDur, 0)
			}
			s.agrr = &aggr
		}
	}
	aggregated := s.agrr
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

func (s *Server) handleDownloadZst(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	ops := s.ops
	fn := s.filename
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
		s.ErrorLn(err)
		return
	}
	defer enc.Close()

	err = ops.CSV(enc)
	if err != nil {
		s.ErrorLn(err)
		return
	}
}

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

func (s *Server) handleRootApi(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodDelete {
		w.WriteHeader(200)
		w.Write([]byte(`bye...`))
		s.server.Close()
		return
	}
	if req.Method == http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write([]byte(`{"status": "ok"}`))
		return
	}
	w.WriteHeader(http.StatusBadRequest)
}

func NewBenchmarkMonitor(listenAddr string) *Server {
	s := &Server{}
	if listenAddr == "" {
		return s
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", s.handleStatus)
	mux.HandleFunc("/v1", s.handleRootApi)
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
		s.ErrorLn(s.server.ListenAndServe())
	}()
	return s
}
