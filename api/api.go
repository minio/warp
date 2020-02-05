package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

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
	Type string `json:"type"`
}

type Server struct {
	status BenchmarkStatus
	ops    bench.Operations

	// lock for Server
	mu sync.Mutex
	// Parent loggers
	infoln  func(data ...interface{})
	errorln func(data ...interface{})
}

func (s *Server) OperationsReady(ops bench.Operations) {
	s.mu.Lock()
	s.status.DataReady = ops != nil
	s.ops = ops
	s.mu.Unlock()
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

func (s *Server) handleStatusfunc(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	st := s.status
	s.mu.Unlock()
	b, err := json.Marshal(st)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(b)
}

func (s *Server) handleDowloadAll(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	b, err := json.Marshal(struct {
	}{})
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(b)
}

func newBenchmarkMonitor(listenAddr string) *Server {
	s := &Server{}
	if listenAddr == "" {
		return s
	}

	http.HandleFunc("/status", s.handleStatusfunc)
	http.HandleFunc("/download_all", s.handleDowloadAll)
	go func() { http.ListenAndServe(listenAddr, nil) }()
	return s
}
