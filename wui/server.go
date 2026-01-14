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

package wui

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/minio/warp/pkg/aggregate"
)

// Server is a web UI server for displaying benchmark results.
type Server struct {
	data     atomic.Pointer[aggregate.Realtime]
	poll     atomic.Pointer[chan<- aggregate.UpdateReq]
	server   *http.Server
	listener net.Listener
	addr     string
}

// New creates a new web UI server with the given benchmark data.
func New(data *aggregate.Realtime) *Server {
	s := Server{}
	s.data.Store(data)
	return &s
}

// Update the data.
func (s *Server) Update(data *aggregate.Realtime) {
	if data != nil {
		s.data.Store(data)
	}
}

// WithPoll the data will register a pollers
func (s *Server) WithPoll(updates chan<- aggregate.UpdateReq) {
	s.poll.Store(&updates)
}

// Start starts the web server on an automatically assigned port.
// Returns the address the server is listening on.
func (s *Server) Start() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("failed to start listener: %w", err)
	}
	s.listener = listener
	s.addr = listener.Addr().String()

	mux := http.NewServeMux()
	s.registerHandlers(mux)

	s.server = &http.Server{
		Handler:           mux,
		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		if err := s.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "Web server error: %v\n", err)
		}
	}()

	return "http://" + s.addr, nil
}

// Address returns the address the server is listening on.
func (s *Server) Address() string {
	return "http://" + s.addr
}

// OpenBrowser attempts to open the default browser to the server address.
func (s *Server) OpenBrowser() error {
	url := s.Address()
	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default: // linux, freebsd, etc.
		cmd = exec.Command("xdg-open", url)
	}

	return cmd.Start()
}

// WaitForKeypress blocks until the user presses Enter.
func (s *Server) WaitForKeypress() {
	reader := bufio.NewReader(os.Stdin)
	_, _ = reader.ReadString('\n')
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown() {
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.server.Shutdown(ctx)
	}
}
