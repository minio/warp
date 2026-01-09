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
	"encoding/json"
	"io/fs"
	"net/http"

	"github.com/minio/warp/pkg/aggregate"
)

// registerHandlers sets up all HTTP routes.
func (s *Server) registerHandlers(mux *http.ServeMux) {
	// Serve static files
	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic("failed to create static filesystem: " + err.Error())
	}

	// API endpoint for benchmark data
	mux.HandleFunc("/api/data", s.handleData)

	// Serve static files
	mux.Handle("/", http.FileServerFS(staticFS))
}

// apiResponse wraps the benchmark data with metadata.
type apiResponse struct {
	AutoUpdate bool                `json:"auto_update"`
	Data       *aggregate.Realtime `json:"data"`
}

// handleData returns the benchmark data as JSON.
func (s *Server) handleData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	data := s.data.Load()
	autoUpdate := false
	if poll := s.poll.Load(); poll != nil {
		ch := *poll
		into := make(chan *aggregate.Realtime, 1)
		ch <- aggregate.UpdateReq{C: into}
		select {
		case <-r.Context().Done():
			return
		case data = <-into:
		}
		if data == nil {
			data = &aggregate.Realtime{}
		}
		autoUpdate = !data.Final
	}

	resp := apiResponse{
		AutoUpdate: autoUpdate,
		Data:       data,
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(resp); err != nil {
		http.Error(w, "Failed to encode data: "+err.Error(), http.StatusInternalServerError)
		return
	}
}
