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

	// Serve index.html for root path directly to avoid redirect loop
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		content, err := fs.ReadFile(staticFS, "index.html")
		if err != nil {
			http.Error(w, "index.html not found", http.StatusNotFound)
			return
		}
		w.Write(content)
	})

	// Serve static files (css, js)
	mux.HandleFunc("/style.css", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		content, _ := fs.ReadFile(staticFS, "style.css")
		w.Write(content)
	})

	mux.HandleFunc("/app.js", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		content, _ := fs.ReadFile(staticFS, "app.js")
		w.Write(content)
	})
}

// handleData returns the benchmark data as JSON.
func (s *Server) handleData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(s.data); err != nil {
		http.Error(w, "Failed to encode data: "+err.Error(), http.StatusInternalServerError)
		return
	}
}
