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

package cli

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"
)

// TestServeUpdatesAfterStage checks that client aggregates collected during a stage
// are still served after it ends. The UI blocked forever when they were not (#498).
func TestServeUpdatesAfterStage(t *testing.T) {
	// Fake warp client reporting the stage as finished with 5 requests.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		var si serverInfo
		if ws.ReadJSON(&si) != nil || ws.WriteJSON(clientReply{Time: time.Now()}) != nil {
			return
		}
		for {
			var req serverRequest
			if ws.ReadJSON(&req) != nil {
				return
			}
			resp := clientReply{Time: time.Now(), Update: &aggregate.Realtime{Total: aggregate.LiveAggregate{TotalRequests: 5}}}
			resp.StageInfo.Finished = true
			if ws.WriteJSON(resp) != nil {
				return
			}
		}
	}))
	defer srv.Close()

	c := newConnections([]string{strings.TrimPrefix(srv.URL, "http://")})
	c.info = func(...any) {}
	c.errLn = func(data ...any) { t.Log(data...) }
	defer c.closeAll()
	if err := c.connect(0); err != nil {
		t.Fatal(err)
	}
	updates := make(chan aggregate.UpdateReq, 10)
	go c.serveUpdates(t.Context(), updates)
	if err := c.waitForStage(t.Context(), stageBenchmark, false, &bench.Common{}, true); err != nil {
		t.Fatal(err)
	}

	// More requests than the channel can buffer.
	for i := range 2 * cap(updates) {
		resp := make(chan *aggregate.Realtime, 1)
		select {
		case updates <- aggregate.UpdateReq{C: resp}:
		case <-time.After(5 * time.Second):
			t.Fatalf("request %d: send blocked", i)
		}
		select {
		case r := <-resp:
			if r.Total.TotalRequests != 5 {
				t.Fatalf("request %d: got %d total requests, want 5", i, r.Total.TotalRequests)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("request %d: no response", i)
		}
	}
}
