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
	"testing"
	"time"

	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"
)

// TestServeUpdatesAfterStage checks that update requests are still answered
// after the benchmark stage ends. The UI blocked forever when they were not (#498).
func TestServeUpdatesAfterStage(t *testing.T) {
	c := newConnections([]string{"a", "b"})
	c.lastUpdate[1].Store(&aggregate.Realtime{Total: aggregate.LiveAggregate{TotalRequests: 5}})
	updates := make(chan aggregate.UpdateReq, 10)
	go c.serveUpdates(t.Context(), updates)

	// No clients are connected, so this returns at once, like a finished stage.
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
