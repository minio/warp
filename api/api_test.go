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

package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/minio/warp/pkg/aggregate"
)

// TestStatusCanceled checks that a canceled status request returns, both when its
// update request is never accepted and when it is accepted but never answered.
func TestStatusCanceled(t *testing.T) {
	for _, accept := range []bool{false, true} {
		t.Run(fmt.Sprint("accept=", accept), func(t *testing.T) {
			ups := make(chan aggregate.UpdateReq)
			s := &Server{updates: ups}
			ctx, cancel := context.WithCancel(t.Context())
			req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/status", nil)
			done := make(chan struct{})
			go func() {
				s.handleStatus(httptest.NewRecorder(), req)
				close(done)
			}()
			var r aggregate.UpdateReq
			if accept {
				select {
				case r = <-ups:
				case <-time.After(5 * time.Second):
					t.Fatal("handler did not send an update request")
				}
			}
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("handler did not return after the request was canceled")
			}
			if accept {
				// A late reply must not block the responder.
				select {
				case r.C <- &aggregate.Realtime{}:
				default:
					t.Fatal("late reply blocked")
				}
			}
		})
	}
}
