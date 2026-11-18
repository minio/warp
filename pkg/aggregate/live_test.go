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

package aggregate

import "testing"

func TestRealtimeMergeFinal(t *testing.T) {
	final := func(n int) *Realtime { return &Realtime{Final: true, Total: LiveAggregate{TotalRequests: n}} }
	live := func(n int) *Realtime { return &Realtime{Total: LiveAggregate{TotalRequests: n}} }
	tests := []struct {
		name string
		in   []*Realtime
		want bool
	}{
		{"final", []*Realtime{final(5)}, true},
		{"all final", []*Realtime{final(5), final(5)}, true},
		{"final then live", []*Realtime{final(5), live(5)}, false},
		{"live then final", []*Realtime{live(5), final(5)}, false},
		{"empty live then final", []*Realtime{live(0), final(5)}, false},
		{"final then empty live", []*Realtime{final(5), live(0)}, false},
		{"nil ignored", []*Realtime{nil, final(5), nil}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var r Realtime
			for _, in := range tt.in {
				r.Merge(in)
			}
			if r.Final != tt.want {
				t.Errorf("Final = %v, want %v", r.Final, tt.want)
			}
		})
	}
}
