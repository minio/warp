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

// TestRealtimeMergeKeepsInputs checks that cached snapshots can be merged repeatedly
// without the merge writing into them.
func TestRealtimeMergeKeepsInputs(t *testing.T) {
	agg := func() LiveAggregate {
		return LiveAggregate{
			TotalRequests:      1,
			ThroughputByHost:   map[string]Throughput{"h": {Operations: 1, Bytes: 10}},
			ThroughputByClient: map[string]Throughput{"c": {Operations: 1, Bytes: 10}},
		}
	}
	snapshot := func() *Realtime {
		get := agg()
		return &Realtime{Total: agg(), ByOpType: map[string]*LiveAggregate{"GET": &get}}
	}
	bytes := func(l LiveAggregate) float64 {
		return l.ThroughputByHost["h"].Bytes + l.ThroughputByClient["c"].Bytes
	}
	a, b := snapshot(), snapshot()
	for i := range 2 {
		var r Realtime
		r.Merge(a)
		r.Merge(b)
		if got := bytes(r.Total) + bytes(*r.ByOpType["GET"]); got != 80 {
			t.Fatalf("merge %d: got %v bytes, want 80", i, got)
		}
	}
	if got := bytes(a.Total) + bytes(*a.ByOpType["GET"]); got != 40 {
		t.Fatalf("input changed: got %v bytes, want 40", got)
	}
}
