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

package control

import "github.com/minio/warp/pkg/aggregate"

// applyClientNames rewrites client identifiers in a result from the address used
// in distributed mode to the friendly pool name, so the dashboard's "by client"
// view shows names. Identifiers that don't match a known client (e.g. older
// results keyed by a random ID) are left unchanged.
func (s *Service) applyClientNames(rt *aggregate.Realtime) {
	if rt == nil {
		return
	}
	names := map[string]string{}
	for _, c := range s.store.ListClients() {
		if c.Address != "" && c.Name != "" {
			names[c.Address] = c.Name
		}
	}
	if len(names) == 0 {
		return
	}
	rename := func(k string) string {
		if n, ok := names[k]; ok {
			return n
		}
		return k
	}

	remapLA := func(la *aggregate.LiveAggregate) {
		if la == nil {
			return
		}
		if len(la.Clients) > 0 {
			nc := make(aggregate.MapAsSlice, len(la.Clients))
			for k := range la.Clients {
				nc[rename(k)] = struct{}{}
			}
			la.Clients = nc
		}
		if len(la.ThroughputByClient) > 0 {
			m := make(map[string]aggregate.Throughput, len(la.ThroughputByClient))
			for k, v := range la.ThroughputByClient {
				m[rename(k)] = v
			}
			la.ThroughputByClient = m
		}
		if len(la.Requests) > 0 {
			m := make(map[string]aggregate.RequestSegments, len(la.Requests))
			for k, v := range la.Requests {
				m[rename(k)] = v
			}
			la.Requests = m
		}
	}

	remapLA(&rt.Total)
	for _, la := range rt.ByOpType {
		remapLA(la)
	}
	for _, la := range rt.ByHost {
		remapLA(la)
	}
	for _, la := range rt.ByCategory {
		remapLA(la)
	}
	// The ByClient map is keyed by client identifier; remap both keys and values.
	if len(rt.ByClient) > 0 {
		m := make(map[string]*aggregate.LiveAggregate, len(rt.ByClient))
		for k, v := range rt.ByClient {
			remapLA(v)
			m[rename(k)] = v
		}
		rt.ByClient = m
	}
}
