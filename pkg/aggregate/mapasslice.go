/*
 * Warp (C) 2019-2025 MinIO, Inc.
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

import (
	"bytes"
	"encoding/json"
	"sort"
)

// MapAsSlice is a key-only map that is serialized as an array.
type MapAsSlice map[string]struct{}

// Add value
func (m *MapAsSlice) Add(k string) {
	if *m == nil {
		*m = make(MapAsSlice)
	}
	mp := *m
	mp[k] = struct{}{}
}

// AddMap adds another map.
func (m *MapAsSlice) AddMap(other MapAsSlice) {
	if *m == nil {
		*m = make(MapAsSlice, len(other))
	}
	mp := *m
	for k := range other {
		mp[k] = struct{}{}
	}
}

// AddSlice adds a slice.
func (m *MapAsSlice) AddSlice(other []string) {
	if *m == nil {
		*m = make(MapAsSlice, len(other))
	}
	mp := *m
	for _, k := range other {
		mp[k] = struct{}{}
	}
}

// SetSlice replaces the value with the content of a slice.
func (m *MapAsSlice) SetSlice(v []string) {
	*m = make(MapAsSlice, len(v))
	mp := *m
	for _, k := range v {
		mp[k] = struct{}{}
	}
}

// Clone returns a clone.
func (m *MapAsSlice) Clone() MapAsSlice {
	if m == nil {
		return MapAsSlice(nil)
	}
	mm := make(MapAsSlice, len(*m))
	for k, v := range *m {
		mm[k] = v
	}
	return mm
}

// MarshalJSON provides output as JSON.
func (m MapAsSlice) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	var dst bytes.Buffer
	dst.WriteByte('[')
	x := m.Slice()
	for i, k := range x {
		dst.WriteByte('"')
		json.HTMLEscape(&dst, []byte(k))
		dst.WriteByte('"')
		if i < len(x)-1 {
			dst.WriteByte(',')
		}
	}
	dst.WriteByte(']')

	return dst.Bytes(), nil
}

// UnmarshalJSON reads an array of strings and sets them as keys in the map.
func (m *MapAsSlice) UnmarshalJSON(b []byte) error {
	var tmp []string
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	if tmp == nil {
		*m = nil
		return nil
	}
	var dst = make(MapAsSlice, len(tmp))
	for _, v := range tmp {
		dst[v] = struct{}{}
	}
	*m = dst
	return nil
}

// Slice returns the keys as a sorted slice.
func (m MapAsSlice) Slice() []string {
	x := make([]string, 0, len(m))
	for k := range m {
		x = append(x, k)
	}
	sort.Strings(x)
	return x
}
