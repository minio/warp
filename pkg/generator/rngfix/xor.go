// Copyright (c) 2015-2025 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package rngfix

// xor32Go will do out[x] = in[x] ^ v[x mod 32]
// len(in) must >= len(out) and length must be a multiple of 32.
func xor32Go(in, out []byte, v *[4]uint64) { //nolint:unused
	if unsafeEnabled {
		// Faster with "unsafe", slower without.
		var i int
		for len(out)-i >= 32 {
			if len(in)-i < 32 {
				panic("short input")
			}
			store64(out, i, v[0]^load64(in, i))
			store64(out, i+8, v[1]^load64(in, i+8))
			store64(out, i+16, v[2]^load64(in, i+16))
			store64(out, i+24, v[3]^load64(in, i+24))
			i += 32
		}
	} else {
		for len(out) >= 32 {
			inS := in[:32]
			v0 := v[0] ^ load64(inS, 0)
			v1 := v[1] ^ load64(inS, 8)
			v2 := v[2] ^ load64(inS, 16)
			v3 := v[3] ^ load64(inS, 24)
			store64(out, 0, v0)
			store64(out, 8, v1)
			store64(out, 16, v2)
			store64(out, 24, v3)
			out = out[32:]
			in = in[32:]
		}
	}
}
