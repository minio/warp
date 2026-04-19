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

// We enable 64 bit LE platforms:

//go:build (amd64 || arm64 || ppc64le || riscv64) && !nounsafe && !purego && !appengine

package rngfix

import (
	"unsafe"
)

const unsafeEnabled = true //nolint:unused

func load64(b []byte, i int) uint64 { //nolint:unused
	// return binary.LittleEndian.Uint64(b[i:])
	// return *(*uint64)(unsafe.Pointer(&b[i]))
	return *(*uint64)(unsafe.Pointer(uintptr(unsafe.Pointer(&b[0])) + uintptr(i)*unsafe.Sizeof(b[0])))
}

func store64(b []byte, i int, v uint64) { //nolint:unused
	// binary.LittleEndian.PutUint64(b, v)
	*(*uint64)(unsafe.Pointer(uintptr(unsafe.Pointer(&b[0])) + uintptr(i)*unsafe.Sizeof(b[0]))) = v
}
