// Copyright (c) 2015-2021 MinIO, Inc.
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

//+build !noasm
//+build !appengine
//+build !gccgo

// func xorSlice(in, out []byte, v *[4]uint64)
TEXT ·xorSlice(SB), 7, $0
	MOVQ  v+48(FP), AX         // AX: v
	MOVQ  in+0(FP), SI         // SI: &in
	MOVQ  out+24(FP), DX       // DX: &out
	MOVQ  out_len+32(FP), R9   // R9: len(out)
	MOVOU (AX), X0             // v[x]
	MOVOU 16(AX), X1           // v[x+2]
	SHRQ  $5, R9               // len(in) / 32
	JZ    done_xor_sse2_32

loopback_xor_sse2_32:
	MOVOU (SI), X2             // in[x]
	MOVOU 16(SI), X3           // in[x+16]
	PXOR  X0, X2
	PXOR  X1, X3
	MOVOU X2, (DX)
	MOVOU X3, 16(DX)
	ADDQ  $32, SI              // in+=32
	ADDQ  $32, DX              // out+=32
	SUBQ  $1, R9
	JNZ   loopback_xor_sse2_32

done_xor_sse2_32:
	RET
