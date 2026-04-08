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

import (
	"encoding/binary"
	"errors"
	"io"
	"math/bits"
	"math/rand"
	"time"
)

const (
	bufferLog  = 14
	bufferSize = 1 << bufferLog
	bufferMask = bufferSize - 1
)

// Reader will return a reader that will return an endless stream of
// pseudo-random data.
// The Reader supports seeking and arbitrary async reads from io.ReadAt.
type Reader struct {
	buf          [bufferSize]byte
	tmp          [32]byte
	subxor       [4]uint64
	o            *readerOptions
	offset       int64
	bufferSeeded bool
}

type readerOptions struct {
	rng       io.Reader
	size      int64
	fullReset bool
}

// ReaderOption provides an option to NewReader.
type ReaderOption func(options *readerOptions) error

// NewReader returns a new Reader.
func NewReader(opts ...ReaderOption) (*Reader, error) {
	var r Reader
	o := readerOptions{
		size: -1,
		rng:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for _, opt := range opts {
		err := opt(&o)
		if err != nil {
			return nil, err
		}
	}
	r.o = &o
	return &r, r.init()
}

func (r *Reader) init() error {
	if !r.bufferSeeded {
		_, err := io.ReadFull(r.o.rng, r.buf[:])
		if err != nil {
			return err
		}
		r.bufferSeeded = !r.o.fullReset
	}
	// Always reset subkeys.
	_, err := io.ReadFull(r.o.rng, r.tmp[:])
	if err != nil {
		return err
	}
	r.subxor[0] = binary.LittleEndian.Uint64(r.tmp[:8])
	r.subxor[1] = binary.LittleEndian.Uint64(r.tmp[8:16])
	r.subxor[2] = binary.LittleEndian.Uint64(r.tmp[16:24])
	r.subxor[3] = binary.LittleEndian.Uint64(r.tmp[24:32])
	r.offset = 0
	return nil
}

// scrambleU64 is xxh3, but for len=8 only.
func scrambleU64(v uint64) uint64 {
	h64 := v ^ (0x1cad21f72c81017c ^ 0xdb979083e96dd4de)
	h64 = bits.RotateLeft64(h64, 49) ^ bits.RotateLeft64(h64, 24)
	h64 *= 0x9fb21c651e98df25
	h64 ^= (h64 >> 35) + 8
	h64 *= 0x9fb21c651e98df25
	h64 ^= h64 >> 28
	return h64
}

// Read satisfies the io.Reader interface.
func (r *Reader) Read(p []byte) (n int, err error) {
	var keys [4]uint64
	const debug = false
	isEOF := false
	if r.o.size >= 0 && int64(len(p))+r.offset >= r.o.size {
		isEOF = true
		p = p[:r.o.size-r.offset]
	}
	for len(p) > 0 {
		// Keys are the same for the block.
		blockN := uint64(r.offset >> bufferLog)
		scrambleBase := scrambleU64(blockN)
		for i := range keys[:] {
			// Generate 4 unique keys, and mix in offset again multiplied by a prime.
			keys[i] = scrambleBase ^ r.subxor[i] ^ (blockN * 11400714785074694791)
		}
		if r.offset&31 != 0 || len(p) < 32 {
			// Fill until we align
			startAligned := (r.offset & bufferMask >> 5) << 5
			xorSlice(r.buf[startAligned:], r.tmp[:], &keys)
			startCopy := r.offset & 31
			copied := copy(p, r.tmp[startCopy:])
			if copied == 0 {
				panic("no progress")
			}
			p = p[copied:]
			n += copied
			r.offset += int64(copied)
			continue
		}
		if debug && r.offset&31 != 0 {
			panic(r.offset & 31)
		}
		// Input is aligned.
		input := r.buf[r.offset&bufferMask:]
		lenAligned := min((len(p)>>5)<<5, len(input))
		xorSlice(input, p[:lenAligned], &keys)
		n += lenAligned
		p = p[lenAligned:]
		r.offset += int64(lenAligned)
		if len(p) < 32 && n > 0 && !isEOF {
			// Do short read to keep alignment
			break
		}
	}
	if isEOF {
		return n, io.EOF
	}
	return n, nil
}

// ReadAt satisfies the io.ReaderAt interface.
func (r *Reader) ReadAt(p []byte, off int64) (n int, err error) {
	var keys [4]uint64
	const debug = false
	isEOF := false
	if r.o.size >= 0 && int64(len(p))+off >= r.o.size {
		isEOF = true
		p = p[:r.o.size-off]
	}
	for len(p) > 0 {
		// Keys are the same for the block.
		blockN := uint64(off >> bufferLog)
		scrambleBase := scrambleU64(blockN)
		for i := range keys[:] {
			// Generate 4 unique keys, and mix in offset again multiplied by a prime.
			keys[i] = scrambleBase ^ r.subxor[i] ^ (blockN * 11400714785074694791)
		}
		if off&31 != 0 || len(p) < 32 {
			// Fill until we align
			startAligned := (off & bufferMask >> 5) << 5
			xorSlice(r.buf[startAligned:], r.tmp[:], &keys)
			startCopy := off & 31
			copied := copy(p, r.tmp[startCopy:])
			if copied == 0 {
				panic("no progress")
			}
			p = p[copied:]
			n += copied
			off += int64(copied)
			continue
		}
		if debug && off&31 != 0 {
			panic(off & 31)
		}
		// Input is aligned.
		input := r.buf[off&bufferMask:]
		lenAligned := min((len(p)>>5)<<5, len(input))
		xorSlice(input, p[:lenAligned], &keys)
		n += lenAligned
		p = p[lenAligned:]
		off += int64(lenAligned)
	}
	if isEOF {
		return n, io.EOF
	}
	return n, nil
}

// Seek provides stream seeking via io.Seeker interface.
// Streams without a size set cannot seek relative to end.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		r.offset = offset
	case io.SeekCurrent:
		r.offset += offset
	case io.SeekEnd:
		if r.o.size < 0 {
			return 0, errors.New("Seek: seeking to end of endless stream")
		}
		r.offset = r.o.size + offset
	default:
		return 0, errors.New("Seek: invalid whence")
	}
	if r.offset < 0 {
		return 0, errors.New("Seek: negative offset")
	}
	if r.o.size >= 0 && r.offset > r.o.size {
		return 0, io.ErrUnexpectedEOF
	}
	return r.offset, nil
}

// Reset will reset the stream and scramble the seed.
func (r *Reader) Reset() error {
	return r.init()
}

// ResetSize will reset the stream and scramble the seed,
// as well as setting a new size.
func (r *Reader) ResetSize(size int64) error {
	r.o.size = size
	return r.init()
}

// WithRNG allows to use a specific reader for entropy.
// Otherwise a math/rand prng with a seed based on current time will be added.
func WithRNG(rng io.Reader) ReaderOption {
	return func(o *readerOptions) error {
		o.rng = rng
		return nil
	}
}

// WithSize limits the reader to a specific size.
func WithSize(size int64) ReaderOption {
	return func(o *readerOptions) error {
		o.size = size
		return nil
	}
}

// WithFullReset will fully re-seed the reader on Reset.
// If set the entire stream will be randomized.
// If not set, it would be possible to derive a 32 byte xor value
// that makes it possible to predict a stream from the previous output.
func WithFullReset(b bool) ReaderOption {
	return func(o *readerOptions) error {
		o.fullReset = b
		return nil
	}
}
