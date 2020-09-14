/*
 * Warp (C) 2019-2020 MinIO, Inc.
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

package generator

import (
	"errors"
	"io"
)

type circularBuffer struct {
	data []byte
	// left aliases the data at the current read position.
	left []byte

	// The total number of bytes to return
	// When this
	want int64
	read int64
}

// Reset will reset the circular buffer.
// The number of bytes to return can be specified.
// If the number of bytes wanted is <= 0 the value will not be updated.
func (c *circularBuffer) Reset(want int64) io.ReadSeeker {
	if want > 0 {
		c.want = want
	}
	c.read = 0
	c.left = c.data
	return c
}

// Implement seeker compatible circular buffer,
// implemented for minio-go to allow retries.
func (c *circularBuffer) Seek(offset int64, whence int) (n int64, err error) {
	// Switch through whence.
	switch whence {
	default:
		return 0, errors.New("circularBuffer.Seek: invalid whence")
	case io.SeekStart:
		if offset > c.want {
			return 0, io.EOF
		}
		c.read = offset
	case io.SeekCurrent:
		if offset+c.read > c.want {
			return 0, io.EOF
		}
		c.read += offset
	case io.SeekEnd:
		if offset > 0 {
			return 0, io.EOF
		}
		if c.want+offset < 0 {
			return 0, io.ErrShortBuffer
		}
		c.read = c.want + offset
	}
	if c.read < 0 {
		return 0, errors.New("circularBuffer.Seek: negative position")
	}
	return c.read, nil
}

// newCircularBuffer returns a new circular buffer.
// Data will be served
func newCircularBuffer(data []byte, size int64) *circularBuffer {
	return &circularBuffer{
		data: data,
		left: data,
		want: size,
		read: 0,
	}
}

func (c *circularBuffer) Read(p []byte) (n int, err error) {
	if len(c.data) == 0 {
		return 0, errors.New("circularBuffer: no data")
	}
	for len(p) > 0 {
		if len(c.left) == 0 {
			c.left = c.data
		}
		remain := c.want - c.read
		if remain <= 0 {
			if remain != 0 {
				panic(remain)
			}
			return n, io.EOF
		}

		// Make sure we don't overread.
		toDo := c.left
		if int64(len(toDo)) > remain {
			toDo = toDo[:remain]
		}

		copied := copy(p, toDo)
		// Assign remaining back to c.left
		c.left = toDo[copied:]
		p = p[copied:]
		c.read += int64(copied)
		n += copied
	}
	return n, nil
}
