/*
 * Warp (C) 2019- MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
func (c *circularBuffer) Reset(want int64) io.Reader {
	if want > 0 {
		c.want = want
	}
	c.read = 0
	c.left = c.data
	return c
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
