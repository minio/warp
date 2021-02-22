/*
 * Warp (C) 2021 MinIO, Inc.
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
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"io"
	"math"
	"math/rand"

	"github.com/secure-io/sio-go"
)

type scrambler struct {
	// The total number of bytes to return
	want int64
	// Number of bytes read
	read int64
	// Data source
	stream *sio.EncReader
}

// Reset will reset the scrambler.
// The number of bytes to return can be specified.
// If the number of bytes wanted is <= 0 the value will not be updated.
func (c *scrambler) Reset(want int64) io.ReadSeeker {
	if want > 0 {
		c.want = want
	}
	c.read = 0
	return c
}

// Implement seeker compatible circular buffer,
// implemented for minio-go to allow retries.
func (c *scrambler) Seek(offset int64, whence int) (n int64, err error) {
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

// newCircularBuffer a reader that will produce (virtually) infinitely amounts of random data.
func newScrambler(data []byte, size int64, rng *rand.Rand) *scrambler {
	var randSrc [16]byte

	_, err := io.ReadFull(rng, randSrc[:])
	if err != nil {
		panic(err)
	}
	rand.New(rng).Read(randSrc[:])
	block, _ := aes.NewCipher(randSrc[:])
	gcm, _ := cipher.NewGCM(block)
	stream := sio.NewStream(gcm, sio.BufSize)

	return &scrambler{
		want:   size,
		read:   0,
		stream: stream.EncryptReader(newCircularBuffer(data, math.MaxInt64), randSrc[:stream.NonceSize()], nil),
	}
}

func (c *scrambler) Read(p []byte) (n int, err error) {
	remain := c.want - c.read
	if remain <= 0 {
		if remain != 0 {
			panic(remain)
		}
		return n, io.EOF
	}
	// Make sure we don't overread.
	toDo := len(p)
	if int64(toDo) > remain {
		p = p[:remain]
	}
	copied, err := io.ReadFull(c.stream, p)
	// Assign remaining back to c.left
	p = p[copied:]
	c.read += int64(copied)
	if c.read == c.want {
		return copied, io.EOF
	}
	return copied, nil
}
