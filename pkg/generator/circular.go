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

func (c *circularBuffer) Reset() io.Reader {
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
