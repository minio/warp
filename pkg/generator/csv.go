package generator

import (
	"errors"
	"fmt"
	"math/rand"
)

func WithCSV() CsvOpts {
	return csvOptsDefaults()
}

func (o CsvOpts) Apply() Option {
	return func(opts *Options) error {
		if err := o.validate(); err != nil {
			return err
		}
		opts.csv = o
		opts.src = newCsv
		return nil
	}
}

func (o CsvOpts) validate() error {
	if o.rows < 0 {
		return errors.New("csv: rows <= 0")
	}
	if o.cols < 0 {
		return errors.New("csv: cols <= 0")
	}
	if o.minLen > o.maxLen {
		return fmt.Errorf("WithCSV.FieldLen: min:%d > max:%d", o.minLen, o.maxLen)
	}

	return nil
}

// Size sets the size of generated CSV.
func (o CsvOpts) Size(cols, rows int) CsvOpts {
	o.rows = rows
	o.cols = cols
	return o
}

// Comma sets the comma character. Only ASCII values should be used.
func (o CsvOpts) Comma(c byte) CsvOpts {
	o.comma = c
	return o
}

// FieldLen sets the length of each field.
func (o CsvOpts) FieldLen(min, max int) CsvOpts {
	o.minLen = min
	o.maxLen = max
	return o
}

// RngSeed will which to a fixed RNG seed to make usage predictable.
func (o CsvOpts) RngSeed(s int64) CsvOpts {
	o.seed = s
	return o
}

// CsvOpts provides options for CSV generation.
type CsvOpts struct {
	err            error
	cols, rows     int
	comma          byte
	seed           int64
	minLen, maxLen int
}

func csvOptsDefaults() CsvOpts {
	return CsvOpts{
		err:    nil,
		cols:   500,
		rows:   15,
		comma:  ',',
		seed:   rand.Int63(),
		minLen: 5,
		maxLen: 15,
	}
}

type csvSource struct {
	o       Options
	buf     *circularBuffer
	builder []byte
	obj     Object

	// We may need a faster RNG for this...
	rng *rand.Rand
}

func newCsv(o Options) (Source, error) {
	c := csvSource{
		o: o,
	}
	c.builder = make([]byte, 0, o.csv.maxLen+1)
	c.buf = newCircularBuffer(make([]byte, o.csv.maxLen*(o.csv.cols+1)*(o.csv.rows+1)), o.totalSize)
	c.rng = rand.New(rand.NewSource(o.csv.seed))
	c.obj.ContentType = "text/csv"
	c.obj.Size = o.totalSize
	c.obj.setPrefix(o)

	return &c, nil
}

func (c *csvSource) Object() *Object {
	opts := c.o.csv
	var dst = c.buf.data[:0]
	for i := 0; i < opts.rows; i++ {
		for j := 0; j < opts.cols; j++ {
			fieldLen := 1 + opts.minLen
			if opts.minLen != opts.maxLen {
				fieldLen += c.rng.Intn(opts.maxLen - opts.minLen)
			}
			build := c.builder[:fieldLen]
			randAsciiBytes(build[:fieldLen-1], c.rng)
			build[fieldLen-1] = opts.comma
			if j == opts.cols-1 {
				build[fieldLen-1] = '\n'
			}
			dst = append(dst, build...)
		}
	}
	c.buf.data = dst
	c.obj.Reader = c.buf.Reset()
	var nBuf [16]byte
	randAsciiBytes(nBuf[:], c.rng)
	c.obj.setName(string(nBuf[:]) + ".csv")
	return &c.obj

}

func (c *csvSource) String() string {
	return fmt.Sprintf("CSV data. %d columns, %d rows.", c.o.csv.cols, c.o.csv.rows)
}
