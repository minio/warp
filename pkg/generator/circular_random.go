package generator

import (
	"fmt"
	"io"
	"math/rand"
)

// CircularRandomOpts are the options for the circular random data source.
type CircularRandomOpts struct {
	seed *int64
	size int
}

func WithCircularRandomData() CircularRandomOpts {
	return CircularRandomOpts{
		seed: nil,
		// Use 2^20 + 1 (1MB + 1 byte) as default size
		size: 1<<20 + 1,
	}
}

// Apply Circular Random data options.
func (o CircularRandomOpts) Apply() Option {
	return func(opts *Options) error {
		if err := o.validate(); err != nil {
			return err
		}
		opts.circularRandom = o
		opts.src = newCircularRandom
		return nil
	}
}

func (o CircularRandomOpts) validate() error {
	if o.size <= 0 {
		return fmt.Errorf("circular random: size must be > 0, got %d", o.size)
	}
	return nil
}

// RngSeed will set a fixed RNG seed to make usage predictable.
func (o CircularRandomOpts) RngSeed(s int64) CircularRandomOpts {
	o.seed = &s
	return o
}

// Size will set the size of the circular buffer.
func (o CircularRandomOpts) Size(s int) CircularRandomOpts {
	o.size = s
	return o
}

type circularRandomSrc struct {
	buf []byte
	rng *rand.Rand
	obj Object
	o   Options
	pos int64
}

func newCircularRandom(o Options) (Source, error) {
	rndSrc := rand.NewSource(int64(rand.Uint64()))
	if o.circularRandom.seed != nil {
		rndSrc = rand.NewSource(*o.circularRandom.seed)
	}
	rng := rand.New(rndSrc)

	size := o.circularRandom.size
	if size <= 0 {
		return nil, fmt.Errorf("size must be > 0, got %d", size)
	}

	// Generate random data for the circular buffer
	buf := make([]byte, size)
	_, err := io.ReadFull(rng, buf)
	if err != nil {
		return nil, err
	}

	r := &circularRandomSrc{
		o:   o,
		rng: rng,
		buf: buf,
		obj: Object{
			Reader:      nil,
			Name:        "",
			ContentType: "application/octet-stream",
			Size:        0,
		},
		pos: 0,
	}
	r.obj.setPrefix(o)
	return r, nil
}

func (r *circularRandomSrc) Object() *Object {
	var nBuf [16]byte
	randASCIIBytes(nBuf[:], r.rng)
	r.obj.Size = r.o.getSize(r.rng)
	r.obj.setName(fmt.Sprintf("%s.crnd", string(nBuf[:])))

	// Create a new reader for this object, continuing from the current position
	r.obj.Reader = &circularReader{
		buf:    r.buf,
		pos:    r.pos,
		end:    r.pos + r.obj.Size,
		remain: r.obj.Size,
	}

	// Update the position for the next object
	r.pos = (r.pos + r.obj.Size) % int64(len(r.buf))

	return &r.obj
}

func (r *circularRandomSrc) String() string {
	if r.o.randSize {
		return fmt.Sprintf("Circular Random data; random size up to %d bytes", r.o.totalSize)
	}
	return fmt.Sprintf("Circular Random data; %d bytes total", r.o.totalSize)
}

func (r *circularRandomSrc) Prefix() string {
	return r.obj.Prefix
}

type circularReader struct {
	buf    []byte
	pos    int64
	end    int64
	remain int64
}

func (cr *circularReader) Read(p []byte) (n int, err error) {
	if cr.remain <= 0 {
		return 0, io.EOF
	}

	if int64(len(p)) > cr.remain {
		p = p[:cr.remain]
	}

	n = len(p)
	for i := range p {
		p[i] = cr.buf[cr.pos%int64(len(cr.buf))]
		cr.pos++
	}

	cr.remain -= int64(n)
	return n, nil
}

func (cr *circularReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = cr.pos - cr.end + cr.remain + offset
	case io.SeekEnd:
		abs = cr.remain + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if abs < 0 {
		return 0, fmt.Errorf("negative position: %d", abs)
	}

	if abs > cr.end-cr.pos {
		cr.remain = 0
		return cr.end - cr.pos, io.EOF
	}

	cr.remain = cr.end - cr.pos - abs
	return abs, nil
}
