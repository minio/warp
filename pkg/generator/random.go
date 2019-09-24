package generator

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
)

func WithRandomData() RandomOpts {
	return randomOptsDefaults()
}

// Apply Random data options.
func (o RandomOpts) Apply() Option {
	return func(opts *Options) error {
		if err := o.validate(); err != nil {
			return err
		}
		opts.random = o
		opts.src = newRandom
		return nil
	}
}

func (o RandomOpts) validate() error {
	if o.size <= 0 {
		return errors.New("random: size <= 0")
	}
	return nil
}

// RngSeed will which to a fixed RNG seed to make usage predictable.
func (o RandomOpts) RngSeed(s int64) RandomOpts {
	o.seed = s
	return o
}

// Size will set a block size.
// Data of this size will be repeated until output size has been reached.
func (o RandomOpts) Size(s int) RandomOpts {
	o.size = s
	return o
}

// RandomOpts are the options for the random data source.
type RandomOpts struct {
	seed int64
	size int
}

func randomOptsDefaults() RandomOpts {
	return RandomOpts{
		seed: rand.Int63(),
		// 10 MB before we wrap around.
		size: 10 << 20,
	}
}

type randomSrc struct {
	buf    *circularBuffer
	rng    *rand.Rand
	xorSrc [16]byte
}

func newRandom(o Options) (Source, error) {
	rng := rand.New(rand.NewSource(o.random.seed))
	size := o.random.size
	if int64(size) > o.totalSize {
		size = int(o.totalSize)
	}
	if size <= 0 {
		return nil, fmt.Errorf("size must be >= 0, got %d", size)
	}
	data := make([]byte, size)
	_, err := io.ReadFull(rng, data)
	if err != nil {
		return nil, err
	}

	return &randomSrc{
		rng: rng,
		buf: newCircularBuffer(data, o.totalSize),
	}, nil
}

func (r *randomSrc) Reader() io.Reader {
	data := r.buf.data
	if len(data) < 128 {
		_, err := io.ReadFull(r.rng, data)
		if err != nil {
			panic(err)
		}
		return r.buf
	}

	// Xor with a 16 byte buffer.
	_, err := io.ReadFull(r.rng, r.xorSrc[:])
	if err != nil {
		panic(err)
	}
	for i := range data {
		data[i] ^= r.xorSrc[i&15]
	}
	return r.buf.Reset()
}

func (r *randomSrc) String() string {
	return fmt.Sprintf("Random data; %d bytes total, %d byte buffer", r.buf.want, len(r.buf.data))
}
