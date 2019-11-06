package generator

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"io"
	"math/rand"

	"github.com/secure-io/sio-go"
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
	o       Options
	buf     *circularBuffer
	rng     *rand.Rand
	randSrc [16]byte
	databuf *bytes.Reader
	obj     Object
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
	r := randomSrc{
		o:       o,
		databuf: bytes.NewReader(data),
		rng:     rng,
		buf:     newCircularBuffer(data, o.totalSize),
		obj: Object{
			Reader:      nil,
			Name:        "",
			ContentType: "application/octet-stream",
			Size:        o.totalSize,
		},
	}
	return &r, nil
}

func (r *randomSrc) Object() *Object {
	data := r.buf.data
	var nBuf [16]byte
	randAsciiBytes(nBuf[:], r.rng)
	r.obj.setName(string(nBuf[:]) + ".bin")

	if len(data) < 128 {
		_, err := io.ReadFull(r.rng, data)
		if err != nil {
			panic(err)
		}
		r.obj.Reader = r.buf
		return &r.obj
	}

	_, err := io.ReadFull(r.rng, r.randSrc[:])
	if err != nil {
		panic(err)
	}

	// Scramble data
	block, _ := aes.NewCipher(r.randSrc[:])
	gcm, _ := cipher.NewGCM(block)
	stream := sio.NewStream(gcm, sio.BufSize)
	r.databuf.Reset(data)
	rr := stream.EncryptReader(r.databuf, r.randSrc[:stream.NonceSize()], nil)
	_, err = io.ReadFull(rr, data)
	if err != nil {
		panic(err)
	}
	r.obj.Reader = r.buf.Reset()
	return &r.obj
}

func (r *randomSrc) String() string {
	return fmt.Sprintf("Random data; %d bytes total, %d byte buffer", r.buf.want, len(r.buf.data))
}
