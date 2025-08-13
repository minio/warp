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
	"fmt"
	"math/rand"
	"sync/atomic"

	"github.com/minio/pkg/v3/rng"
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
	o.seed = &s
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
	seed *int64
	size int
}

func randomOptsDefaults() RandomOpts {
	return RandomOpts{
		seed: nil,
		// Use 128KB as base.
		size: 128 << 10,
	}
}

type randomSrc struct {
	source  *rng.Reader
	rng     *rand.Rand
	obj     Object
	o       Options
	counter atomic.Uint64
}

func newRandom(o Options) (Source, error) {
	rndSrc := rand.NewSource(int64(rand.Uint64()))
	if o.random.seed != nil {
		rndSrc = rand.NewSource(*o.random.seed)
	}

	size := o.random.size
	if int64(size) > o.totalSize {
		size = int(o.totalSize)
	}
	if size <= 0 {
		return nil, fmt.Errorf("size must be >= 0, got %d", size)
	}

	input, err := rng.NewReader(rng.WithRNG(rand.New(rndSrc)), rng.WithSize(o.totalSize))
	if err != nil {
		return nil, err
	}
	r := randomSrc{
		o:      o,
		rng:    rand.New(rndSrc),
		source: input,
		obj: Object{
			Reader:      nil,
			Name:        "",
			ContentType: "application/octet-stream",
			Size:        0,
		},
	}
	r.obj.setPrefix(o)
	return &r, nil
}

func (r *randomSrc) Object() *Object {
	n := r.counter.Add(1)
	var nBuf [16]byte
	randASCIIBytes(nBuf[:], r.rng)
	r.obj.Size = r.o.getSize(r.rng)
	r.obj.setName(fmt.Sprintf("%d.%s.rnd", n, string(nBuf[:])))

	// Reset scrambler
	r.source.ResetSize(r.obj.Size)
	r.obj.Reader = r.source
	return &r.obj
}

func (r *randomSrc) String() string {
	if r.o.randSize {
		return fmt.Sprintf("Random data; random size up to %d bytes", r.o.totalSize)
	}
	return fmt.Sprintf("Random data; %d bytes total", r.o.totalSize)
}

func (r *randomSrc) Prefix() string {
	return r.obj.Prefix
}
