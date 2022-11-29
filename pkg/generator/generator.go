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
	"math"
	"math/rand"
	"path"
	"runtime"
)

// Option provides options for data generation.
// Use WithXXXX().Apply() to select data types and set options.
type Option func(o *Options) error

type Source interface {
	// Requesting a new reader will scramble data, so the new reader will not return the same data.
	// Requesting a reader is designed to be as lightweight as possible.
	// Only a single reader can be used concurrently.
	Object() *Object

	// String returns a human readable description of the source.
	String() string

	// Prefix returns the prefix if any.
	Prefix() string
}

type Object struct {
	// Reader will return a reader that will return the number of requested bytes
	// and EOF on all subsequent calls.
	Reader io.ReadSeeker

	// A random generated name.
	Name string

	// Corresponding mime type
	ContentType string

	// Size of the object to expect.
	Size int64

	Prefix string

	VersionID string
}

// Objects is a slice of objects.
type Objects []Object

// Prefixes returns all prefixes.
func (o Objects) Prefixes() []string {
	prefixes := make(map[string]struct{}, runtime.GOMAXPROCS(0))
	for _, p := range o {
		prefixes[p.Prefix] = struct{}{}
	}
	res := make([]string, 0, len(prefixes))
	for p := range prefixes {
		res = append(res, p)
	}
	return res
}

// MergeObjectPrefixes merges prefixes from several slices of objects.
func MergeObjectPrefixes(o []Objects) []string {
	prefixes := make(map[string]struct{}, runtime.GOMAXPROCS(0))
	for _, objs := range o {
		for _, p := range objs {
			prefixes[p.Prefix] = struct{}{}
		}
	}
	res := make([]string, 0, len(prefixes))
	for p := range prefixes {
		res = append(res, p)
	}
	return res
}

func (o *Object) setPrefix(opts Options) {
	if opts.randomPrefix <= 0 {
		o.Prefix = opts.customPrefix
		return
	}
	b := make([]byte, opts.randomPrefix)
	rng := rand.New(rand.NewSource(int64(rand.Uint64())))
	randASCIIBytes(b, rng)
	o.Prefix = path.Join(opts.customPrefix, string(b))
}

func (o *Object) setName(s string) {
	if len(o.Prefix) == 0 {
		o.Name = s
		return
	}
	o.Name = o.Prefix + "/" + s
}

// New return data source.
func New(opts ...Option) (Source, error) {
	options := defaultOptions()
	for _, ofn := range opts {
		err := ofn(&options)
		if err != nil {
			return nil, err
		}
	}
	if options.src == nil {
		return nil, errors.New("internal error: generator Source was nil")
	}
	return options.src(options)
}

// NewFn return data source.
func NewFn(opts ...Option) (func() Source, error) {
	options := defaultOptions()
	for _, ofn := range opts {
		err := ofn(&options)
		if err != nil {
			return nil, err
		}
	}
	if options.src == nil {
		return nil, errors.New("internal error: generator Source was nil")
	}

	return func() Source {
		s, err := options.src(options)
		if err != nil {
			panic(err)
		}
		return s
	}, nil
}

const asciiLetters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890()"

var asciiLetterBytes [len(asciiLetters)]byte

func init() {
	for i, v := range asciiLetters {
		asciiLetterBytes[i] = byte(v)
	}
}

// randASCIIBytes fill destination with pseudorandom ASCII characters [a-ZA-Z0-9].
// Should never be considered for true random data generation.
func randASCIIBytes(dst []byte, rng *rand.Rand) {
	// Use a single seed.
	v := rng.Uint64()
	rnd := uint32(v)
	rnd2 := uint32(v >> 32)
	for i := range dst {
		dst[i] = asciiLetterBytes[int(rnd>>16)%len(asciiLetterBytes)]
		rnd ^= rnd2
		rnd *= 2654435761
	}
}

// GetExpRandSize will return an exponential random size from 1 to and including max.
// Minimum size: 127 bytes, max scale is 256 times smaller than max size.
// Average size will be max_size * 0.179151.
func GetExpRandSize(rng *rand.Rand, max int64) int64 {
	if max < 10 {
		if max == 0 {
			return 0
		}
		return 1 + rng.Int63n(max)
	}
	logSizeMax := math.Log2(float64(max - 1))
	logSizeMin := math.Max(7, logSizeMax-8)
	lsDelta := logSizeMax - logSizeMin
	random := rng.Float64()
	logSize := random * lsDelta
	if logSize > 1 {
		return 1 + int64(math.Pow(2, logSize+logSizeMin))
	}
	// For lowest part, do linear
	return 1 + int64(random*math.Pow(2, logSizeMin+1))
}

// GetDistributionSize will pick a random value from the provided distribution list.
func GetDistributionSize(rng *rand.Rand, dist []int64) int64 {
	idx := 1 + rng.Int63n(int64(len(dist))) // generates a random value between [1, 100] inclusive.
	return int64(dist[idx-1])               // returns a value between [0, 99].
}
