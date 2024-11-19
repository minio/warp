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

	Prefix string

	VersionID string

	// Size of the object to expect.
	Size int64
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
func GetExpRandSize(rng *rand.Rand, minSize, maxSize int64) int64 {
	if maxSize-minSize < 10 {
		if maxSize-minSize <= 0 {
			return 0
		}
		return 1 + minSize + rng.Int63n(maxSize-minSize)
	}
	logSizeMaxSize := math.Log2(float64(maxSize - 1))
	logSizeMinSize := math.Max(7, logSizeMaxSize-8)
	if minSize > 0 {
		logSizeMinSize = math.Log2(float64(minSize - 1))
	}
	lsDelta := logSizeMaxSize - logSizeMinSize
	random := rng.Float64()
	logSize := random * lsDelta
	if logSize > 1 {
		return 1 + int64(math.Pow(2, logSize+logSizeMinSize))
	}
	// For lowest part, do equal distribution
	return 1 + minSize + int64(random*math.Pow(2, logSizeMinSize+1))
}
