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
	"math/rand"
	"path"
	"runtime"
	"strings"

	"github.com/minio/minio-go/v7/pkg/set"
)

// path2BucketPrefixWithBasePath returns bucket and prefix, if any,
// of a 'path'. basePath is trimmed from the front of the 'path'.
func path2BucketPrefixWithBasePath(basePath, path string) (bucket, prefix string) {
	path = strings.TrimPrefix(path, basePath)
	path = strings.TrimPrefix(path, "/")
	m := strings.Index(path, "/")
	if m < 0 {
		return path, ""
	}
	return path[:m], path[m+len("/"):]
}

func path2BucketPrefix(s string) (bucket, prefix string) {
	return path2BucketPrefixWithBasePath("", s)
}

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
}

type Object struct {
	// Reader will return a reader that will return the number of requested bytes
	// and EOF on all subsequent calls.
	Reader io.Reader

	// Bucket where this object to be uploaded
	Bucket string

	// Pre-defined prefix with in the bucket where the object
	// is uploaded to, obj.Name already has the prefix set
	Prefix string

	// A random generated name.
	Name string

	// Corresponding mime type
	ContentType string

	// Size of the object to expect.
	Size int64

	VersionID string
}

// Objects is a slice of objects.
type Objects []Object

func (o Objects) Prefix() string {
	var p Object
	for _, p = range o {
		break
	}
	return p.Prefix
}

// Bucket return the first bucket name from the first object.
func (o Objects) Bucket() string {
	var p Object
	for _, p = range o {
		break
	}
	return p.Bucket
}

// Prefixes returns all prefixes.
func (o Objects) Prefixes() []string {
	prefixes := make(set.StringSet, runtime.GOMAXPROCS(0))
	for _, p := range o {
		prefixes.Add(p.Prefix)
	}
	return prefixes.ToSlice()
}

// MergeObjectPrefixes merges prefixes from several slices of objects.
func MergeObjectPrefixes(o []Objects) []string {
	prefixes := make(set.StringSet, runtime.GOMAXPROCS(0))
	for _, objs := range o {
		for _, p := range objs {
			prefixes.Add(p.Prefix)
		}
	}
	return prefixes.ToSlice()
}

func (o *Object) setRandomSubPrefix(opts Options) {
	if opts.randomSubPrefix > 0 {
		b := make([]byte, opts.randomSubPrefix)
		rng := rand.New(rand.NewSource(int64(rand.Uint64())))
		randASCIIBytes(b, rng)
		o.Prefix = path.Join(o.Prefix, string(b))
	}
}

func (o *Object) setName(s string) {
	o.Name = path.Clean(o.Prefix + "/" + s)
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
