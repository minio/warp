/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package generator

import (
	"errors"
	"io"
	"math/rand"
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
}

type Object struct {
	// Reader will return a reader that will return the number of requested bytes
	// and EOF on all subsequent calls.
	Reader io.Reader

	// A random generated name.
	Name string

	// Corresponding mime type
	ContentType string

	// Size of the object to expect.
	Size int64

	PreFix string
}

func (o *Object) setPrefix(opts Options) {
	if opts.randomPrefix <= 0 {
		o.PreFix = ""
		return
	}
	b := make([]byte, opts.randomPrefix)
	rng := rand.New(rand.NewSource(int64(rand.Uint64())))
	randAsciiBytes(b, rng)
	o.PreFix = string(b)
}

func (o *Object) setName(s string) {
	if len(o.PreFix) == 0 {
		o.Name = s
		return
	}
	o.Name = o.PreFix + "/" + s
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

// randAsciiBytes fill destination with pseudorandom ASCII characters [a-ZA-Z0-9].
// Should never be considered for true random data generation.
func randAsciiBytes(dst []byte, rng *rand.Rand) {
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
