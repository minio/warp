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
	"math"
	"math/rand"
)

// Options provides options.
// Use WithXXX functions to set them.
type Options struct {
	src          func(o Options) (Source, error)
	totalSize    int64
	randSize     bool
	csv          CsvOpts
	random       RandomOpts
	randomPrefix int
}

// OptionApplier allows to abstract generator options.
type OptionApplier interface {
	Apply() Option
}

// getSize will return a size for an object.
func (o Options) getSize(rng *rand.Rand) int64 {
	if !o.randSize {
		return o.totalSize
	}
	logSizeMax := math.Log2(float64(o.totalSize - 1))
	// Minimum size: 127 bytes, max scale is 256 times smaller than max size.
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

func defaultOptions() Options {
	o := Options{
		src:          newRandom,
		totalSize:    1 << 20,
		csv:          csvOptsDefaults(),
		random:       randomOptsDefaults(),
		randomPrefix: 0,
	}
	return o
}

// WithSize sets the size of the generated data.
func WithSize(n int64) Option {
	return func(o *Options) error {
		if n <= 0 {
			return errors.New("WithSize: size must be > 0")
		}
		if o.randSize && o.totalSize < 256 {
			return errors.New("WithSize: random sized objects should be at least 256 bytes")
		}

		o.totalSize = n
		return nil
	}
}

// WithRandomSize will randomize the size from 1 byte to the total size set.
func WithRandomSize(b bool) Option {
	return func(o *Options) error {
		if o.totalSize > 0 && o.totalSize < 256 {
			return errors.New("WithRandomSize: Random sized objects should be at least 256 bytes")
		}
		o.randSize = b
		return nil
	}
}

// WithPrefixSize sets prefix size.
func WithPrefixSize(n int) Option {
	return func(o *Options) error {
		if n < 0 {
			return errors.New("WithPrefixSize: size must be >= 0 and <= 16")
		}
		if n > 16 {
			return errors.New("WithPrefixSize: size must be >= 0 and <= 16")
		}
		o.randomPrefix = n
		return nil
	}
}
