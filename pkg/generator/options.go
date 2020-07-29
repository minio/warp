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
	"math"
	"math/rand"
)

// Options provides options.
// Use WithXXX functions to set them.
type Options struct {
	src             func(o Options) (Source, error)
	totalSize       int64
	randSize        bool
	csv             CsvOpts
	random          RandomOpts
	prefix          string
	randomSubPrefix int
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
		src:             newRandom,
		totalSize:       1 << 20,
		csv:             csvOptsDefaults(),
		random:          randomOptsDefaults(),
		randomSubPrefix: 8,
	}
	return o
}

func WithPrefix(prefix string) Option {
	return func(o *Options) error {
		if prefix == "" {
			return errors.New("WithPrefix: prefix must not be empty")
		}
		o.prefix = prefix
		bucket, prefix := path2BucketPrefix(prefix)
		if bucket == "" {
			return errors.New("WithPrefix: bucket must not be empty")
		}
		if prefix == "" {
			return errors.New("WithPrefix: prefix must not be empty with bucket")
		}
		return nil
	}
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
