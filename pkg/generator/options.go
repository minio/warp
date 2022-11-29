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
	"math/rand"
)

// Options provides options.
// Use WithXXX functions to set them.
type Options struct {
	src          func(o Options) (Source, error)
	totalSize    int64
	randSize     bool
	dist         []int64
	customPrefix string
	csv          CsvOpts
	random       RandomOpts
	text         TextOpts
	randomPrefix int
	compRatio    int
}

// OptionApplier allows to abstract generator options.
type OptionApplier interface {
	Apply() Option
}

// getSize will return a size for an object.
func (o Options) getSize(rng *rand.Rand) int64 {
	if o.randSize {
		return GetExpRandSize(rng, o.totalSize)
	} else if len(o.dist) > 0 {
		return GetDistributionSize(rng, o.dist)
	}
	return o.totalSize
}

func defaultOptions() Options {
	o := Options{
		src:          newRandom,
		totalSize:    1 << 20,
		csv:          csvOptsDefaults(),
		random:       randomOptsDefaults(),
		text:         textOptsDefaults(),
		randomPrefix: 0,
	}
	return o
}

const MIN_RAND_SIZE = 256

// WithSize sets the size of the generated data.
func WithSize(n int64) Option {
	return func(o *Options) error {
		if n <= 0 {
			return errors.New("WithSize: size must be > 0")
		}
		if o.randSize && o.totalSize < MIN_RAND_SIZE {
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

// WithSizeDistribution will distribute the size based on the provided specification.
func WithSizeDistribution(dist []int64) Option {
	return func(o *Options) error {
		o.dist = dist
		return nil
	}
}

// WithCustomPrefix adds custom prefix under bucket where all warp content is created.
func WithCustomPrefix(prefix string) Option {
	return func(o *Options) error {
		o.customPrefix = prefix
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

// WithCompression sets the compression ratio.
func WithCompression(compRatio int) Option {
	return func(o *Options) error {
		o.compRatio = compRatio
		return nil
	}
}