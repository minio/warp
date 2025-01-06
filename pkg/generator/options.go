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

	hist "github.com/jfsmig/prng/histogram"
)

// Options provides options.
// Use WithXXX functions to set them.
type Options struct {
	src          func(o Options) (Source, error)
	customPrefix string
	random       RandomOpts
	minSize      int64
	totalSize    int64
	randomPrefix int
	randSize     bool

	// Activates the use of a distribution of sizes
	flagSizesDistribution bool
	sizesDistribution     hist.Int64Distribution
}

// OptionApplier allows to abstract generator options.
type OptionApplier interface {
	Apply() Option
}

// getSize will return a size for an object.
func (o Options) getSize(rng *rand.Rand) int64 {
	if o.flagSizesDistribution {
		return o.sizesDistribution.Poll(rng)
	}
	if !o.randSize {
		return o.totalSize
	}
	return GetExpRandSize(rng, o.minSize, o.totalSize)
}

func defaultOptions() Options {
	o := Options{
		src:          newRandom,
		totalSize:    1 << 20,
		random:       randomOptsDefaults(),
		randomPrefix: 0,
	}
	return o
}

func WithSizeHistograms(encoded string) Option {
	return func(o *Options) error {
		var err error
		o.sizesDistribution, err = hist.ParseCSV(encoded)
		if err != nil {
			return err
		}
		o.flagSizesDistribution = true
		return nil
	}
}

// WithMinMaxSize sets the min and max size of the generated data.
func WithMinMaxSize(minSize, maxSize int64) Option {
	return func(o *Options) error {
		if minSize <= 0 {
			return errors.New("WithMinMaxSize: minSize must be >= 0")
		}
		if maxSize < 0 {
			return errors.New("WithMinMaxSize: maxSize must be > 0")
		}
		if minSize > maxSize {
			return errors.New("WithMinMaxSize: minSize must be < maxSize")
		}
		if o.randSize && maxSize < 256 {
			return errors.New("WithMinMaxSize: random sized objects should be at least 256 bytes")
		}

		o.totalSize = maxSize
		o.minSize = minSize
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
		if b && o.totalSize > 0 && o.totalSize < 256 {
			return errors.New("WithRandomSize: Random sized objects should be at least 256 bytes")
		}
		o.randSize = b
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
