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
	"path"

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
	// randSizeMode controls random-size behaviour:
	//   ""      – disabled (fixed size)
	//   "log2"  – legacy log₂ distribution (--obj.randsize / --obj.rand-log2)
	//   "logn"  – lognormal distribution   (--obj.rand-logn)
	randSizeMode  string
	randSizeSigma float64 // log-space sigma for lognormal; 0 means use default (1.0)

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
	switch o.randSizeMode {
	case "log2":
		return GetExpRandSize(rng, o.minSize, o.totalSize)
	case "logn":
		return GetLogNormalRandSize(rng, o.minSize, o.totalSize, o.randSizeSigma)
	default:
		return o.totalSize
	}
}

// GeneratePrefix returns a prefix string based on the configured options.
// If randomPrefix > 0, a random subdirectory of that length is added under customPrefix.
// Otherwise, customPrefix is returned as-is.
func (o Options) GeneratePrefix() string {
	if o.randomPrefix <= 0 {
		return o.customPrefix
	}
	b := make([]byte, o.randomPrefix)
	rng := rand.New(rand.NewSource(int64(rand.Uint64())))
	randASCIIBytes(b, rng)
	return path.Join(o.customPrefix, string(b))
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
		if o.randSizeMode != "" && maxSize < 256 {
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
		if o.randSizeMode != "" && o.totalSize < 256 {
			return errors.New("WithSize: random sized objects should be at least 256 bytes")
		}

		o.totalSize = n
		return nil
	}
}

// WithRandomSize sets the legacy log₂ random-size mode when b is true.
// Kept for backward compatibility; equivalent to WithRandomSizeMode("log2").
func WithRandomSize(b bool) Option {
	return func(o *Options) error {
		if b {
			if o.totalSize > 0 && o.totalSize < 256 {
				return errors.New("WithRandomSize: Random sized objects should be at least 256 bytes")
			}
			o.randSizeMode = "log2"
		}
		return nil
	}
}

// WithRandomSizeMode sets the random-size distribution:
//
//	"log2" – legacy log₂ distribution (equal count per doubling, upstream behaviour)
//	"logn" – lognormal distribution  (bell curve in log-space, realistic workloads)
//	""     – fixed size (no randomisation)
func WithRandomSizeMode(mode string) Option {
	return func(o *Options) error {
		switch mode {
		case "log2", "logn", "":
		default:
			return errors.New("WithRandomSizeMode: mode must be \"log2\", \"logn\", or \"\"")
		}
		if mode != "" && o.totalSize > 0 && o.totalSize < 256 {
			return errors.New("WithRandomSizeMode: random sized objects should be at least 256 bytes")
		}
		if mode != "" {
			o.randSizeMode = mode
		}
		return nil
	}
}

// WithRandomSizeSigma sets the log-space standard deviation for the lognormal size
// distribution (used with --obj.rand-logn). Pass 0 to use the default of 1.0.
func WithRandomSizeSigma(sigma float64) Option {
	return func(o *Options) error {
		if sigma < 0 {
			return errors.New("WithRandomSizeSigma: sigma must be >= 0")
		}
		o.randSizeSigma = sigma
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
