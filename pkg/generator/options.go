package generator

import "errors"

// Options provides options.
// Use WithXXX functions to set them.
type Options struct {
	src       func(o Options) (Source, error)
	totalSize int64
	csv       CsvOpts
	random    RandomOpts
}

func defaultOptions() Options {
	o := Options{
		src:       newRandom,
		totalSize: 1 << 20,
		csv:       csvOptsDefaults(),
		random:    randomOptsDefaults(),
	}
	return o
}

// WithSize sets the size of the generated data.
func WithSize(n int64) Option {
	return func(o *Options) error {
		if n < 0 {
			return errors.New("WithSize: size must be > 0")
		}
		o.totalSize = n
		return nil
	}
}