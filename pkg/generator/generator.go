package generator

import (
	"errors"
	"io"
)

// Option provides options for data generation.
// Use WithXXXX().Apply() to select data types and set options.
type Option func(o *Options) error

type Source interface {
	// Reader will return a reader that will return the number of requested bytes
	// and EOF on all subsequent calls.
	// Requesting a new reader will scramble data, so the new reader will not return the same data.
	// Requesting a reader is designed to be as lightweight as possible.
	// Only a single reader can be used concurrently.
	Reader() io.Reader

	// String returns a human readable description of the source.
	String() string
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
