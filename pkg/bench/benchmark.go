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

package bench

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/v2/console"
	"github.com/minio/warp/pkg/generator"

	"golang.org/x/time/rate"
)

type Benchmark interface {
	// Prepare for the benchmark run
	Prepare(ctx context.Context) error

	// Start will execute the main benchmark.
	// Operations should begin executing when the start channel is closed.
	Start(ctx context.Context, wait chan struct{}) (Operations, error)

	// Clean up after the benchmark run.
	Cleanup(ctx context.Context)

	// Common returns the common parameters.
	GetCommon() *Common
}

// Common contains common benchmark parameters.
type Common struct {
	// Default Put options.
	PutOpts minio.PutObjectOptions

	PrepareProgress chan float64

	// Custom is returned to server if set by clients.
	Custom map[string]string

	// ExtraFlags contains extra flags to add to remote clients.
	ExtraFlags map[string]string
	Source     func() generator.Source
	ExtraOut   []chan<- Operation

	// Error should log an error similar to fmt.Print(data...)
	Error func(data ...interface{})

	Client func() (cl *minio.Client, done func())

	Collector *Collector

	Location string
	Bucket   string

	// Auto termination is set when this is > 0.
	AutoTermDur time.Duration

	// ClientIdx is the client index.
	// Will be 0 if single client.
	ClientIdx int

	AutoTermScale float64

	Concurrency int

	// Running in client mode.
	ClientMode bool
	Locking    bool

	// Clear bucket before benchmark
	Clear bool

	// DiscardOutput output.
	DiscardOutput bool // indicates if we prefer a terse output useful in lengthy runs

	// Does destination support versioning?
	Versioned bool

	// ratelimiting
	RpsLimiter *rate.Limiter
}

const (
	// Split active ops into this many segments.
	autoTermSamples = 25

	// Number of segments that must be within limit.
	// The last segment will be the one considered 'current speed'.
	autoTermCheck = 7
)

// GetCommon implements interface compatible implementation
func (c *Common) GetCommon() *Common {
	return c
}

// ErrorF formatted error printer
func (c *Common) ErrorF(format string, data ...interface{}) {
	c.Error(fmt.Sprintf(format, data...))
}

// createEmptyBucket will create an empty bucket
// or delete all content if it already exists.
func (c *Common) createEmptyBucket(ctx context.Context) error {
	cl, done := c.Client()
	defer done()
	x, err := cl.BucketExists(ctx, c.Bucket)
	if err != nil {
		return err
	}

	if x && c.Locking {
		_, _, _, err := cl.GetBucketObjectLockConfig(ctx, c.Bucket)
		if err != nil {
			if !c.Clear {
				return errors.New("not allowed to clear bucket to re-create bucket with locking")
			}
			if bvc, err := cl.GetBucketVersioning(ctx, c.Bucket); err == nil {
				c.Versioned = bvc.Status == "Enabled"
			}
			console.Eraseline()
			console.Infof("\rClearing Bucket %q to enable locking...", c.Bucket)
			c.deleteAllInBucket(ctx)
			err = cl.RemoveBucket(ctx, c.Bucket)
			if err != nil {
				return err
			}
			// Recreate bucket.
			x = false
		}
	}

	if !x {
		console.Eraseline()
		console.Infof("\rCreating Bucket %q...", c.Bucket)
		err := cl.MakeBucket(ctx, c.Bucket, minio.MakeBucketOptions{
			Region:        c.Location,
			ObjectLocking: c.Locking,
		})
		// In client mode someone else may have created it first.
		// Check if it exists now.
		// We don't test against a specific error since we might run against many different servers.
		if err != nil {
			x, err2 := cl.BucketExists(ctx, c.Bucket)
			if err2 != nil {
				return err2
			}
			if !x {
				// It still doesn't exits, return original error.
				return err
			}
		}
	}
	if bvc, err := cl.GetBucketVersioning(ctx, c.Bucket); err == nil {
		c.Versioned = bvc.Status == "Enabled"
	}

	if c.Clear {
		console.Eraseline()
		console.Infof("\rClearing Bucket %q...", c.Bucket)
		c.deleteAllInBucket(ctx)
	}
	return nil
}

// deleteAllInBucket will delete all content in a bucket.
// If no prefixes are specified everything in bucket is deleted.
func (c *Common) deleteAllInBucket(ctx context.Context, prefixes ...string) {
	if len(prefixes) == 0 {
		prefixes = []string{""}
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	cl, done := c.Client()
	defer done()

	objectsCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(objectsCh)
		opts := minio.ListObjectsOptions{
			Recursive:    true,
			WithVersions: c.Versioned,
		}
		for _, prefix := range prefixes {
			opts.Prefix = prefix
			if prefix != "" {
				opts.Prefix = prefix + "/"
			}
			for object := range cl.ListObjects(ctx, c.Bucket, opts) {
				if object.Err != nil {
					c.Error(object.Err)
					return
				}
				objectsCh <- object
			}
			console.Eraseline()
			console.Infof("\rClearing Prefix %q...", strings.Join([]string{c.Bucket, opts.Prefix}, "/"))
		}
	}()

	delOpts := minio.RemoveObjectsOptions{}
	_, _, _, errLock := cl.GetBucketObjectLockConfig(ctx, c.Bucket)
	if errLock == nil {
		delOpts.GovernanceBypass = true
	}

	errCh := cl.RemoveObjects(ctx, c.Bucket, objectsCh, delOpts)
	for err := range errCh {
		if err.Err != nil {
			c.Error(err.Err)
			continue
		}
	}
}

// prepareProgress updates preparation progess with the value 0->1.
func (c *Common) prepareProgress(progress float64) {
	if c.PrepareProgress == nil {
		return
	}
	progress = math.Max(0, math.Min(1, progress))
	select {
	case c.PrepareProgress <- progress:
	default:
	}
}

func (c *Common) addCollector() {
	if c.DiscardOutput {
		c.Collector = NewNullCollector()
	} else {
		c.Collector = NewCollector()
	}
	c.Collector.extra = c.ExtraOut
}

func (c *Common) rpsLimit(ctx context.Context) error {
	if c.RpsLimiter == nil {
		return nil
	}

	return c.RpsLimiter.Wait(ctx)
}
