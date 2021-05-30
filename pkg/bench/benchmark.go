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
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/generator"
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
	Client func() (cl *minio.Client, done func())

	Concurrency int
	Source      func() generator.Source
	Bucket      string
	Location    string

	// Running in client mode.
	ClientMode bool
	// Clear bucket before benchmark
	Clear           bool
	PrepareProgress chan float64
	// Does destination support versioning?
	Versioned bool

	// Auto termination is set when this is > 0.
	AutoTermDur   time.Duration
	AutoTermScale float64

	// Default Put options.
	PutOpts minio.PutObjectOptions

	// Error should log an error similar to fmt.Print(data...)
	Error func(data ...interface{})
}

const (
	// Split active ops into this many segments.
	autoTermSamples = 25

	// Number of segments that must be within limit.
	// The last segment will be the one considered 'current speed'.
	autoTermCheck = 7
)

func (c *Common) GetCommon() *Common {
	return c
}

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

	if !x {
		console.Infof("\rCreating Bucket %q...", c.Bucket)
		err := cl.MakeBucket(ctx, c.Bucket, minio.MakeBucketOptions{
			Region: c.Location,
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
	var wg sync.WaitGroup
	wg.Add(len(prefixes))
	for _, prefix := range prefixes {
		go func(prefix string) {
			defer wg.Done()

			doneCh := make(chan struct{})
			defer close(doneCh)
			cl, done := c.Client()
			defer done()
			remove := make(chan minio.ObjectInfo, 1000)
			errCh := cl.RemoveObjects(ctx, c.Bucket, remove, minio.RemoveObjectsOptions{})
			defer func() {
				// Signal we are done
				close(remove)
				// Wait for deletes to finish
				err := <-errCh
				if err.Err != nil {
					c.Error(err.Err)
				}
			}()

			objects := cl.ListObjects(ctx, c.Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true, WithVersions: c.Versioned})
			for {
				select {
				case obj, ok := <-objects:
					if !ok {
						return
					}
					if obj.Err != nil {
						c.Error(obj.Err)
						continue
					}
				sendNext:
					for {
						select {
						case remove <- minio.ObjectInfo{
							Key:       obj.Key,
							VersionID: obj.VersionID,
						}:
							break sendNext
						case err := <-errCh:
							c.Error(err)
						}
					}
				case err := <-errCh:
					c.Error(err)
				}
			}
		}(prefix)
	}
	wg.Wait()

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
