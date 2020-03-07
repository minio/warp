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
	"math"
	"sync"
	"time"

	"github.com/minio/minio-go/v6"
	"github.com/minio/minio/pkg/console"
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

	// Auto termination is set when this is > 0.
	AutoTermDur   time.Duration
	AutoTermScale float64

	// Default Put options.
	PutOpts minio.PutObjectOptions
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

// createEmptyBucket will create an empty bucket
// or delete all content if it already exists.
func (c *Common) createEmptyBucket(ctx context.Context) error {
	cl, done := c.Client()
	defer done()

	console.Infof("Creating Bucket %q...\n", c.Bucket)
	// In client mode someone else may have created it
	// first. Check if it exists now. We don't test
	// against a specific error since we might run
	// against many different servers.
	if err := cl.MakeBucket(c.Bucket, c.Location); err != nil {
		switch minio.ToErrorResponse(err).Code {
		case "BucketAlreadyOwnedByYou":
		case "BucketAlreadyExists":
		default:
			return err
		}
	}
	if c.Clear {
		console.Infof("Clearing Bucket %q...\n", c.Bucket)
		c.deleteAllInBucket(ctx)
	}
	return nil
}

// deleteAllInBucket will delete all content in a bucket.
// If no prefixes are specified everything in bucket is deleted.
func (c *Common) deleteAllInBucket(ctx context.Context, prefixes ...string) {
	doneCh := make(chan struct{})
	defer close(doneCh)
	cl, done := c.Client()
	defer done()
	if len(prefixes) == 0 {
		prefixes = []string{""}
	}
	var wg sync.WaitGroup
	remove := make(chan string, 1)
	errCh := cl.RemoveObjectsWithContext(ctx, c.Bucket, remove)
	wg.Add(len(prefixes))
	for _, prefix := range prefixes {
		go func(prefix string) {
			defer wg.Done()
			objects := cl.ListObjectsV2(c.Bucket, prefix, true, doneCh)
			for {
				select {
				case obj, ok := <-objects:
					if !ok {
						return
					}
					if obj.Err != nil {
						console.Error(obj.Err)
					}
					remove <- obj.Key
				case err := <-errCh:
					console.Error(err)
				}
			}
		}(prefix)
	}
	wg.Wait()
	// Signal we are done
	close(remove)
	// Wait for deletes to finish
	err := <-errCh
	if err.Err != nil {
		console.Error(err.Err)
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
