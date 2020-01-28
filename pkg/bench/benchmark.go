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

package bench

import (
	"context"
	"math"
	"time"

	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio-go/v6"
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
	x, err := cl.BucketExists(c.Bucket)
	if err != nil {
		return err
	}
	if !x {
		console.Infof("Creating Bucket %q...\n", c.Bucket)
		err := cl.MakeBucket(c.Bucket, c.Location)

		// In client mode someone else may have created it first.
		// Check if it exists now.
		// We don't test against a specific error since we might run against many different servers.
		if err != nil {
			x, err2 := cl.BucketExists(c.Bucket)
			if err2 != nil {
				return err2
			}
			if !x {
				// It still doesn't exits, return original error.
				return err
			}
		}
	}
	if c.Clear {
		console.Infof("Clearing Bucket %q...\n", c.Bucket)
		c.deleteAllInBucket(ctx)
	}
	return nil
}

// deleteAllInBucket will delete all content in a bucket.
func (c *Common) deleteAllInBucket(ctx context.Context) {
	doneCh := make(chan struct{})
	defer close(doneCh)
	cl, done := c.Client()
	defer done()
	objects := cl.ListObjectsV2(c.Bucket, "", true, doneCh)
	remove := make(chan string, 1)
	errCh := cl.RemoveObjectsWithContext(ctx, c.Bucket, remove)
	for {
		select {
		case obj, ok := <-objects:
			if !ok {
				close(remove)
				// Wait for deletes to finish
				err := <-errCh
				if err.Err != nil {
					console.Error(err.Err)
				}
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
