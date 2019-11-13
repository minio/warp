/*
 * Warp (C) 2019- MinIO, Inc.
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
	"log"

	"github.com/minio/minio-go/v6"
	"github.com/minio/warp/pkg/generator"
)

type Benchmark interface {
	// Prepare for the benchmark run
	Prepare(ctx context.Context)

	// Start will execute the main benchmark.
	// Operations should begin executing when the start channel is closed.
	Start(ctx context.Context, sync chan struct{}) Operations

	// Clean up after the benchmark run.
	Cleanup(ctx context.Context)
}

// Common contains common benchmark parameters.
type Common struct {
	Client *minio.Client

	Concurrency int
	Source      func() generator.Source
	Bucket      string
	Location    string

	// Default Put options.
	PutOpts minio.PutObjectOptions
}

// createEmptyBucket will create an empty bucket
// or delete all content if it already exists.
func (c *Common) createEmptyBucket(ctx context.Context) {
	x, err := c.Client.BucketExists(c.Bucket)
	if err != nil {
		log.Fatal(err)
	}
	if !x {
		err = c.Client.MakeBucket(c.Bucket, c.Location)
		if err != nil {
			log.Fatal(err)
		}
		return
	}
	c.deleteAllInBucket(ctx)
}

// deleteAllInBucket will delete all content in a bucket.
func (c *Common) deleteAllInBucket(ctx context.Context) {
	doneCh := make(chan struct{})
	defer close(doneCh)
	objects := c.Client.ListObjectsV2(c.Bucket, "", true, doneCh)
	remove := make(chan string, 1)
	errCh := c.Client.RemoveObjectsWithContext(ctx, c.Bucket, remove)
	for {
		select {
		case obj, ok := <-objects:
			if !ok {
				close(remove)
				// Wait for deletes to finish
				err := <-errCh
				if err.Err != nil {
					log.Fatal(err.Err)
				}
				return
			}
			if obj.Err != nil {
				log.Fatal(obj.Err)
			}
			//log.Printf("Deleting: %+v", obj)
			remove <- obj.Key
		case err := <-errCh:
			log.Fatal(err)
		}
	}
}
