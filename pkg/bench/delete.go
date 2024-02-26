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
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/v2/console"
	"github.com/minio/warp/pkg/generator"
)

// Delete benchmarks delete speed.
type Delete struct {
	Common
	objects generator.Objects

	CreateObjects int
	BatchSize     int
	ListExisting  bool
	ListFlat      bool
	ListPrefix    string
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (d *Delete) Prepare(ctx context.Context) error {
	var groupErr error

	// prepare the bench by listing object from the bucket
	if d.ListExisting {
		cl, done := d.Client()

		// ensure the bucket exist
		found, err := cl.BucketExists(ctx, d.Bucket)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("bucket %s does not exist and --list-existing has been set", d.Bucket)
		}

		// list all objects
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		objectCh := cl.ListObjects(ctx, d.Bucket, minio.ListObjectsOptions{
			Prefix:    d.ListPrefix,
			Recursive: !d.ListFlat,
		})

		for object := range objectCh {
			if object.Err != nil {
				return object.Err
			}
			obj := generator.Object{
				Name: object.Key,
				Size: object.Size,
			}

			d.objects = append(d.objects, obj)

			// limit to ListingMaxObjects
			if d.CreateObjects > 0 && len(d.objects) >= d.CreateObjects {
				break
			}
		}
		if len(d.objects) == 0 {
			return (fmt.Errorf("no objects found for bucket %s", d.Bucket))
		}
		done()
		d.Collector = NewCollector()

		// Shuffle objects.
		// Benchmark will pick from slice in order.
		a := d.objects
		rand.Shuffle(len(a), func(i, j int) {
			a[i], a[j] = a[j], a[i]
		})
		return groupErr
	}

	if err := d.createEmptyBucket(ctx); err != nil {
		return err
	}
	src := d.Source()
	console.Eraseline()
	console.Info("\rUploading ", d.CreateObjects, " objects of ", src.String())
	var wg sync.WaitGroup
	wg.Add(d.Concurrency)
	d.addCollector()
	obj := make(chan struct{}, d.CreateObjects)
	for i := 0; i < d.CreateObjects; i++ {
		obj <- struct{}{}
	}
	close(obj)
	var mu sync.Mutex
	for i := 0; i < d.Concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			src := d.Source()

			for range obj {
				opts := d.PutOpts
				rcv := d.Collector.Receiver()
				done := ctx.Done()

				select {
				case <-done:
					return
				default:
				}

				if d.rpsLimit(ctx) != nil {
					return
				}

				obj := src.Object()
				client, cldone := d.Client()
				op := Operation{
					OpType:   http.MethodPut,
					Thread:   uint16(i),
					Size:     obj.Size,
					File:     obj.Name,
					ObjPerOp: 1,
					Endpoint: client.EndpointURL().String(),
				}

				opts.ContentType = obj.ContentType
				op.Start = time.Now()
				res, err := client.PutObject(ctx, d.Bucket, obj.Name, obj.Reader, obj.Size, opts)
				op.End = time.Now()
				if err != nil {
					err := fmt.Errorf("upload error: %w", err)
					d.Error(err)
					mu.Lock()
					if groupErr == nil {
						groupErr = err
					}
					mu.Unlock()
					return
				}
				obj.VersionID = res.VersionID

				if res.Size != obj.Size {
					err := fmt.Errorf("short upload. want: %d, got %d", obj.Size, res.Size)
					d.Error(err)
					mu.Lock()
					if groupErr == nil {
						groupErr = err
					}
					mu.Unlock()
					return
				}
				cldone()
				mu.Lock()
				obj.Reader = nil
				d.objects = append(d.objects, *obj)
				d.prepareProgress(float64(len(d.objects)) / float64(d.CreateObjects))
				mu.Unlock()
				rcv <- op
			}
		}(i)
	}
	wg.Wait()

	// Shuffle objects.
	// Benchmark will pick from slice in order.
	a := d.objects
	rand.Shuffle(len(a), func(i, j int) {
		a[i], a[j] = a[j], a[i]
	})
	return groupErr
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (d *Delete) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
	var wg sync.WaitGroup
	wg.Add(d.Concurrency)
	c := d.Collector
	if d.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, http.MethodDelete, d.AutoTermScale, autoTermCheck, autoTermSamples, d.AutoTermDur)
	}
	// Non-terminating context.
	nonTerm := context.Background()

	var mu sync.Mutex
	for i := 0; i < d.Concurrency; i++ {
		go func(i int) {
			rcv := c.Receiver()
			defer wg.Done()
			done := ctx.Done()

			<-wait
			for {
				select {
				case <-done:
					return
				default:
				}

				if d.rpsLimit(ctx) != nil {
					return
				}

				// Fetch d.BatchSize objects
				mu.Lock()
				if len(d.objects) == 0 {
					mu.Unlock()
					return
				}
				objs := d.objects
				if len(objs) > d.BatchSize {
					objs = objs[:d.BatchSize]
				}
				d.objects = d.objects[len(objs):]
				mu.Unlock()

				// Queue all in batch.
				objects := make(chan minio.ObjectInfo, len(objs))
				for _, obj := range objs {
					objects <- minio.ObjectInfo{Key: obj.Name, VersionID: obj.VersionID}
				}
				close(objects)

				client, cldone := d.Client()
				op := Operation{
					OpType:   http.MethodDelete,
					Thread:   uint16(i),
					Size:     0,
					File:     "",
					ObjPerOp: len(objs),
					Endpoint: client.EndpointURL().String(),
				}
				if d.DiscardOutput {
					op.File = ""
				}

				op.Start = time.Now()
				// RemoveObjectsWithContext will split any batches > 1000 into separate requests.
				errCh := client.RemoveObjects(nonTerm, d.Bucket, objects, minio.RemoveObjectsOptions{})

				// Wait for errCh to close.
				for {
					err, ok := <-errCh
					if !ok {
						break
					}
					if err.Err != nil {
						d.Error(err.Err)
						op.Err = err.Err.Error()
					}
				}
				op.End = time.Now()
				cldone()
				rcv <- op
			}
		}(i)
	}
	wg.Wait()
	return c.Close(), nil
}

// Cleanup deletes everything uploaded to the bucket.
func (d *Delete) Cleanup(ctx context.Context) {
	if len(d.objects) > 0 && !d.ListExisting {
		d.deleteAllInBucket(ctx, d.objects.Prefixes()...)
	}
}
