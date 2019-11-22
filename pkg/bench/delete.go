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
	"math/rand"
	"sync"
	"time"

	"github.com/minio/mc/pkg/console"

	"github.com/minio/warp/pkg/generator"
)

// Get benchmarks download speed.
type Delete struct {
	CreateObjects int
	BatchSize     int
	Collector     *Collector
	objects       []generator.Object

	Common
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (d *Delete) Prepare(ctx context.Context) {
	d.createEmptyBucket(ctx)
	src := d.Source()
	console.Infoln("Uploading", d.CreateObjects, "Objects of", src.String())
	var wg sync.WaitGroup
	wg.Add(d.Concurrency)
	d.Collector = NewCollector()
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
				obj := src.Object()
				op := Operation{
					OpType:   "PUT",
					Thread:   uint16(i),
					Size:     obj.Size,
					File:     obj.Name,
					ObjPerOp: 1,
				}
				opts.ContentType = obj.ContentType
				op.Start = time.Now()
				n, err := d.Client.PutObject(d.Bucket, obj.Name, obj.Reader, obj.Size, opts)
				op.End = time.Now()
				if err != nil {
					console.Fatal("upload error:", err)
				}
				if n != obj.Size {
					console.Fatal("short upload. want:", obj.Size, ", got:", n)
				}
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
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (d *Delete) Start(ctx context.Context, start chan struct{}) Operations {
	var wg sync.WaitGroup
	wg.Add(d.Concurrency)
	c := d.Collector
	var mu sync.Mutex
	for i := 0; i < d.Concurrency; i++ {
		go func(i int) {
			rcv := c.Receiver()
			defer wg.Done()
			done := ctx.Done()

			<-start
			for {
				select {
				case <-done:
					return
				default:
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
				objects := make(chan string, len(objs))
				for _, obj := range objs {
					objects <- obj.Name
				}
				close(objects)

				op := Operation{
					OpType:   "DELETE",
					Thread:   uint16(i),
					Size:     0,
					File:     "",
					ObjPerOp: len(objs),
				}
				op.Start = time.Now()
				// RemoveObjectsWithContext will split any batches > 1000 into separate requests.
				errCh := d.Client.RemoveObjectsWithContext(context.Background(), d.Bucket, objects)

				// Wait for errCh to close.
				for {
					err, ok := <-errCh
					if !ok {
						break
					}
					if err.Err != nil {
						console.Errorln(err.Err)
						op.Err = err.Err.Error()
					}
				}
				op.End = time.Now()
				rcv <- op
			}
		}(i)
	}
	wg.Wait()
	return c.Close()
}

// Cleanup deletes everything uploaded to the bucket.
func (d *Delete) Cleanup(ctx context.Context) {
	d.deleteAllInBucket(ctx)
}
