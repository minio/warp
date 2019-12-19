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
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/minio/mc/pkg/console"
	"github.com/minio/warp/pkg/generator"
)

// Get benchmarks download speed.
type List struct {
	CreateObjects int
	NoPrefix      bool
	Collector     *Collector
	objects       [][]generator.Object

	Common
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (d *List) Prepare(ctx context.Context) {
	d.createEmptyBucket(ctx)
	src := d.Source()
	objPerPrefix := d.CreateObjects / d.Concurrency
	if d.NoPrefix {
		console.Infoln("Uploading", objPerPrefix*d.Concurrency, "Objects of", src.String(), "with no prefixes")
	} else {
		console.Infoln("Uploading", objPerPrefix*d.Concurrency, "Objects of", src.String(), "with", d.Concurrency, "prefixes")
	}
	var wg sync.WaitGroup
	wg.Add(d.Concurrency)
	d.Collector = NewCollector()
	d.objects = make([][]generator.Object, d.Concurrency)
	var mu sync.Mutex
	for i := 0; i < d.Concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			src := d.Source()
			opts := d.PutOpts
			rcv := d.Collector.Receiver()
			done := ctx.Done()
			exists := make(map[string]struct{}, objPerPrefix)

			for j := 0; j < objPerPrefix; j++ {
				select {
				case <-done:
					return
				default:
				}
				obj := src.Object()
				// Assure we don't have duplicates
				for {
					if _, ok := exists[obj.Name]; ok {
						obj = src.Object()
						continue
					}
					break
				}
				exists[obj.Name] = struct{}{}
				client, cldone := d.Client()
				op := Operation{
					OpType:   "PUT",
					Thread:   uint16(i),
					Size:     obj.Size,
					File:     obj.Name,
					ObjPerOp: 1,
					Endpoint: client.EndpointURL().String(),
				}
				opts.ContentType = obj.ContentType
				op.Start = time.Now()
				n, err := client.PutObject(d.Bucket, obj.Name, obj.Reader, obj.Size, opts)
				op.End = time.Now()
				if err != nil {
					console.Fatal("upload error:", err)
				}
				if n != obj.Size {
					console.Fatal("short upload. want:", obj.Size, ", got:", n)
				}
				cldone()
				mu.Lock()
				obj.Reader = nil
				d.objects[i] = append(d.objects[i], *obj)
				d.prepareProgress(float64(len(d.objects)) / float64(objPerPrefix*d.Concurrency))
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
func (d *List) Start(ctx context.Context, start chan struct{}) Operations {
	var wg sync.WaitGroup
	wg.Add(d.Concurrency)
	c := d.Collector
	for i := 0; i < d.Concurrency; i++ {
		go func(i int) {
			rcv := c.Receiver()
			defer wg.Done()
			done := ctx.Done()
			objs := d.objects[i]
			wantN := len(objs)
			if d.NoPrefix {
				wantN *= d.Concurrency
			}

			<-start
			for {
				select {
				case <-done:
					return
				default:
				}

				prefix := objs[0].PreFix
				client, cldone := d.Client()
				op := Operation{
					File:     prefix,
					OpType:   "LIST",
					Thread:   uint16(i),
					Size:     0,
					Endpoint: client.EndpointURL().String(),
				}
				op.Start = time.Now()

				// List all objects with prefix
				listCh := client.ListObjectsV2(d.Bucket, objs[0].PreFix, true, nil)

				// Wait for errCh to close.
				for {
					err, ok := <-listCh
					if !ok {
						break
					}
					if err.Err != nil {
						console.Errorln(err.Err)
						op.Err = err.Err.Error()
					}
					op.ObjPerOp++
					if op.FirstByte == nil {
						now := time.Now()
						op.FirstByte = &now
					}
				}
				if op.ObjPerOp != wantN {
					if op.Err == "" {
						op.Err = fmt.Sprintf("Unexpected object count, want %d, got %d", wantN, op.ObjPerOp)
					}
				}
				op.End = time.Now()
				cldone()
				rcv <- op
			}
		}(i)
	}
	wg.Wait()
	return c.Close()
}

// Cleanup deletes everything uploaded to the bucket.
func (d *List) Cleanup(ctx context.Context) {
	d.deleteAllInBucket(ctx)
}
