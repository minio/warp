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

	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/pkg/generator"
)

// List benchmarks listing speed.
type List struct {
	CreateObjects int
	Collector     *Collector
	objects       []generator.Objects

	Common
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (d *List) Prepare(ctx context.Context) error {
	src := d.Source()
	objPerPrefix := d.CreateObjects / d.Concurrency
	console.Infoln("Uploading", objPerPrefix*d.Concurrency, "Objects of", src.String(), "with", d.Concurrency, "prefixes")
	var wg sync.WaitGroup
	wg.Add(d.Concurrency)
	d.Collector = NewCollector()
	d.objects = make([]generator.Objects, d.Concurrency)
	var mu sync.Mutex
	var groupErr error
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
					OpType:   http.MethodPut,
					Thread:   uint16(i),
					Size:     obj.Size,
					File:     obj.Name,
					ObjPerOp: 1,
					Endpoint: client.EndpointURL().String(),
				}
				opts.ContentType = obj.ContentType
				op.Start = time.Now()
				res, err := client.PutObject(ctx, obj.Bucket, obj.Name, obj.Reader, obj.Size, opts)
				op.End = time.Now()
				if err != nil {
					err := fmt.Errorf("upload error: %w", err)
					console.Error(err)
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
					console.Error(err)
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
	return groupErr
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (d *List) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
	var wg sync.WaitGroup
	wg.Add(d.Concurrency)
	c := d.Collector
	if d.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, "LIST", d.AutoTermScale, autoTermCheck, autoTermSamples, d.AutoTermDur)
	}
	// Non-terminating context.
	nonTerm := context.Background()

	for i := 0; i < d.Concurrency; i++ {
		go func(i int) {
			rcv := c.Receiver()
			defer wg.Done()
			done := ctx.Done()
			objs := d.objects[i]
			wantN := len(objs)

			<-wait
			for {
				select {
				case <-done:
					return
				default:
				}

				client, cldone := d.Client()
				op := Operation{
					File:     objs.Prefix(),
					OpType:   "LIST",
					Thread:   uint16(i),
					Size:     0,
					Endpoint: client.EndpointURL().String(),
				}
				op.Start = time.Now()

				// List all objects with prefix
				listCh := client.ListObjects(nonTerm, objs.Bucket(), minio.ListObjectsOptions{
					WithMetadata: true,
					Prefix:       objs.Prefix(),
				})

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
	return c.Close(), nil
}

// Cleanup deletes everything uploaded to the bucket.
func (d *List) Cleanup(ctx context.Context) {
	d.deleteAllInBucket(ctx, d.objects[0].Bucket(), generator.MergeObjectPrefixes(d.objects)...)
}
