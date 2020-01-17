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
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio-go/v6"
	"github.com/minio/warp/pkg/generator"
)

// Stat benchmarks download speed.
type Stat struct {
	CreateObjects int
	Collector     *Collector
	objects       []generator.Object

	// Default Stat options.
	StatOpts minio.StatObjectOptions
	Common
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (g *Stat) Prepare(ctx context.Context) error {
	if err := g.createEmptyBucket(ctx); err != nil {
		return err
	}
	src := g.Source()
	console.Infoln("Uploading", g.CreateObjects, "Objects of", src.String())
	var wg sync.WaitGroup
	wg.Add(g.Concurrency)
	g.Collector = NewCollector()
	obj := make(chan struct{}, g.CreateObjects)
	for i := 0; i < g.CreateObjects; i++ {
		obj <- struct{}{}
	}
	close(obj)
	var groupErr error
	var mu sync.Mutex
	for i := 0; i < g.Concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			src := g.Source()
			for range obj {
				opts := g.PutOpts
				rcv := g.Collector.Receiver()
				done := ctx.Done()

				select {
				case <-done:
					return
				default:
				}
				obj := src.Object()
				client, cldone := g.Client()
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
				n, err := client.PutObject(g.Bucket, obj.Name, obj.Reader, obj.Size, opts)
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
				if n != obj.Size {
					err := fmt.Errorf("short upload. want: %d, got %d", obj.Size, n)
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
				g.objects = append(g.objects, *obj)
				g.prepareProgress(float64(len(g.objects)) / float64(g.CreateObjects))
				mu.Unlock()
				rcv <- op
			}
		}(i)
	}
	wg.Wait()
	return groupErr
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (g *Stat) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
	var wg sync.WaitGroup
	wg.Add(g.Concurrency)
	c := g.Collector
	if g.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, "STAT", g.AutoTermScale, autoTermCheck, autoTermSamples, g.AutoTermDur)
	}
	for i := 0; i < g.Concurrency; i++ {
		go func(i int) {
			rng := rand.New(rand.NewSource(int64(i)))
			rcv := c.Receiver()
			defer wg.Done()
			opts := g.StatOpts
			done := ctx.Done()

			<-wait
			for {
				select {
				case <-done:
					return
				default:
				}
				obj := g.objects[rng.Intn(len(g.objects))]
				client, cldone := g.Client()
				op := Operation{
					OpType:   "STAT",
					Thread:   uint16(i),
					Size:     0,
					File:     obj.Name,
					ObjPerOp: 1,
					Endpoint: client.EndpointURL().String(),
				}
				op.Start = time.Now()
				var err error
				objI, err := client.StatObject(g.Bucket, obj.Name, opts)
				if err != nil {
					console.Errorln("StatObject error:", err)
					op.Err = err.Error()
					op.End = time.Now()
					rcv <- op
					cldone()
					continue
				}
				op.End = time.Now()
				if objI.Size != obj.Size && op.Err == "" {
					op.Err = fmt.Sprint("unexpected file size. want:", obj.Size, ", got:", objI.Size)
					console.Errorln(op.Err)
				}
				rcv <- op
				cldone()
			}
		}(i)
	}
	wg.Wait()
	return c.Close(), nil
}

// Cleanup deletes everything uploaded to the bucket.
func (g *Stat) Cleanup(ctx context.Context) {
	g.deleteAllInBucket(ctx)
}
