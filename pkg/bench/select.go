/*
 * Warp (C) 2020 MinIO, Inc.
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
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/v2/console"
	"github.com/minio/warp/pkg/generator"
)

// Select benchmarks download speed.
type Select struct {
	Common

	// Default Select options.
	SelectOpts minio.SelectObjectOptions
	objects    generator.Objects

	CreateObjects int
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (g *Select) Prepare(ctx context.Context) error {
	if err := g.createEmptyBucket(ctx); err != nil {
		return err
	}
	src := g.Source()
	console.Eraseline()
	console.Info("\rUploading ", g.CreateObjects, " objects of ", src.String())
	var wg sync.WaitGroup
	wg.Add(g.Concurrency)
	g.addCollector()
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

				if g.rpsLimit(ctx) != nil {
					return
				}

				obj := src.Object()
				client, cldone := g.Client()
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
				res, err := client.PutObject(ctx, g.Bucket, obj.Name, obj.Reader, obj.Size, opts)
				op.End = time.Now()
				if err != nil {
					err := fmt.Errorf("upload error: %w", err)
					g.Error(err)
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
					g.Error(err)
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
func (g *Select) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
	var wg sync.WaitGroup
	wg.Add(g.Concurrency)
	c := g.Collector
	if g.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, "SELECT", g.AutoTermScale, autoTermCheck, autoTermSamples, g.AutoTermDur)
	}

	// Non-terminating context.
	nonTerm := context.Background()

	for i := 0; i < g.Concurrency; i++ {
		go func(i int) {
			rng := rand.New(rand.NewSource(int64(i)))
			rcv := c.Receiver()
			defer wg.Done()
			opts := g.SelectOpts
			done := ctx.Done()

			<-wait
			for {
				select {
				case <-done:
					return
				default:
				}

				if g.rpsLimit(ctx) != nil {
					return
				}

				fbr := firstByteRecorder{}
				obj := g.objects[rng.Intn(len(g.objects))]
				client, cldone := g.Client()
				op := Operation{
					OpType:   "SELECT",
					Thread:   uint16(i),
					Size:     obj.Size,
					File:     obj.Name,
					ObjPerOp: 1,
					Endpoint: client.EndpointURL().String(),
				}

				op.Start = time.Now()
				var err error
				o, err := client.SelectObjectContent(nonTerm, g.Bucket, obj.Name, opts)
				fbr.r = o
				if err != nil {
					g.Error("download error: ", err)
					op.Err = err.Error()
					op.End = time.Now()
					rcv <- op
					cldone()
					continue
				}
				if _, err = io.Copy(io.Discard, &fbr); err != nil {
					g.Error("download error: ", err)
					op.Err = err.Error()
					op.Size = 0
				}
				op.FirstByte = fbr.t
				op.End = time.Now()
				rcv <- op
				cldone()
				o.Close()
			}
		}(i)
	}
	wg.Wait()
	return c.Close(), nil
}

// Cleanup deletes everything uploaded to the bucket.
func (g *Select) Cleanup(ctx context.Context) {
	g.deleteAllInBucket(ctx, g.objects.Prefixes()...)
}
