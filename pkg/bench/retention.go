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

// Retention benchmarks download speed.
type Retention struct {
	Common
	objects generator.Objects

	CreateObjects int
	Versions      int
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (g *Retention) Prepare(ctx context.Context) error {
	if err := g.createEmptyBucket(ctx); err != nil {
		return err
	}
	cl, done := g.Client()
	if !g.Versioned {
		err := cl.EnableVersioning(ctx, g.Bucket)
		if err != nil {
			return err
		}
		g.Versioned = true
		done()
	}

	src := g.Source()
	console.Eraseline()
	console.Info("\rUploading ", g.CreateObjects, " objects with ", g.Versions, " versions each of ", src.String())
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
				name := obj.Name
				for ver := 0; ver < g.Versions; ver++ {
					// New input for each version
					obj := src.Object()
					obj.Name = name
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
					g.prepareProgress(float64(len(g.objects)) / float64(g.CreateObjects*g.Versions))
					mu.Unlock()
					rcv <- op
				}
			}
		}(i)
	}
	wg.Wait()
	return groupErr
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (g *Retention) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
	var wg sync.WaitGroup
	wg.Add(g.Concurrency)
	c := g.Collector
	if g.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, http.MethodGet, g.AutoTermScale, autoTermCheck, autoTermSamples, g.AutoTermDur)
	}

	// Non-terminating context.
	nonTerm := context.Background()

	for i := 0; i < g.Concurrency; i++ {
		go func(i int) {
			rng := rand.New(rand.NewSource(int64(i)))
			rcv := c.Receiver()
			defer wg.Done()
			done := ctx.Done()
			var opts minio.PutObjectRetentionOptions

			<-wait
			mode := minio.Governance
			for {
				select {
				case <-done:
					return
				default:
				}

				if g.rpsLimit(ctx) != nil {
					return
				}

				obj := g.objects[rng.Intn(len(g.objects))]
				client, cldone := g.Client()
				op := Operation{
					OpType:   "RETENTION",
					Thread:   uint16(i),
					Size:     0,
					File:     obj.Name,
					ObjPerOp: 1,
					Endpoint: client.EndpointURL().String(),
				}

				op.Start = time.Now()
				opts.VersionID = obj.VersionID
				t := op.Start.Add(24 * time.Hour)
				opts.RetainUntilDate = &t
				opts.Mode = &mode
				opts.GovernanceBypass = true
				err := client.PutObjectRetention(nonTerm, g.Bucket, obj.Name, opts)
				if err != nil {
					g.Error("put retention error:", err)
					op.Err = err.Error()
					op.End = time.Now()
					rcv <- op
					cldone()
					continue
				}
				op.End = time.Now()
				rcv <- op
				cldone()
			}
		}(i)
	}
	wg.Wait()
	return c.Close(), nil
}

// Cleanup deletes everything uploaded to the bucket.
func (g *Retention) Cleanup(ctx context.Context) {
	g.deleteAllInBucket(ctx, g.objects.Prefixes()...)
}
