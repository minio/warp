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
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/generator"
)

// Get benchmarks download speed.
type Get struct {
	CreateObjects int
	RandomRanges  bool
	Collector     *Collector
	objects       generator.Objects
	Versions      int
	ListExisting  bool
	ListFlat      bool
	ListPrefix    string

	// Default Get options.
	GetOpts minio.GetObjectOptions
	Common
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (g *Get) Prepare(ctx context.Context) error {
	// prepare the bench by listing object from the bucket
	if g.ListExisting {
		cl, done := g.Client()

		// ensure the bucket exist
		found, err := cl.BucketExists(ctx, g.Bucket)
		if err != nil {
			return err
		}
		if !found {
			return (fmt.Errorf("bucket %s does not exist and --list-existing has been set", g.Bucket))
		}

		// list all objects
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		objectCh := cl.ListObjects(ctx, g.Bucket, minio.ListObjectsOptions{
			WithVersions: g.Versions > 1,
			Prefix:       g.ListPrefix,
			Recursive:    !g.ListFlat,
		})

		versions := map[string]int{}

		for object := range objectCh {
			if object.Err != nil {
				return object.Err
			}
			if object.Size == 0 {
				continue
			}
			obj := generator.Object{
				Name: object.Key,
				Size: object.Size,
			}

			if g.Versions > 1 {
				if object.VersionID == "" {
					continue
				}

				if version, found := versions[object.Key]; found {
					if version >= g.Versions {
						continue
					}
					versions[object.Key]++
				} else {
					versions[object.Key] = 1
				}
				obj.VersionID = object.VersionID
			}

			g.objects = append(g.objects, obj)

			// limit to ListingMaxObjects
			if g.CreateObjects > 0 && len(g.objects) >= g.CreateObjects {
				break
			}
		}
		if len(g.objects) == 0 {
			return (fmt.Errorf("no objects found for bucket %s", g.Bucket))
		}
		done()
		g.Collector = NewCollector()
		return nil
	}

	// prepare the bench by creating the bucket and pushing some objects
	if err := g.createEmptyBucket(ctx); err != nil {
		return err
	}
	if g.Versions > 1 {
		cl, done := g.Client()
		if !g.Versioned {
			err := cl.EnableVersioning(ctx, g.Bucket)
			if err != nil {
				return err
			}
			g.Versioned = true
		}
		done()
	}
	console.Eraseline()
	x := ""
	if g.Versions > 1 {
		x = fmt.Sprintf(" with %d versions each", g.Versions)
	}
	console.Info("\rUploading ", g.CreateObjects, " objects", x)

	var wg sync.WaitGroup
	wg.Add(g.Concurrency)
	g.Collector = NewCollector()
	obj := make(chan struct{}, g.CreateObjects)
	for i := 0; i < g.CreateObjects; i++ {
		obj <- struct{}{}
	}
	rcv := g.Collector.rcv
	close(obj)
	var groupErr error
	var mu sync.Mutex

	for i := 0; i < g.Concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			src := g.Source()
			opts := g.PutOpts

			for range obj {
				select {
				case <-ctx.Done():
					return
				default:
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

type firstByteRecorder struct {
	t *time.Time
	r io.Reader
}

func (f *firstByteRecorder) Read(p []byte) (n int, err error) {
	if f.t != nil || len(p) == 0 {
		return f.r.Read(p)
	}
	// Read a single byte.
	n, err = f.r.Read(p[:1])
	if n > 0 {
		t := time.Now()
		f.t = &t
	}
	return n, err
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (g *Get) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
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
			opts := g.GetOpts
			done := ctx.Done()

			<-wait
			for {
				select {
				case <-done:
					return
				default:
				}
				fbr := firstByteRecorder{}
				obj := g.objects[rng.Intn(len(g.objects))]
				client, cldone := g.Client()
				op := Operation{
					OpType:   http.MethodGet,
					Thread:   uint16(i),
					Size:     obj.Size,
					File:     obj.Name,
					ObjPerOp: 1,
					Endpoint: client.EndpointURL().String(),
				}
				if g.RandomRanges && op.Size > 2 {
					// Randomize length similar to --obj.randsize
					size := generator.GetExpRandSize(rng, 0, op.Size-2)
					start := rng.Int63n(op.Size - size)
					end := start + size
					op.Size = end - start + 1
					opts.SetRange(start, end)
				}
				op.Start = time.Now()
				var err error
				if g.Versions > 1 {
					opts.VersionID = obj.VersionID
				}
				o, err := client.GetObject(nonTerm, g.Bucket, obj.Name, opts)
				if err != nil {
					g.Error("download error:", err)
					op.Err = err.Error()
					op.End = time.Now()
					rcv <- op
					cldone()
					continue
				}
				fbr.r = o
				n, err := io.Copy(io.Discard, &fbr)
				if err != nil {
					g.Error("download error:", err)
					op.Err = err.Error()
				}
				op.FirstByte = fbr.t
				op.End = time.Now()
				if n != op.Size && op.Err == "" {
					op.Err = fmt.Sprint("unexpected download size. want:", op.Size, ", got:", n)
					g.Error(op.Err)
				}
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
func (g *Get) Cleanup(ctx context.Context) {
	if !g.ListExisting {
		g.deleteAllInBucket(ctx, g.objects.Prefixes()...)
	}
}
