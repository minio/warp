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
	"errors"
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

// Mixed benchmarks mixed operations all inclusive.
type Mixed struct {
	Common
	Dist *MixedDistribution

	GetOpts       minio.GetObjectOptions
	StatOpts      minio.StatObjectOptions
	CreateObjects int
}

// MixedDistribution keeps track of operation distribution
// and currently available objects.
type MixedDistribution struct {
	// Operation -> distribution.
	Distribution map[string]float64
	objects      map[string]generator.Object
	rng          *rand.Rand

	ops []string

	current int
	mu      sync.Mutex
}

func (m *MixedDistribution) Generate(allocObjs int) error {
	if m.Distribution[http.MethodDelete] > m.Distribution[http.MethodPut] {
		return errors.New("DELETE distribution cannot be bigger than PUT")
	}
	m.objects = make(map[string]generator.Object, allocObjs)

	err := m.normalize()
	if err != nil {
		return err
	}

	const genOps = 1000
	m.ops = make([]string, 0, genOps)
	for op, dist := range m.Distribution {
		add := int(0.5 + dist*genOps)
		for i := 0; i < add; i++ {
			m.ops = append(m.ops, op)
		}
	}
	m.rng = rand.New(rand.NewSource(0xabad1dea))
	m.rng.Shuffle(len(m.ops), func(i, j int) {
		m.ops[i], m.ops[j] = m.ops[j], m.ops[i]
	})
	return nil
}

func (m *MixedDistribution) Objects() generator.Objects {
	res := make(generator.Objects, 0, len(m.objects))
	for _, v := range m.objects {
		res = append(res, v)
	}
	return res
}

func (m *MixedDistribution) normalize() error {
	total := 0.0
	for op, dist := range m.Distribution {
		if dist < 0 {
			return fmt.Errorf("negative distribution requested for op %q", op)
		}
		total += dist
	}
	if total == 0 {
		return fmt.Errorf("no distribution set, total is 0")
	}
	for op, dist := range m.Distribution {
		m.Distribution[op] = dist / total
	}
	return nil
}

func (m *MixedDistribution) randomObj() (obj generator.Object, done func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Use map randomness to select.
	for k, o := range m.objects {
		delete(m.objects, k)
		return o, func() {
			m.mu.Lock()
			m.objects[k] = obj
			m.mu.Unlock()
		}
	}
	panic("ran out of objects")
}

func (m *MixedDistribution) deleteRandomObj() generator.Object {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Use map randomness to select.
	for k, o := range m.objects {
		delete(m.objects, k)
		return o
	}
	panic("ran out of objects")
}

func (m *MixedDistribution) addObj(o generator.Object) {
	m.mu.Lock()
	m.objects[o.Name] = o
	m.mu.Unlock()
}

func (m *MixedDistribution) getOp() string {
	m.mu.Lock()
	op := m.ops[m.current]
	m.current = (m.current + 1) % len(m.ops)
	m.mu.Unlock()
	return op
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (g *Mixed) Prepare(ctx context.Context) error {
	if g.CreateObjects <= g.Concurrency {
		return errors.New("initial number of objects should be at least matching concurrency")
	}
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
		go func() {
			defer wg.Done()
			src := g.Source()

			for range obj {
				opts := g.PutOpts
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
				client, clDone := g.Client()
				opts.ContentType = obj.ContentType
				res, err := client.PutObject(ctx, g.Bucket, obj.Name, obj.Reader, obj.Size, opts)
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
				clDone()
				obj.Reader = nil
				g.Dist.addObj(*obj)
				g.prepareProgress(float64(len(g.Dist.objects)) / float64(g.CreateObjects))
			}
		}()
	}
	wg.Wait()
	return groupErr
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (g *Mixed) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
	var wg sync.WaitGroup
	wg.Add(g.Concurrency)
	c := g.Collector
	if g.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, "", g.AutoTermScale, autoTermCheck, autoTermSamples, g.AutoTermDur)
	}
	// Non-terminating context.
	nonTerm := context.Background()

	for i := 0; i < g.Concurrency; i++ {
		go func(i int) {
			rcv := c.Receiver()
			defer wg.Done()
			done := ctx.Done()
			src := g.Source()
			putOpts := g.PutOpts
			statOpts := g.StatOpts
			getOpts := g.GetOpts

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

				operation := g.Dist.getOp()
				switch operation {
				case http.MethodGet:
					fbr := firstByteRecorder{}
					obj, objDone := g.Dist.randomObj()
					client, clDone := g.Client()
					op := Operation{
						OpType:   operation,
						Thread:   uint16(i),
						Size:     obj.Size,
						File:     obj.Name,
						ObjPerOp: 1,
						Endpoint: client.EndpointURL().String(),
					}

					op.Start = time.Now()
					var err error
					getOpts.VersionID = obj.VersionID
					o, err := client.GetObject(nonTerm, g.Bucket, obj.Name, getOpts)
					fbr.r = o
					if err != nil {
						g.Error("download error:", err)
						op.Err = err.Error()
						op.End = time.Now()
						rcv <- op
						clDone()
						objDone()
						continue
					}
					n, err := io.Copy(io.Discard, &fbr)
					if err != nil {
						g.Error("download error:", err)
						op.Err = err.Error()
					}
					op.FirstByte = fbr.t
					op.End = time.Now()
					if n != obj.Size && op.Err == "" {
						op.Err = fmt.Sprint("unexpected download size. want:", obj.Size, ", got:", n)
						g.Error(op.Err)
					}
					rcv <- op
					objDone()
					clDone()
					o.Close()

				case http.MethodPut:
					obj := src.Object()
					putOpts.ContentType = obj.ContentType
					client, clDone := g.Client()
					op := Operation{
						OpType:   operation,
						Thread:   uint16(i),
						Size:     obj.Size,
						File:     obj.Name,
						ObjPerOp: 1,
						Endpoint: client.EndpointURL().String(),
					}
					op.Start = time.Now()
					res, err := client.PutObject(nonTerm, g.Bucket, obj.Name, obj.Reader, obj.Size, putOpts)
					op.End = time.Now()
					if err != nil {
						g.Error("upload error:", err)
						op.Err = err.Error()
					}
					obj.VersionID = res.VersionID

					if res.Size != obj.Size && op.Err == "" {
						err := fmt.Sprint("short upload. want:", obj.Size, ", got:", res.Size)
						if op.Err == "" {
							op.Err = err
						}
						g.Error(err)
					}
					clDone()
					if op.Err == "" {
						g.Dist.addObj(*obj)
					}
					rcv <- op
				case http.MethodDelete:
					client, clDone := g.Client()
					obj := g.Dist.deleteRandomObj()
					op := Operation{
						OpType:   operation,
						Thread:   uint16(i),
						Size:     0,
						File:     obj.Name,
						ObjPerOp: 1,
						Endpoint: client.EndpointURL().String(),
					}

					op.Start = time.Now()
					err := client.RemoveObject(nonTerm, g.Bucket, obj.Name, minio.RemoveObjectOptions{VersionID: obj.VersionID})
					op.End = time.Now()
					clDone()
					if err != nil {
						g.Error("delete error: ", err)
						op.Err = err.Error()
					}
					rcv <- op
				case "STAT":
					obj, objDone := g.Dist.randomObj()
					client, clDone := g.Client()
					op := Operation{
						OpType:   operation,
						Thread:   uint16(i),
						Size:     0,
						File:     obj.Name,
						ObjPerOp: 1,
						Endpoint: client.EndpointURL().String(),
					}
					op.Start = time.Now()
					var err error
					objI, err := client.StatObject(nonTerm, g.Bucket, obj.Name, statOpts)
					if err != nil {
						g.Error("stat error: ", err)
						op.Err = err.Error()
					}
					op.End = time.Now()
					if objI.Size != obj.Size && op.Err == "" {
						op.Err = fmt.Sprint("unexpected stat size. want:", obj.Size, ", got:", objI.Size)
						g.Error(op.Err)
					}
					rcv <- op
					objDone()
					clDone()
				default:
					g.Error("unknown operation: ", operation)
				}
			}
		}(i)
	}
	wg.Wait()
	return c.Close(), nil
}

// Cleanup deletes everything uploaded to the bucket.
func (g *Mixed) Cleanup(ctx context.Context) {
	g.deleteAllInBucket(ctx, g.Dist.Objects().Prefixes()...)
}
