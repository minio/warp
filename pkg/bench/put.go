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
	"net/http"
	"sync"
	"time"

	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/generator"
)

// Put benchmarks upload speed.
type Put struct {
	Common
	prefixes      map[string]struct{}
	CreateObjects int
}

// PutOpts are options for PutObject.
type PutOpts struct {
	objects map[string]generator.Object
}

func (m *PutOpts) Generate(allocObjs int) error {

	m.objects = make(map[string]generator.Object, allocObjs)
	for i := 0; i < allocObjs; i++ {
		obj := generator.Object{
			Name:        fmt.Sprintf("obj-%d", i),
			ContentType: "application/octet-stream",
		}
		m.objects[obj.Name] = obj
	}
	return nil
}

func (m *PutOpts) Objects() generator.Objects {
	res := make(generator.Objects, 0, len(m.objects))
	for _, v := range m.objects {
		res = append(res, v)
	}
	return res
}

// Prepare will create an empty bucket ot delete any content already there.
func (u *Put) Prepare(ctx context.Context) error {
	return u.createEmptyBucket(ctx)
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (u *Put) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
	src := u.Source()
	var wg sync.WaitGroup
	if u.CreateObjects != 2500 && ctx.Value("duration") != 5*time.Minute {
		console.Info("\rUploading ", u.CreateObjects*u.Concurrency, " objects of ", src.String())
	}
	wg.Add(u.Concurrency)
	u.addCollector()
	c := u.Collector
	if u.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, http.MethodPut, u.AutoTermScale, autoTermCheck, autoTermSamples, u.AutoTermDur)
	}
	u.prefixes = make(map[string]struct{}, u.Concurrency)
	obj := make(chan struct{}, u.CreateObjects)
	for i := 0; i < u.CreateObjects; i++ {
		obj <- struct{}{}
	}
	close(obj)
	// Non-terminating context.
	nonTerm := context.Background()
	objectcount := 0
	for i := 0; i < u.Concurrency; i++ {
		src := u.Source()
		u.prefixes[src.Prefix()] = struct{}{}
		go func(i int) {
			rcv := c.Receiver()
			defer wg.Done()
			opts := u.PutOpts
			done := ctx.Done()

			<-wait
			for {
				select {
				case <-done:
					return
				default:
				}
				obj := src.Object()
				opts.ContentType = obj.ContentType
				client, cldone := u.Client()
				op := Operation{
					OpType:   http.MethodPut,
					Thread:   uint16(i),
					Size:     obj.Size,
					ObjPerOp: 1,
					File:     obj.Name,
					Endpoint: client.EndpointURL().String(),
				}

				op.Start = time.Now()
				res, err := client.PutObject(nonTerm, u.Bucket, obj.Name, obj.Reader, obj.Size, opts)
				op.End = time.Now()
				if err != nil {
					u.Error("upload error: ", err)
					op.Err = err.Error()
				}
				obj.VersionID = res.VersionID

				if res.Size != obj.Size && op.Err == "" {
					err := fmt.Sprint("short upload. want:", obj.Size, ", got:", res.Size)
					if op.Err == "" {
						op.Err = err
					}
					u.Error(err)
				}
				op.Size = res.Size
				cldone()
				rcv <- op
				objectcount++
				if ctx.Value("duration") != 5*time.Minute && u.CreateObjects != 2500 && objectcount >= u.CreateObjects {
					return
				}
			}
		}(i)
	}
	wg.Wait()
	return c.Close(), nil
}

// Cleanup deletes everything uploaded to the bucket.
func (u *Put) Cleanup(ctx context.Context) {
	pf := make([]string, 0, len(u.prefixes))
	for p := range u.prefixes {
		pf = append(pf, p)
	}
	u.deleteAllInBucket(ctx, pf...)
}
