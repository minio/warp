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

	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/pkg/generator"
)

// Put benchmarks upload speed.
type Put struct {
	Common
	objects generator.Objects
}

// Prepare will create an empty bucket ot delete any content already there.
func (u *Put) Prepare(ctx context.Context) error {
	return nil
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (u *Put) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
	var wg sync.WaitGroup
	wg.Add(u.Concurrency)
	c := NewCollector()
	if u.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, http.MethodPut, u.AutoTermScale, autoTermCheck, autoTermSamples, u.AutoTermDur)
	}

	// Non-terminating context.
	nonTerm := context.Background()

	var mu sync.Mutex
	for i := 0; i < u.Concurrency; i++ {
		src := u.Source()
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
					File:     obj.Name,
					ObjPerOp: 1,
					Endpoint: client.EndpointURL().String(),
				}
				op.Start = time.Now()
				res, err := client.PutObject(nonTerm, obj.Bucket, obj.Name, obj.Reader, obj.Size, opts)
				op.End = time.Now()
				if err != nil {
					console.Errorln("upload error:", err)
					op.Err = err.Error()
				}
				obj.VersionID = res.VersionID

				if res.Size != obj.Size && op.Err == "" {
					err := fmt.Sprint("short upload. want:", obj.Size, ", got:", res.Size)
					if op.Err == "" {
						op.Err = err
					}
					console.Errorln(err)
				}

				mu.Lock()
				u.objects = append(u.objects, *obj)
				mu.Unlock()

				op.Size = res.Size
				cldone()
				rcv <- op
			}
		}(i)
	}
	wg.Wait()
	return c.Close(), nil
}

// Cleanup deletes everything uploaded to the bucket.
func (u *Put) Cleanup(ctx context.Context) {
	u.deleteAllInBucket(ctx, u.objects.Bucket(), u.objects.Prefixes()...)
}
