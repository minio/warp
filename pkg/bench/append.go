/*
 * Warp (C) 2019-2025 MinIO, Inc.
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

	"github.com/minio/minio-go/v7"
)

// Append benchmarks upload speed via appends.
type Append struct {
	Common
	prefixes map[string]struct{}
}

// Prepare will create an empty bucket or delete any content already there.
func (u *Append) Prepare(ctx context.Context) error {
	return u.createEmptyBucket(ctx)
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (u *Append) Start(ctx context.Context, wait chan struct{}) error {
	var wg sync.WaitGroup
	wg.Add(u.Concurrency)
	c := u.Collector
	if u.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, http.MethodPut, u.AutoTermScale, autoTermCheck, autoTermSamples, u.AutoTermDur)
	}
	u.prefixes = make(map[string]struct{}, u.Concurrency)

	// Non-terminating context.
	nonTerm := context.Background()

	for i := 0; i < u.Concurrency; i++ {
		src := u.Source()
		u.prefixes[src.Prefix()] = struct{}{}
		go func(i int) {
			part := 1
			tmp := src.Object()
			masterObj := *tmp

			rcv := c.Receiver()
			defer wg.Done()

			// Copy usermetadata and usertags per concurrent thread.
			opts := u.PutOpts
			opts.UserMetadata = make(map[string]string, len(u.PutOpts.UserMetadata))
			opts.UserTags = make(map[string]string, len(u.PutOpts.UserTags))
			// Only create 1 part on initial upload.
			opts.DisableMultipart = true
			for k, v := range u.PutOpts.UserMetadata {
				opts.UserMetadata[k] = v
			}
			for k, v := range u.PutOpts.UserTags {
				opts.UserTags[k] = v
			}
			aOpts := minio.AppendObjectOptions{
				Progress:             nil,
				ChunkSize:            0,
				DisableContentSha256: opts.DisableContentSha256,
			}

			done := ctx.Done()

			<-wait
			for {
				if part >= 10000 {
					tmp := src.Object()
					masterObj = *tmp
					part = 1
				}

				select {
				case <-done:
					return
				default:
				}

				if u.rpsLimit(ctx) != nil {
					return
				}
				obj := src.Object()
				obj.Name = masterObj.Name
				obj.Prefix = masterObj.Prefix
				obj.ContentType = masterObj.ContentType

				opts.ContentType = obj.ContentType
				client, cldone := u.Client()
				op := Operation{
					OpType:   "APPEND",
					Thread:   uint16(i),
					Size:     obj.Size,
					ObjPerOp: 1,
					File:     obj.Name,
					Endpoint: client.EndpointURL().String(),
				}

				op.Start = time.Now()
				var err error
				var res minio.UploadInfo
				if part == 1 {
					res, err = client.PutObject(nonTerm, u.Bucket, obj.Name, obj.Reader, obj.Size, opts)
				} else {
					res, err = client.AppendObject(nonTerm, u.Bucket, obj.Name, obj.Reader, obj.Size, aOpts)
				}
				op.End = time.Now()
				if err != nil {
					u.Error("upload error: ", err)
					op.Err = err.Error()
				}

				if res.Size != int64(part)*obj.Size && op.Err == "" {
					err := fmt.Sprint("part ", part, " short upload. want:", int64(part)*obj.Size, ", got:", res.Size)
					if op.Err == "" {
						op.Err = err
					}
					u.Error(err)
				}
				part++
				cldone()
				rcv <- op
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// Cleanup deletes everything uploaded to the bucket.
func (u *Append) Cleanup(ctx context.Context) {
	pf := make([]string, 0, len(u.prefixes))
	for p := range u.prefixes {
		pf = append(pf, p)
	}
	u.deleteAllInBucket(ctx, pf...)
}
