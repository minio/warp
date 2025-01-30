/*
 * Warp (C) 2019-2023 MinIO, Inc.
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

// Fanout benchmarks upload speed.
type Fanout struct {
	Common
	Copies   int
	prefixes map[string]struct{}
}

// Prepare will create an empty bucket or delete any content already there.
func (u *Fanout) Prepare(ctx context.Context) error {
	return u.createEmptyBucket(ctx)
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (u *Fanout) Start(ctx context.Context, wait chan struct{}) error {
	var wg sync.WaitGroup
	wg.Add(u.Concurrency)
	c := u.Collector
	if u.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, http.MethodPost, u.AutoTermScale, autoTermCheck, autoTermSamples, u.AutoTermDur)
	}
	u.prefixes = make(map[string]struct{}, u.Concurrency)

	// Non-terminating context.
	nonTerm := context.Background()

	for i := 0; i < u.Concurrency; i++ {
		src := u.Source()
		u.prefixes[src.Prefix()] = struct{}{}
		go func(i int) {
			rcv := c.Receiver()
			defer wg.Done()
			opts := minio.PutObjectFanOutRequest{
				Entries:  make([]minio.PutObjectFanOutEntry, u.Copies),
				Checksum: minio.Checksum{},
				SSE:      nil,
			}
			done := ctx.Done()

			<-wait
			for {
				select {
				case <-done:
					return
				default:
				}
				obj := src.Object()
				for i := range opts.Entries {
					opts.Entries[i] = minio.PutObjectFanOutEntry{
						Key:                fmt.Sprintf("%s/copy-%d.ext", obj.Name, i),
						UserMetadata:       u.PutOpts.UserMetadata,
						UserTags:           u.PutOpts.UserTags,
						ContentType:        obj.ContentType,
						ContentEncoding:    u.PutOpts.ContentEncoding,
						ContentDisposition: u.PutOpts.ContentDisposition,
						ContentLanguage:    u.PutOpts.ContentLanguage,
						CacheControl:       u.PutOpts.CacheControl,
					}
				}
				client, cldone := u.Client()
				op := Operation{
					OpType:   http.MethodPost,
					Thread:   uint16(i),
					Size:     obj.Size * int64(u.Copies),
					ObjPerOp: u.Copies,
					File:     obj.Name,
					Endpoint: client.EndpointURL().String(),
				}

				op.Start = time.Now()
				res, err := client.PutObjectFanOut(nonTerm, u.Bucket, obj.Reader, opts)
				op.End = time.Now()
				if err != nil {
					u.Error("upload error: ", err)
					op.Err = err.Error()
				}

				var firstErr string
				nErrs := 0
				for _, r := range res {
					if r.Error != "" {
						if firstErr == "" {
							firstErr = r.Error
						}
						nErrs++
					}
				}
				if op.Err == "" && nErrs > 0 {
					op.Err = fmt.Sprintf("%d copies failed. First error: %v", nErrs, firstErr)
				}

				if len(res) != u.Copies && op.Err == "" {
					err := fmt.Sprint("short upload. want:", u.Copies, " copies, got:", len(res))
					if op.Err == "" {
						op.Err = err
					}
					u.Error(err)
				}

				cldone()
				rcv <- op
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// Cleanup deletes everything uploaded to the bucket.
func (u *Fanout) Cleanup(ctx context.Context) {
	pf := make([]string, 0, len(u.prefixes))
	for p := range u.prefixes {
		pf = append(pf, p)
	}
	u.deleteAllInBucket(ctx, pf...)
}
