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
	"mime/multipart"
	"net/http"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/warp/pkg/generator"
)

// Put benchmarks upload speed.
type Put struct {
	Common
	PostObject bool
	prefixes   map[string]struct{}
	cl         *http.Client
}

// Prepare will create an empty bucket ot delete any content already there.
func (u *Put) Prepare(ctx context.Context) error {
	if u.PostObject {
		u.cl = &http.Client{
			Transport: u.Transport,
		}
	}
	return u.createEmptyBucket(ctx)
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (u *Put) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
	var wg sync.WaitGroup
	wg.Add(u.Concurrency)
	u.addCollector()
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

				if u.rpsLimit(ctx) != nil {
					return
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
				var err error
				var res minio.UploadInfo
				if !u.PostObject {
					res, err = client.PutObject(nonTerm, u.Bucket(), obj.Name, obj.Reader, obj.Size, opts)
				} else {
					op.OpType = http.MethodPost
					var verID string
					verID, err = u.postPolicy(ctx, client, u.Bucket(), obj)
					if err == nil {
						res.Size = obj.Size
						res.VersionID = verID
					}
				}
				op.End = time.Now()
				if err != nil {
					u.Error( fmt.Sprintf("upload error in PutObject. Error '%s', filename '%s', bucket '%s', size '%s'", err, obj.Name, u.Bucket(), obj.Size ) )
					op.Err = err.Error()
				}
				obj.VersionID = res.VersionID

				if res.Size != obj.Size && op.Err == "" {
					err := fmt.Sprintf("short upload; wanted %s and got %s. filename '%s', bucket '%s', size '%s'", obj.Size, res.Size, obj.Name, u.Bucket())
					if op.Err == "" {
						op.Err = err
					}
					u.Error(err)
				}
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
	pf := make([]string, 0, len(u.prefixes))
	for p := range u.prefixes {
		pf = append(pf, p)
	}
	u.deleteAllInBucket(ctx, pf...)
}

// postPolicy will upload using https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html API.
func (u *Put) postPolicy(ctx context.Context, c *minio.Client, bucket string, obj *generator.Object) (versionID string, err error) {
	pp := minio.NewPostPolicy()
	pp.SetEncryption(u.PutOpts.ServerSideEncryption)
	err = errors.Join(
		pp.SetContentType(obj.ContentType),
		pp.SetBucket(bucket),
		pp.SetKey(obj.Name),
		pp.SetContentLengthRange(obj.Size, obj.Size),
		pp.SetExpires(time.Now().Add(24*time.Hour)),
	)
	if err != nil {
		return "", err
	}
	url, form, err := c.PresignedPostPolicy(ctx, pp)
	if err != nil {
		return "", err
	}
	pr, pw := io.Pipe()
	defer pr.Close()
	writer := multipart.NewWriter(pw)
	go func() {
		for k, v := range form {
			if err := writer.WriteField(k, v); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
		ff, err := writer.CreateFormFile("file", obj.Name)
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		_, err = io.Copy(ff, obj.Reader)
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.CloseWithError(writer.Close())
	}()

	req, err := http.NewRequest(http.MethodPost, url.String(), pr)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// make POST request with form data
	resp, err := u.cl.Do(req)
	if err != nil {
		return "", err
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: (%d) %s", resp.StatusCode, resp.Status)
	}

	return resp.Header.Get("x-amz-version-id"), nil
}
