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
	"maps"
	"mime/multipart"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/warp/pkg/generator"
)

// lastByteRecorder records the time when Read returns io.EOF.
// For uploads, this approximates when the request body was fully consumed.
type lastByteRecorder struct {
	t *time.Time
	r io.Reader
}

func (l *lastByteRecorder) Read(p []byte) (n int, err error) {
	n, err = l.r.Read(p)
	if err == io.EOF {
		t := time.Now()
		l.t = &t
	}
	return n, err
}

// Put benchmarks upload speed.
type Put struct {
	Common
	PostObject bool
	prefixes   map[string]struct{}
	cl         *http.Client
	rdmaPool   *rdmaPool
}

// Prepare will create an empty bucket or delete any content already there.
func (u *Put) Prepare(ctx context.Context) error {
	if u.PostObject {
		u.cl = &http.Client{
			Transport: u.Transport,
		}
	}
	if err := u.prepareRDMAPool(); err != nil {
		return err
	}
	if err := u.createEmptyBucket(ctx); err != nil {
		// A failed Prepare returns without the runner calling Cleanup, so the
		// pool allocated above would stay pinned for the life of the process.
		u.rdmaPool.Free()
		u.rdmaPool = nil
		return err
	}
	return nil
}

// prepareRDMAPool sizes and allocates the pinned buffers before the run.
//
// One buffer per worker, sized to hold a whole object when a descriptor can
// name one, and to a fixed window when it cannot -- past that size the upload
// streams as multipart and the buffer only selects the RDMA path rather than
// carrying the object.
func (u *Put) prepareRDMAPool() error {
	if u.RDMAMode == RDMAModeOff {
		return nil
	}
	size := int(u.ObjSize)
	if u.streamsRDMA() {
		size = rdmaWindowOrDefault(u.RDMAWindow)
	}
	pool, err := newRDMAPool(u.RDMAMode, u.Concurrency, size)
	if err != nil {
		return err
	}
	u.rdmaPool = pool
	return nil
}

// streamsRDMA reports whether uploads go out as a multipart stream rather than
// a single RDMA transfer: either the caller asked for it, or the object is
// larger than one descriptor can name.
func (u *Put) streamsRDMA() bool {
	return u.RDMAWindow > 0 || u.ObjSize > MaxRDMADescriptorSize
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (u *Put) Start(ctx context.Context, wait chan struct{}) error {
	var wg sync.WaitGroup
	wg.Add(u.Concurrency)
	c := u.Collector
	if u.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, http.MethodPut, u.AutoTermScale, autoTermCheck, autoTermSamples, u.AutoTermDur)
	}
	u.prefixes = make(map[string]struct{}, u.Concurrency)
	nonTerm := context.Background()

	for i := 0; i < u.Concurrency; i++ {
		src := u.Source()
		u.prefixes[src.Prefix()] = struct{}{}
		go func(i int) {
			rcv := c.Receiver()
			defer wg.Done()

			// Copy usermetadata and usertags per concurrent thread.
			opts := u.PutOpts
			opts.UserMetadata = make(map[string]string, len(u.PutOpts.UserMetadata))
			opts.UserTags = make(map[string]string, len(u.PutOpts.UserTags))
			maps.Copy(opts.UserMetadata, u.PutOpts.UserMetadata)
			maps.Copy(opts.UserTags, u.PutOpts.UserTags)

			done := ctx.Done()

			// GPU buffers are registered from whichever OS thread cgo happens
			// to use. Pin the worker to one thread and bind a CUDA context
			// there -- see bindGPUThread, including what is no longer certain
			// about it now that libs3rdma does the registering.
			//
			// Registered before the buffer cleanup below so that, defers being
			// LIFO, the thread stays locked until after cudaFree has run.
			if u.RDMAMode == RDMAModeGPU {
				runtime.LockOSThread()
				defer runtime.UnlockOSThread()
				if berr := bindGPUThread(); berr != nil {
					u.Error("rdma gpu bind:", berr)
					return
				}
			}

			// One pinned buffer per worker, taken from the pool allocated in
			// Prepare. Allocating here would charge the first operation of
			// every worker for a page-aligned allocation and the ibv_reg_mr
			// that pins it -- work unrelated to the transfer being measured.
			wbuf := u.rdmaPool.Get(i)

			// A buffer the worker had to allocate for itself is not in the
			// pool, so Cleanup will not free it. Track which kind this is:
			// releasing a pooled buffer here would free it twice.
			pooled := wbuf != nil
			defer func() {
				if wbuf != nil && !pooled {
					freeRDMABuf(wbuf)
				}
			}()

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
					Thread:   uint32(i),
					Size:     obj.Size,
					ObjPerOp: 1,
					File:     obj.Name,
					Endpoint: client.EndpointURL().String(),
				}

				op.Start = time.Now()
				var err error
				var res minio.UploadInfo
				if !u.PostObject {
					if u.RDMAMode != RDMAModeOff {
						// Stage generator output into a CPU or GPU buffer
						// (per --rdma) so minio-go's RDMA dispatch path can
						// RDMA-WRITE it directly.
						//
						// With --rdma.window the buffer is a fixed staging
						// window instead of the whole object: the reader goes
						// to minio-go, which streams it as an RDMA multipart
						// upload and pins one part at a time. That is what
						// carries objects past the 4 GiB an RDMA descriptor
						// can address, and it keeps a 16 GiB run from asking
						// for 16 GiB of pinned memory per worker.
						// A single RDMA transfer cannot name more than a
						// descriptor's 32-bit size field, so anything larger
						// has to stream as multipart whether or not it was
						// asked for. Deciding here rather than letting the
						// SDK reject it keeps warp from pinning an object's
						// worth of memory only to fail on every operation.
						stream := u.streamsRDMA() || obj.Size > MaxRDMADescriptorSize
						bufSize := int(obj.Size)
						if stream {
							bufSize = rdmaWindowOrDefault(u.RDMAWindow)
						}
						// The pool is sized from the configured object size, so
						// this only fires if the generator produced something
						// larger than advertised.
						if wbuf == nil || wbuf.size < bufSize {
							if wbuf != nil && !pooled {
								freeRDMABuf(wbuf)
							}
							pooled = false
							wbuf, err = allocRDMABuf(u.RDMAMode, bufSize)
						}
						if err == nil {
							if stream {
								opts.RDMABuffer = wbuf.ptr
								opts.RDMABufferSize = bufSize
								res, err = client.PutObject(nonTerm, u.Bucket, obj.Name, obj.Reader, obj.Size, opts)
							} else if serr := stageToRDMABuf(wbuf, obj.Reader, int(obj.Size)); serr != nil {
								err = fmt.Errorf("rdma upload prep: %w", serr)
							} else {
								opts.RDMABuffer = wbuf.ptr
								opts.RDMABufferSize = int(obj.Size)
								res, err = client.PutObject(nonTerm, u.Bucket, obj.Name, nil, obj.Size, opts)
							}
						}
					} else {
						res, err = client.PutObject(nonTerm, u.Bucket, obj.Name, obj.Reader, obj.Size, opts)
					}
				} else {
					op.OpType = http.MethodPost
					var verID string
					verID, err = u.postPolicy(ctx, client, u.Bucket, obj)
					if err == nil {
						res.Size = obj.Size
						res.VersionID = verID
					}
				}
				op.End = time.Now()
				// LastByte drives the reported TTFB, as End-LastByte. That only
				// means anything while the request body streams: the RDMA path
				// drains the reader into the staging buffer before the transfer
				// starts, so LastByte would land at the head of the operation and
				// TTFB would report the whole transfer. Leave it unset and the
				// column is dropped rather than filled with a number that invites
				// comparison against the HTTP path.
				if u.RDMAMode == RDMAModeOff {
					op.LastByte = obj.Reader.LastByte()
				}
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
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// Cleanup deletes everything uploaded to the bucket.
func (u *Put) Cleanup(ctx context.Context) {
	u.rdmaPool.Free()
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
