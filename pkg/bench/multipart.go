/*
 * Warp (C) 2019-2022 MinIO, Inc.
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
	"strconv"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/v2/console"
	"github.com/minio/warp/pkg/generator"
)

// Multipart benchmarks multipart upload+download speed.
type Multipart struct {
	Common

	// Default Get options.
	GetOpts  minio.GetObjectOptions
	ObjName  string
	UploadID string

	objects     generator.Objects
	CreateParts int
	PartStart   int
}

// InitOnce will be run once
func (g *Multipart) InitOnce(ctx context.Context) error {
	if err := g.createEmptyBucket(ctx); err != nil {
		return err
	}
	console.Eraseline()
	console.Info("\rCreating Object...")

	cl, done := g.Client()
	c := minio.Core{Client: cl}
	defer done()
	uploadID, err := c.NewMultipartUpload(ctx, g.Bucket, g.ObjName, g.PutOpts)
	if err != nil {
		return err
	}
	g.UploadID = uploadID
	return nil
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (g *Multipart) Prepare(ctx context.Context) error {
	g.PartStart += g.CreateParts * g.ClientIdx
	if g.PartStart+g.CreateParts > 10001 {
		return errors.New("10000 part limit exceeded")
	}
	console.Println("")
	console.Eraseline()
	console.Info("Uploading ", g.CreateParts, " object parts")

	var wg sync.WaitGroup
	wg.Add(g.Concurrency)
	g.addCollector()
	obj := make(chan int, g.CreateParts)
	for i := 0; i < g.CreateParts; i++ {
		obj <- i + g.PartStart
	}
	rcv := g.Collector.rcv
	close(obj)
	var groupErr error
	var mu sync.Mutex

	if g.Custom == nil {
		g.Custom = make(map[string]string, g.CreateParts)
	}
	for i := 0; i < g.Concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			src := g.Source()
			opts := g.PutOpts

			for partN := range obj {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if g.rpsLimit(ctx) != nil {
					return
				}

				name := g.ObjName
				// New input for each version
				obj := src.Object()
				obj.Name = name
				client, cldone := g.Client()
				core := minio.Core{Client: client}
				op := Operation{
					OpType:   http.MethodPut,
					Thread:   uint16(i),
					Size:     obj.Size,
					File:     obj.Name,
					ObjPerOp: 1,
					Endpoint: client.EndpointURL().String(),
				}
				if g.DiscardOutput {
					op.File = ""
				}

				opts.ContentType = obj.ContentType
				mpopts := minio.PutObjectPartOptions{
					SSE: g.Common.PutOpts.ServerSideEncryption,
				}
				op.Start = time.Now()
				res, err := core.PutObjectPart(ctx, g.Bucket, obj.Name, g.UploadID, partN, obj.Reader, obj.Size, mpopts)
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
				obj.VersionID = res.ETag
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
				g.Custom[strconv.Itoa(partN)] = res.ETag
				g.prepareProgress(float64(len(g.objects)) / float64(g.CreateParts))
				mu.Unlock()
				rcv <- op
			}
		}(i)
	}
	wg.Wait()
	return groupErr
}

func (g *Multipart) AfterPrepare(ctx context.Context) error {
	cl, done := g.Client()
	c := minio.Core{Client: cl}
	defer done()
	var parts []minio.CompletePart
	i := 1
	for {
		etag, ok := g.Custom[strconv.Itoa(i)]
		if !ok {
			break
		}
		parts = append(parts, minio.CompletePart{PartNumber: i, ETag: etag})
		i++
	}
	console.Eraseline()
	console.Infof("\rCompleting Object with %d parts...", len(parts))
	_, err := c.CompleteMultipartUpload(ctx, g.Bucket, g.ObjName, g.UploadID, parts, g.PutOpts)
	return err
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (g *Multipart) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
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

				if g.rpsLimit(ctx) != nil {
					return
				}

				fbr := firstByteRecorder{}
				part := rng.Intn(len(g.objects))
				obj := g.objects[part]
				part += g.PartStart
				client, cldone := g.Client()
				op := Operation{
					OpType:   http.MethodGet,
					Thread:   uint16(i),
					Size:     obj.Size,
					File:     obj.Name,
					ObjPerOp: 1,
					Endpoint: client.EndpointURL().String(),
				}

				op.Start = time.Now()
				opts.PartNumber = part
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
func (g *Multipart) Cleanup(ctx context.Context) {
	g.deleteAllInBucket(ctx, "")
}
