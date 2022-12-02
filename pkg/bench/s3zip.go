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
	"archive/zip"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/generator"
)

// S3Zip benchmarks download from a zip file.
type S3Zip struct {
	CreateFiles int
	ZipObjName  string
	Collector   *Collector
	objects     generator.Objects

	Common
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (g *S3Zip) Prepare(ctx context.Context) error {
	if err := g.createEmptyBucket(ctx); err != nil {
		return err
	}

	g.Collector = NewCollector()
	src := g.Source()
	console.Eraseline()
	console.Info("\rUploading", g.ZipObjName, "with ", g.CreateFiles, " files each of ", src.String())

	client, cldone := g.Client()
	defer cldone()
	pr, pw := io.Pipe()
	zw := zip.NewWriter(pw)

	go func() {
		for i := 0; i < g.CreateFiles; i++ {
			opts := g.PutOpts
			done := ctx.Done()

			select {
			case <-done:
				return
			default:
			}
			obj := src.Object()

			opts.ContentType = obj.ContentType
			header := zip.FileHeader{
				Name:   obj.Name,
				Method: 0,
			}

			f, err := zw.CreateHeader(&header)
			if err != nil {
				err := fmt.Errorf("zip create error: %w", err)
				g.Error(err)
				pw.CloseWithError(err)
				return
			}
			_, err = io.Copy(f, obj.Reader)
			if err != nil {
				err := fmt.Errorf("zip write error: %w", err)
				g.Error(err)
				pw.CloseWithError(err)
				return
			}

			obj.Reader = nil
			g.objects = append(g.objects, *obj)
			g.prepareProgress(float64(i) / float64(g.CreateFiles))
		}
		pw.CloseWithError(zw.Close())
	}()

	// TODO: Add header to index.
	// g.PutOpts.Set("x-minio-extract", "true")
	_, err := client.PutObject(ctx, g.Bucket, g.ZipObjName, pr, -1, g.PutOpts)
	pr.CloseWithError(err)
	if err == nil {
		var opts minio.GetObjectOptions
		opts.Set("x-minio-extract", "true")

		oi, err2 := client.GetObject(ctx, g.Bucket, path.Join(g.ZipObjName, g.objects[0].Name), opts)
		if err2 != nil {
			err = err2
		}
		if err == nil {
			st, err2 := oi.Stat()
			err = err2
			if err == nil && st.Size != g.objects[0].Size {
				err = fmt.Errorf("unexpected download size. want: %v, got %v", g.objects[0].Size, st.Size)
			}
		}
	}
	return err
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (g *S3Zip) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
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
			var opts minio.GetObjectOptions

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
					OpType:   "GET",
					Thread:   uint16(i),
					Size:     obj.Size,
					File:     path.Join(g.ZipObjName, obj.Name),
					ObjPerOp: 1,
					Endpoint: client.EndpointURL().String(),
				}

				op.Start = time.Now()
				opts.Set("x-minio-extract", "true")

				o, err := client.GetObject(nonTerm, g.Bucket, op.File, opts)
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
func (g *S3Zip) Cleanup(ctx context.Context) {
	g.deleteAllInBucket(ctx)
}
