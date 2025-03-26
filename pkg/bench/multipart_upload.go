package bench

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"
)

// MultipartUpload benchmarks multipart upload speed.
type MultipartUpload struct {
	Common

	PartsNumber      int
	PartsConcurrency int
}

// Prepare for the benchmark run
func (g *MultipartUpload) Prepare(ctx context.Context) error {
	return g.createEmptyBucket(ctx)
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (g *MultipartUpload) Start(ctx context.Context, wait chan struct{}) error {
	eg, ctx := errgroup.WithContext(ctx)
	c := g.Collector
	if g.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, http.MethodPut, g.AutoTermScale, autoTermCheck, autoTermSamples, g.AutoTermDur)
	}

	for i := 0; i < g.Concurrency; i++ {
		thread := uint16(i)
		eg.Go(func() error {
			<-wait

			for ctx.Err() == nil {
				objectName := g.Source().Object().Name

				uploadID, err := g.createMultupartUpload(ctx, objectName)
				if errors.Is(err, context.Canceled) {
					return nil
				}
				if err != nil {
					g.Error("create multipart upload error:", err)
					continue
				}

				err = g.uploadParts(ctx, thread, objectName, uploadID)
				if errors.Is(err, context.Canceled) {
					return nil
				}
				if err != nil {
					g.Error("upload parts error:", err)
					continue
				}

				err = g.completeMultipartUpload(ctx, objectName, uploadID)
				if err != nil {
					g.Error("complete multipart upload")
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

// Cleanup up after the benchmark run.
func (g *MultipartUpload) Cleanup(ctx context.Context) {
	g.deleteAllInBucket(ctx, "")
}

func (g *MultipartUpload) createMultupartUpload(ctx context.Context, objectName string) (string, error) {
	if err := g.rpsLimit(ctx); err != nil {
		return "", err
	}

	// Non-terminating context.
	nonTerm := context.Background()

	client, done := g.Client()
	defer done()
	c := minio.Core{Client: client}
	return c.NewMultipartUpload(nonTerm, g.Bucket, objectName, g.PutOpts)
}

func (g *MultipartUpload) uploadParts(ctx context.Context, thread uint16, objectName string, uploadID string) error {
	partIdxCh := make(chan int, g.PartsNumber)
	for i := range g.PartsNumber {
		partIdxCh <- i + 1
	}
	close(partIdxCh)

	eg, ctx := errgroup.WithContext(ctx)

	// Non-terminating context.
	nonTerm := context.Background()

	for i := 0; i < g.PartsConcurrency; i++ {
		eg.Go(func() error {
			for ctx.Err() == nil {
				var partIdx int
				var ok bool
				select {
				case partIdx, ok = <-partIdxCh:
					if !ok {
						return nil
					}
				case <-ctx.Done():
					continue
				}

				if err := g.rpsLimit(ctx); err != nil {
					return err
				}

				obj := g.Source().Object()
				client, done := g.Client()
				defer done()
				core := minio.Core{Client: client}
				op := Operation{
					OpType:   http.MethodPut,
					Thread:   thread,
					Size:     obj.Size,
					File:     obj.Name,
					ObjPerOp: 1,
					Endpoint: client.EndpointURL().String(),
				}
				if g.DiscardOutput {
					op.File = ""
				}

				opts := minio.PutObjectPartOptions{
					SSE:                  g.Common.PutOpts.ServerSideEncryption,
					DisableContentSha256: g.PutOpts.DisableContentSha256,
				}

				op.Start = time.Now()
				_, err := core.PutObjectPart(nonTerm, g.Bucket, objectName, uploadID, partIdx, obj.Reader, obj.Size, opts)
				op.End = time.Now()
				if err != nil {
					err := fmt.Errorf("upload error: %w", err)
					g.Error(err)
					return err
				}
				g.Collector.Receiver() <- op
			}

			return nil
		})
	}

	return eg.Wait()
}

func (g *MultipartUpload) completeMultipartUpload(_ context.Context, objectName string, uploadID string) error {
	// Non-terminating context.
	nonTerm := context.Background()

	cl, done := g.Client()
	c := minio.Core{Client: cl}
	defer done()
	_, err := c.CompleteMultipartUpload(nonTerm, g.Bucket, objectName, uploadID, nil, g.PutOpts)
	return err
}
