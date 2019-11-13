package bench

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/minio/mc/pkg/console"
)

// Put benchmarks upload speed.
type Put struct {
	Common
}

// Prepare will create an empty bucket ot delete any content already there.
func (u *Put) Prepare(ctx context.Context) {
	u.createEmptyBucket(ctx)
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (u *Put) Start(ctx context.Context, start chan struct{}) Operations {
	var wg sync.WaitGroup
	wg.Add(u.Concurrency)
	c := NewCollector()
	for i := 0; i < u.Concurrency; i++ {
		go func(i int) {
			rcv := c.Receiver()
			defer wg.Done()
			src := u.Source()
			opts := u.PutOpts
			done := ctx.Done()

			<-start
			for {
				select {
				case <-done:
					return
				default:
				}
				obj := src.Object()
				opts.ContentType = obj.ContentType
				op := Operation{
					OpType:   "PUT",
					Thread:   uint16(i),
					Size:     obj.Size,
					File:     obj.Name,
					ObjPerOp: 1,
				}
				op.Start = time.Now()
				n, err := u.Client.PutObject(u.Bucket, obj.Name, obj.Reader, obj.Size, opts)
				op.End = time.Now()
				if err != nil {
					console.Println("upload error:", err)
					op.Err = err.Error()
				}
				if n != obj.Size {
					op.Err = fmt.Sprint("short upload. want:", obj.Size, "got:", n)
					console.Println(op.Err)
				}
				rcv <- op
			}
		}(i)
	}
	wg.Wait()
	return c.Close()
}

// Cleanup deletes everything uploaded to the bucket.
func (u *Put) Cleanup(ctx context.Context) {
	u.deleteAllInBucket(ctx)
}
