package bench

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type Upload struct {
	Common
}

func (u *Upload) Prepare(ctx context.Context) {
	u.createEmptyBucket(ctx)
}

func (u *Upload) Start(ctx context.Context, start chan struct{}) Operations {
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
					Op:     "PUT",
					Thread: uint16(i),
					Size:   obj.Size,
					File:   obj.Name,
				}
				op.Start = time.Now()
				n, err := u.Client.PutObject(u.Bucket, obj.Name, obj.Reader, obj.Size, opts)
				op.End = time.Now()
				if err != nil {
					log.Println("upload error:", err)
					op.Err = err.Error()
				}
				if n != obj.Size {
					op.Err = fmt.Sprint("short upload. want:", obj.Size, "got:", n)
					log.Println(op.Err)
				}
				rcv <- op
			}
		}(i)
	}
	wg.Wait()
	return c.Close()
}

func (u *Upload) Cleanup(ctx context.Context) {
	u.deleteAllInBucket(ctx)
}
