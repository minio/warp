package bench

import (
	"context"
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

type Results struct {
	mu         sync.Mutex
	TotalBytes int64
	TotalTime  time.Duration
}

func (u *Upload) Start(ctx context.Context, start chan struct{}) {
	var wg sync.WaitGroup
	wg.Add(u.Concurrency)
	for i := 0; i < u.Concurrency; i++ {
		go func(i int) {
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
				start := time.Now()
				n, err := u.Client.PutObject(u.Bucket, obj.Name, obj.Reader, obj.Size, opts)
				if err != nil {
					log.Println("upload error:", err)
					continue
				}
				if n != obj.Size {
					log.Println("short upload. want:", obj.Size, "got:", n)
					continue
				}
				log.Printf("uploaded %+v, took %v\n", obj, time.Since(start))
			}
		}(i)
	}
	wg.Wait()
}

func (u *Upload) Cleanup(ctx context.Context) {
	u.deleteAllInBucket(ctx)
}
