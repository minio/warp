package bench

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"
	"time"

	"github.com/minio/mc/pkg/console"

	"github.com/minio/minio-go/v6"
	"github.com/minio/warp/pkg/generator"
)

// Get benchmarks download speed.
type Get struct {
	CreateObjects int
	Collector     *Collector
	objects       []generator.Object

	// Default Get options.
	GetOpts minio.GetObjectOptions
	Common
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (g *Get) Prepare(ctx context.Context) {
	console.Println("Creating Bucket...")
	g.createEmptyBucket(ctx)
	console.Println("Uploading", g.CreateObjects, "Objects...")
	var wg sync.WaitGroup
	wg.Add(g.Concurrency)
	g.Collector = NewCollector()
	obj := make(chan struct{}, g.CreateObjects)
	for i := 0; i < g.CreateObjects; i++ {
		obj <- struct{}{}
	}
	close(obj)
	var mu sync.Mutex
	for i := 0; i < g.Concurrency; i++ {
		go func() {
			defer wg.Done()
			src := g.Source()
			for range obj {
				opts := g.PutOpts
				rcv := g.Collector.Receiver()
				done := ctx.Done()

				select {
				case <-done:
					return
				default:
				}
				obj := src.Object()
				op := Operation{
					OpType:   "PUT",
					Thread:   uint16(i),
					Size:     obj.Size,
					File:     obj.Name,
					ObjPerOp: 1,
				}
				opts.ContentType = obj.ContentType
				op.Start = time.Now()
				n, err := g.Client.PutObject(g.Bucket, obj.Name, obj.Reader, obj.Size, opts)
				op.End = time.Now()
				if err != nil {
					console.Fatal("upload error:", err)
				}
				if n != obj.Size {
					console.Fatal(fmt.Sprint("short upload. want:", obj.Size, "got:", n))
				}
				mu.Lock()
				obj.Reader = nil
				g.objects = append(g.objects, *obj)
				mu.Unlock()
				rcv <- op
			}
		}()
	}
	wg.Wait()
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (g *Get) Start(ctx context.Context, start chan struct{}) Operations {
	var wg sync.WaitGroup
	wg.Add(g.Concurrency)
	c := g.Collector
	for i := 0; i < g.Concurrency; i++ {
		go func(i int) {
			rng := rand.New(rand.NewSource(int64(i)))
			rcv := c.Receiver()
			defer wg.Done()
			opts := g.GetOpts
			done := ctx.Done()

			<-start
			for {
				select {
				case <-done:
					return
				default:
				}
				obj := g.objects[rng.Intn(len(g.objects))]
				op := Operation{
					OpType:   "GET",
					Thread:   uint16(i),
					Size:     obj.Size,
					File:     obj.Name,
					ObjPerOp: 1,
				}
				op.Start = time.Now()
				gotObj, err := g.Client.GetObject(g.Bucket, obj.Name, opts)
				if err != nil {
					console.Println("download error:", err)
					op.Err = err.Error()
					op.End = time.Now()
					rcv <- op
					continue
				}
				op.FirstByte = time.Now()
				n, err := io.Copy(ioutil.Discard, gotObj)
				if err != nil {
					console.Println("download error:", err)
					op.Err = err.Error()
				}
				op.End = time.Now()
				if n != obj.Size && op.Err == "" {
					op.Err = fmt.Sprint("unexpected download size. want:", obj.Size, "got:", n)
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
func (g *Get) Cleanup(ctx context.Context) {
	g.deleteAllInBucket(ctx)
}
