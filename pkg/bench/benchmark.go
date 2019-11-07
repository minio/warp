package bench

import (
	"context"
	"log"

	"github.com/minio/minio-go/v6"
	"github.com/minio/warp/pkg/generator"
)

type Benchmark interface {
	Prepare(ctx context.Context)
	Start(ctx context.Context, sync chan struct{})
	Cleanup(ctx context.Context)
}

type Common struct {
	Client *minio.Client

	Concurrency int
	Source      func() generator.Source
	Bucket      string
	Location    string

	// Default Put options.
	PutOpts minio.PutObjectOptions
}

func (c *Common) createEmptyBucket(ctx context.Context) {
	x, err := c.Client.BucketExists(c.Bucket)
	if err != nil {
		log.Fatal(err)
	}
	if !x {
		err = c.Client.MakeBucket(c.Bucket, c.Location)
		if err != nil {
			log.Fatal(err)
		}
		return
	}
	c.deleteAllInBucket(ctx)
}

func (c *Common) deleteAllInBucket(ctx context.Context) {
	doneCh := make(chan struct{})
	defer close(doneCh)
	objects := c.Client.ListObjectsV2(c.Bucket, "", true, doneCh)
	remove := make(chan string, 1)
	errCh := c.Client.RemoveObjectsWithContext(ctx, c.Bucket, remove)
	for {
		select {
		case obj, ok := <-objects:
			if !ok {
				close(remove)
				// Wait for deletes to finish
				err := <-errCh
				if err.Err != nil {
					log.Fatal(err.Err)
				}
				return
			}
			if obj.Err != nil {
				log.Fatal(obj.Err)
			}
			//log.Printf("Deleting: %+v", obj)
			remove <- obj.Key
		case err := <-errCh:
			log.Fatal(err)
		}
	}
}
