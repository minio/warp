package cli

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/minio/cli"
	"github.com/minio/warp/pkg/bench"
)

// mainPut is the entry point for cp command.
func runBench(ctx *cli.Context, b bench.Benchmark) error {
	log.Println("Preparing server.")
	b.Prepare(context.Background())

	tStart := time.Now().Add(time.Second)
	ctx2, cancel := context.WithDeadline(context.Background(), tStart.Add(time.Minute))
	defer cancel()
	start := make(chan struct{})
	go func() {
		<-time.After(time.Until(tStart))
		close(start)
	}()
	log.Println("Done. Starting benchmark")
	ops := b.Start(ctx2, start)
	ops.SortByStartTime()
	log.Println("Done. Starting cleanup")
	b.Cleanup(context.Background())

	return ops.CSV(os.Stdout)
}
