package cli

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/warp/pkg/bench"
)

var benchFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "benchdata",
		Value: "",
		Usage: "Output benchmark data to this file. By default unique filename is generated.",
	},
}

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

	fileName := ctx.String("benchdata")
	if fileName == "" {
		fileName = fmt.Sprintf("%s-benchdata-%s.csv.zst", appName, time.Now().Format("2006-01-02[150405]"))
	}
	f, err := os.Create(fileName)
	if err != nil {
		log.Println("Unable to write benchmark data:", err)
	} else {
		defer f.Close()
		enc, err := zstd.NewWriter(f)
		if err != nil {
			log.Fatal(err)
		}
		defer enc.Close()
		err = ops.CSV(enc)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Benchmark data written to %q\n", fileName)
	}
	printAnalysis(ctx, ops)
	return nil
}
