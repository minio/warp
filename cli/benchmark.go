/*
 * Warp (C) 2019- MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/minio/mc/pkg/probe"

	"github.com/minio/mc/pkg/console"

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

// runBench will run the supplied benchmark and save/print the analysis.
func runBench(ctx *cli.Context, b bench.Benchmark) error {
	console.Println("Preparing server.")
	b.Prepare(context.Background())

	// Start after waiting a second.
	tStart := time.Now().Add(time.Second)
	ctx2, cancel := context.WithDeadline(context.Background(), tStart.Add(time.Minute))
	defer cancel()
	start := make(chan struct{})
	go func() {
		<-time.After(time.Until(tStart))
		close(start)
	}()
	console.Println("Done. Starting benchmark...")
	ops := b.Start(ctx2, start)
	ops.SortByStartTime()
	console.Println("Done. Starting cleanup...")
	b.Cleanup(context.Background())

	fileName := ctx.String("benchdata")
	if fileName == "" {
		fileName = fmt.Sprintf("%s-benchdata-%s.csv.zst", appName, time.Now().Format("2006-01-02[150405]"))
	}
	f, err := os.Create(fileName)
	if err != nil {
		console.Error("Unable to write benchmark data:", err)
	} else {
		defer f.Close()
		enc, err := zstd.NewWriter(f)
		fatalIf(probe.NewError(err), "Unable to compress benchmark output")

		defer enc.Close()
		err = ops.CSV(enc)
		fatalIf(probe.NewError(err), "Unable to write benchmark output")

		console.Printf("Benchmark data written to %q\n", fileName)
	}
	printAnalysis(ctx, ops)
	return nil
}
