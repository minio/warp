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
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/fatih/color"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/warp/pkg/bench"
)

var analyzeFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "analyze.dur",
		Value: "1s",
		Usage: "Split analysis into durations of this length",
	},
	cli.StringFlag{
		Name:  "analyze.out",
		Value: "",
		Usage: "Output aggregated data as to file",
	},
	cli.StringFlag{
		Name:  "analyze.op",
		Value: "",
		Usage: "Only output for this op. Can be GET/PUT/DELETE, etc.",
	},
}

var analyzeCmd = cli.Command{
	Name:   "analyze",
	Usage:  "analyze existing benchmark data",
	Action: mainAnalyze,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] benchmark-data-file

Use - as input to read from stdin.

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
...
 `,
}

// mainAnalyze is the entry point for analyze command.
func mainAnalyze(ctx *cli.Context) error {
	checkAnalyze(ctx)
	args := ctx.Args()
	if len(args) == 0 {
		console.Fatal("No benchmark data file supplied")
	}
	if len(args) > 1 {
		console.Fatal("Only one benchmark file can be given")
	}
	var zstdDec, _ = zstd.NewReader(nil)
	defer zstdDec.Close()
	for _, arg := range args {
		var input io.Reader
		if arg == "-" {
			input = os.Stdin
		} else {
			f, err := os.Open(arg)
			fatalIf(probe.NewError(err), "Unable to open input file")
			defer f.Close()
			input = f
		}
		err := zstdDec.Reset(input)
		fatalIf(probe.NewError(err), "Unable to decompress input")
		b, err := ioutil.ReadAll(zstdDec)
		fatalIf(probe.NewError(err), "Unable to read input")
		ops, err := bench.OperationsFromCSV(bytes.NewBuffer(b))
		fatalIf(probe.NewError(err), "Unable to parse input")
		printAnalysis(ctx, ops)
	}
	return nil
}

func printAnalysis(ctx *cli.Context, ops bench.Operations) {
	var wrSegs io.Writer
	if fn := ctx.String("analyze.out"); fn != "" {
		if fn == "-" {
			wrSegs = os.Stdout
		} else {
			f, err := os.Create(fn)
			fatalIf(probe.NewError(err), "Unable to create create analysis output")
			defer console.Println("Aggregated data saved to", fn)
			defer f.Close()
			wrSegs = f
		}
	}

	for typ, ops := range ops.ByOp() {
		if wantOp := ctx.String("analyze.op"); wantOp != "" {
			if wantOp != typ {
				continue
			}
		}
		console.Println("-------------------")
		segs := ops.Segment(bench.SegmentOptions{
			From:           time.Time{},
			PerSegDuration: analysisDur(ctx),
		})
		if len(segs) <= 1 {
			console.Println("Skipping", typ, "too few samples.")
			continue
		}
		totals, ttfb := ops.Total()
		if totals.TotalBytes > 0 {
			segs.SortByThroughput()
		} else {
			segs.SortByObjsPerSec()
		}

		opo := ops.FirstObjPerOp()
		console.SetColor("Print", color.New(color.FgHiWhite))
		if opo > 1 {
			console.Printf("Operation: %v. Objects per operation: %d. Concurrency: %d. Hosts: %d.\n", typ, opo, ops.Threads(), ops.Hosts())
		} else {
			console.Printf("Operation: %v. Concurrency: %d. Hosts: %d.\n", typ, ops.Threads(), ops.Hosts())
		}
		if errs := ops.Errors(); len(errs) > 0 {
			console.SetColor("Print", color.New(color.FgHiRed))
			console.Println("Errors:", len(errs))
		}
		console.SetColor("Print", color.New(color.FgWhite))
		console.Println("* Average:", totals)
		if ttfb.Average > 0 {
			console.Println("* First Byte:", ttfb)
		}
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Println("\nAggregated, split into", len(segs), "x", analysisDur(ctx), "time segments:")
		console.SetColor("Print", color.New(color.FgWhite))
		console.Println("* Fastest:", segs.Median(1))
		console.Println("* 50% Median:", segs.Median(0.5))
		console.Println("* Slowest:", segs.Median(0.0))
		if wrSegs != nil {
			segs.SortByTime()
			err := segs.CSV(wrSegs)
			errorIf(probe.NewError(err), "Error writing analysis")
		}
	}
}

// analysisDur returns the analysis duration or 0 if un-parsable.
func analysisDur(ctx *cli.Context) time.Duration {
	d, err := time.ParseDuration(ctx.String("analyze.dur"))
	fatalIf(probe.NewError(err), "Invalid -analyze.dur value")
	return d
}

func checkAnalyze(ctx *cli.Context) {
	if analysisDur(ctx) == 0 {
		err := errors.New("-analyze.dur cannot be 0")
		fatal(probe.NewError(err), "Invalid -analyze.dur value")
	}
}
