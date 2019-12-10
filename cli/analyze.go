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
	cli.StringFlag{
		Name:  "analyze.host",
		Value: "",
		Usage: "Only output for this host.",
	},
	cli.BoolFlag{
		Name:  "analyze.hostdetails",
		Usage: "Do detailed time segmentation per host",
	},
	cli.BoolFlag{
		Name:  "requests",
		Usage: "Display individual request stats.",
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
	if onlyHost := ctx.String("analyze.host"); onlyHost != "" {
		ops = ops.FilterByEndpoint(onlyHost)
	}
	hostDetails := ctx.Bool("analyze.hostdetails") && ops.Hosts() > 1

	ops.SortByStartTime()
	types := ops.OpTypes()
	for _, typ := range types {
		if wantOp := ctx.String("analyze.op"); wantOp != "" {
			if wantOp != typ {
				continue
			}
		}
		ops := ops.FilterByOp(typ)
		console.Println("-------------------")
		segs := ops.Segment(bench.SegmentOptions{
			From:           time.Time{},
			PerSegDuration: analysisDur(ctx),
			AllThreads:     true,
		})
		if len(segs) <= 1 {
			console.Println("Skipping", typ, "too few samples.")
			continue
		}
		totals := ops.Total(true)
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
		if ctx.Bool("requests") {
			printRequestAnalysis(ctx, ops)
			console.SetColor("Print", color.New(color.FgHiWhite))
			console.Println("\nThroughput:")
		}
		console.SetColor("Print", color.New(color.FgWhite))
		console.Println("* Average:", totals)

		if eps := ops.Endpoints(); len(eps) > 1 {
			console.SetColor("Print", color.New(color.FgHiWhite))
			console.Println("\nThroughput by host:")

			for _, ep := range eps {
				totals := ops.FilterByEndpoint(ep).Total(false)
				console.SetColor("Print", color.New(color.FgWhite))
				console.Print(" * ", ep, ": Avg: ", totals.ShortString(), "\n")
				if hostDetails {
					ops := ops.FilterByEndpoint(ep)
					segs := ops.Segment(bench.SegmentOptions{
						From:           time.Time{},
						PerSegDuration: analysisDur(ctx),
						AllThreads:     true,
					})
					if len(segs) <= 1 {
						console.Println("Skipping", typ, "host:", ep, " - Too few samples.")
						continue
					}
					totals := ops.Total(true)
					if totals.TotalBytes > 0 {
						segs.SortByThroughput()
					} else {
						segs.SortByObjsPerSec()
					}
					console.SetColor("Print", color.New(color.FgWhite))
					console.Println("\t- Fastest:", segs.Median(1).ShortString())
					console.Println("\t- 50% Median:", segs.Median(0.5).ShortString())
					console.Println("\t- Slowest:", segs.Median(0.0).ShortString())
				}
			}
		}
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Println("\nAggregated Throughput, split into", len(segs), "x", analysisDur(ctx), "time segments:")
		console.SetColor("Print", color.New(color.FgWhite))
		console.Println(" * Fastest:", segs.Median(1))
		console.Println(" * 50% Median:", segs.Median(0.5))
		console.Println(" * Slowest:", segs.Median(0.0))
		if wrSegs != nil {
			segs.SortByTime()
			err := segs.CSV(wrSegs)
			errorIf(probe.NewError(err), "Error writing analysis")

			// Write segments per endpoint
			eps := ops.Endpoints()
			if hostDetails && len(eps) > 1 {
				for _, ep := range eps {
					ops := ops.FilterByEndpoint(ep)
					segs := ops.Segment(bench.SegmentOptions{
						From:           time.Time{},
						PerSegDuration: analysisDur(ctx),
						AllThreads:     true,
					})
					if len(segs) <= 1 {
						continue
					}
					totals := ops.Total(true)
					if totals.TotalBytes > 0 {
						segs.SortByThroughput()
					} else {
						segs.SortByObjsPerSec()
					}
					segs.SortByTime()
					err := segs.CSV(wrSegs)
					errorIf(probe.NewError(err), "Error writing analysis")
				}
			}
		}
	}
}

func printRequestAnalysis(ctx *cli.Context, ops bench.Operations) {
	console.SetColor("Print", color.New(color.FgHiWhite))
	start, end := ops.ActiveTimeRange(true)
	active := ops.FilterInsideRange(start, end)
	console.Print("\nRequests - ", len(active), ":\n")
	console.SetColor("Print", color.New(color.FgWhite))

	if len(active) == 0 {
		console.Println("Not enough requests")
	}
	active.SortByDuration()
	console.Println(" * Fastest:", active.Median(0).Duration(),
		"Slowest:", active.Median(1).Duration(),
		"50%:", active.Median(0.5).Duration(),
		"90%:", active.Median(0.9).Duration(),
		"99%:", active.Median(0.99).Duration())

	ttfb := active.TTFB(start, end)
	if ttfb.Average > 0 {
		console.Println(" * First Byte:", ttfb)
	}
	if eps := ops.Endpoints(); len(eps) > 1 {
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Println("\nRequests by host:")

		for _, ep := range eps {
			filtered := ops.FilterByEndpoint(ep)
			if len(filtered) <= 1 {
				continue
			}
			filtered.SortByDuration()
			console.SetColor("Print", color.New(color.FgWhite))
			console.Println(" *", ep, "-", len(filtered), "requests:",
				"\n\t- Fastest:", filtered.Median(0).Duration(),
				"Slowest:", filtered.Median(1).Duration(),
				"50%:", filtered.Median(0.5).Duration(),
				"90%:", filtered.Median(0.9).Duration())
			ttfb := filtered.TTFB(filtered.TimeRange())
			if ttfb.Average > 0 {
				console.Println("\t- First Byte:", ttfb)
			}
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
