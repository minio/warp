/*
 * Warp (C) 2019-2020 MinIO, Inc.
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
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio/pkg/console"
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
	cli.BoolFlag{
		Name:  "analyze.errors",
		Usage: "Print out errors",
	},
	cli.StringFlag{
		Name:  "analyze.host",
		Value: "",
		Usage: "Only output for this host.",
	},
	cli.DurationFlag{
		Name:   "analyze.skip",
		Usage:  "Additional duration to skip when analyzing data.",
		Hidden: false,
		Value:  0,
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

func printMultiOpAnalysis(ctx *cli.Context, ops bench.Operations, wrSegs io.Writer) {
	allOps := ops
	console.SetColor("Print", color.New(color.FgWhite))
	console.Println("Mixed operations.")

	for _, typ := range ops.OpTypes() {
		console.Println("")
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Println("Operation:", typ)
		console.SetColor("Print", color.New(color.FgWhite))
		ops := ops.FilterByOp(typ)
		if d := ctx.Duration("analyze.skip"); d > 0 {
			start, end := ops.TimeRange()
			start = start.Add(d)
			ops = ops.FilterInsideRange(start, end)
		}
		total := ops.Total(false)
		if len(ops) <= 0 || total.EndsBefore.Sub(total.Start) < analysisDur(ctx) {
			console.Println("Skipping", typ, "too few samples.")
			continue
		}
		pct := 100.0 * float64(len(ops)) / float64(len(allOps))
		console.Printf(" * %v (%.01f%% of operations)\n", total, pct)

		if errs := ops.Errors(); len(errs) > 0 {
			console.SetColor("Print", color.New(color.FgHiRed))
			console.Println("Errors:", len(errs))
			if ctx.Bool("analyze.errors") {
				errs := ops.FilterErrors()
				for _, err := range errs {
					console.Println(err)
				}
			}
			console.SetColor("Print", color.New(color.FgWhite))
		}

		if eps := ops.Endpoints(); len(eps) > 1 {
			console.SetColor("Print", color.New(color.FgWhite))
			console.Println("\nThroughput by host:")

			for _, ep := range eps {
				totals := ops.FilterByEndpoint(ep).Total(false)
				console.SetColor("Print", color.New(color.FgWhite))
				console.Print(" * ", ep, ": Avg: ", totals.ShortString(), "\n")
				if errs := ops.Errors(); len(errs) > 0 {
					console.SetColor("Print", color.New(color.FgHiRed))
					console.Println("Errors:", len(errs))
				}
			}
		}

		if ctx.Bool("requests") {
			printRequestAnalysis(ctx, ops)
			console.SetColor("Print", color.New(color.FgWhite))
		}

		segs := ops.Segment(bench.SegmentOptions{
			From:           time.Time{},
			PerSegDuration: analysisDur(ctx),
			AllThreads:     false,
		})
		writeSegs(ctx, wrSegs, segs, ops, false)
	}
	console.SetColor("Print", color.New(color.FgHiWhite))
	console.Println("\nCluster Total: ", allOps.Total(true))
	console.SetColor("Print", color.New(color.FgWhite))
	if eps := ops.Endpoints(); len(eps) > 1 {
		for _, ep := range eps {
			console.Println(" * "+ep+": ", ops.FilterByEndpoint(ep).Total(false))
		}
	}
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

	if wantOp := ctx.String("analyze.op"); wantOp != "" {
		ops = ops.FilterByOp(wantOp)
	}

	if ops.IsMultiOp() {
		printMultiOpAnalysis(ctx, ops, wrSegs)
		return
	}

	hostDetails := ctx.Bool("analyze.hostdetails") && ops.Hosts() > 1
	ops.SortByStartTime()
	types := ops.OpTypes()
	for _, typ := range types {
		ops := ops.FilterByOp(typ)
		if d := ctx.Duration("analyze.skip"); d > 0 {
			start, end := ops.TimeRange()
			start = start.Add(d)
			ops = ops.FilterInsideRange(start, end)
		}
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
			if ctx.Bool("analyze.errors") {
				errs := ops.FilterErrors()
				for _, err := range errs {
					console.Println(err)
				}
			}
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
				if errs := ops.Errors(); len(errs) > 0 {
					console.SetColor("Print", color.New(color.FgHiRed))
					console.Println("Errors:", len(errs))
				}
				if hostDetails {
					ops := ops.FilterByEndpoint(ep)
					segs := ops.Segment(bench.SegmentOptions{
						From:           time.Time{},
						PerSegDuration: analysisDur(ctx),
						AllThreads:     false,
					})
					console.SetColor("Print", color.New(color.FgWhite))
					if len(segs) <= 1 {
						console.Println("Skipping", typ, "host:", ep, " - Too few samples.")
						continue
					}
					totals := ops.Total(false)
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
		writeSegs(ctx, wrSegs, segs, ops, true)
	}
}

func writeSegs(ctx *cli.Context, wrSegs io.Writer, segs bench.Segments, ops bench.Operations, allThreads bool) {
	if wrSegs != nil {
		hostDetails := ctx.Bool("analyze.hostdetails") && ops.Hosts() > 1
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
					AllThreads:     allThreads,
				})
				if len(segs) <= 1 {
					continue
				}
				totals := ops.Total(allThreads)
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

func printRequestAnalysis(ctx *cli.Context, ops bench.Operations) {
	console.SetColor("Print", color.New(color.FgHiWhite))
	start, end := ops.ActiveTimeRange(false)
	active := ops.FilterInsideRange(start, end)
	if !active.MultipleSizes() {
		// Single type, require one operation per thread.
		start, end = ops.ActiveTimeRange(true)
		active = ops.FilterInsideRange(start, end)

		console.Print("\nRequests considered: ", len(active), ":\n")
		console.SetColor("Print", color.New(color.FgWhite))

		if len(active) == 0 {
			console.Println("Not enough requests")
		}

		active.SortByDuration()

		console.Println(
			" * Avg:", active.AvgDuration().Round(time.Millisecond),
			"50%:", active.Median(0.5).Duration().Round(time.Millisecond),
			"90%:", active.Median(0.9).Duration().Round(time.Millisecond),
			"99%:", active.Median(0.99).Duration().Round(time.Millisecond),
			"Fastest:", active.Median(0).Duration().Round(time.Millisecond),
			"Slowest:", active.Median(1).Duration().Round(time.Millisecond),
		)

		ttfb := active.TTFB(start, end)

		if ttfb.Average > 0 {
			console.Println(" * First Byte:", ttfb)
		}
	} else {
		console.Print("\nRequests considered: ", len(active), ". Multiple sizes, average ", active.AvgSize(), " bytes:\n")
		console.SetColor("Print", color.New(color.FgWhite))

		if len(active) == 0 {
			console.Println("Not enough requests")
		}

		sizes := active.SplitSizes(0.05)
		for _, s := range sizes {
			active := s.Ops

			console.SetColor("Print", color.New(color.FgHiWhite))
			console.Print("\nRequest size ", s.SizeString(), ". Requests - ", len(active), ":\n")
			console.SetColor("Print", color.New(color.FgWhite))

			active.SortByThroughput()
			console.Print(""+
				" * Throughput: Average: ", active.OpThroughput(),
				", 50%: ", active.Median(0.5).BytesPerSec(),
				", 90%: ", active.Median(0.9).BytesPerSec(),
				", 99%: ", active.Median(0.99).BytesPerSec(),
				", Fastest: ", active.Median(0).BytesPerSec(),
				", Slowest: ", active.Median(1).BytesPerSec(),
				"\n")
			ttfb := active.TTFB(start, end)
			if ttfb.Average > 0 {
				console.Println(" * First Byte:", ttfb)
			}
		}
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
				"\n\t- Avg:", filtered.AvgDuration().Round(time.Millisecond),
				"Fastest:", filtered.Median(0).Duration().Round(time.Millisecond),
				"Slowest:", filtered.Median(1).Duration().Round(time.Millisecond),
				"50%:", filtered.Median(0.5).Duration().Round(time.Millisecond),
				"90%:", filtered.Median(0.9).Duration().Round(time.Millisecond))
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
