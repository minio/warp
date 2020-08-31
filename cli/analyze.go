/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/api"
	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"
)

var analyzeFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "analyze.dur",
		Value: "",
		Usage: "Split analysis into durations of this length. Can be '1s', '5s', '1m', etc.",
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
	cli.StringFlag{
		Name:   serverFlagName,
		Usage:  "When running benchmarks open a webserver on this ip:port and keep it running afterwards.",
		Value:  "",
		Hidden: true,
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
  -> see https://github.com/minio/warp#analysis

Use - as input to read from stdin.

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
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
	monitor := api.NewBenchmarkMonitor(ctx.String(serverFlagName))
	defer monitor.Done()
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
		monitor.OperationsReady(ops, strings.TrimSuffix(filepath.Base(arg), ".csv.zst"), commandLine(ctx))
	}
	return nil
}

func printMixedOpAnalysis(ctx *cli.Context, aggr aggregate.Aggregated) {
	console.SetColor("Print", color.New(color.FgWhite))
	console.Println("Mixed operations.")

	if aggr.MixedServerStats == nil {
		console.Errorln("No mixed stats")
	}
	for _, ops := range aggr.Operations {
		console.Println("")
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Println("Operation:", ops.Type)
		console.SetColor("Print", color.New(color.FgWhite))

		if ops.Skipped {
			console.Println("Skipping", ops.Type, "too few samples.")
			continue
		}
		pct := 100.0 * float64(ops.Throughput.Operations) / float64(aggr.MixedServerStats.Operations)
		console.Printf(" * Operations: %v (%.01f%% of operations)\n", ops.Throughput.Operations, pct)

		if ops.Errors > 0 {
			console.SetColor("Print", color.New(color.FgHiRed))
			console.Println("Errors:", ops.Errors)
			if ctx.Bool("analyze.errors") {
				for _, err := range ops.FirstErrors {
					console.Println(err)
				}
			}
			console.SetColor("Print", color.New(color.FgWhite))
		}
		eps := ops.ThroughputByHost
		if len(eps) == 1 {
			console.Println(" * Throughput:", ops.Throughput)
		}

		if len(eps) > 1 {
			console.SetColor("Print", color.New(color.FgWhite))
			console.Println("\nThroughput by host:")

			for ep, totals := range eps {
				console.SetColor("Print", color.New(color.FgWhite))
				console.Print(" * ", ep, ": Avg: ", totals.String(), "\n")
				if totals.Errors > 0 {
					console.SetColor("Print", color.New(color.FgHiRed))
					console.Println("Errors:", totals.Errors)
				}
			}
		}

		if ctx.Bool("requests") {
			printRequestAnalysis(ctx, ops)
			console.SetColor("Print", color.New(color.FgWhite))
		}
	}
	console.SetColor("Print", color.New(color.FgHiWhite))
	console.Println("\nCluster Total: ", aggr.MixedServerStats.String())
	console.SetColor("Print", color.New(color.FgWhite))
	if eps := aggr.MixedThroughputByHost; len(eps) > 1 {
		for ep, ops := range eps {
			console.Println(" * "+ep+": ", ops.String())
		}
	}
}

func printAnalysis(ctx *cli.Context, o bench.Operations) {
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
		o = o.FilterByEndpoint(onlyHost)
	}

	if wantOp := ctx.String("analyze.op"); wantOp != "" {
		o = o.FilterByOp(wantOp)
	}

	aggr := aggregate.Aggregate(o, analysisDur(ctx, o.Duration()), ctx.Duration("analyze.skip"))
	isMixed := o.IsMixed()
	for _, ops := range aggr.Operations {
		writeSegs(ctx, wrSegs, o.FilterByOp(ops.Type), isMixed)
	}

	if globalJSON {
		b, err := json.MarshalIndent(aggr, "", "  ")
		fatalIf(probe.NewError(err), "Unable to marshal data.")
		if err != nil {
			console.Errorln(err)
		}
		os.Stdout.Write(b)
		return
	}

	if isMixed {
		printMixedOpAnalysis(ctx, aggr)
		return
	}
	hostDetails := ctx.Bool("analyze.hostdetails") && o.Hosts() > 1

	for _, ops := range aggr.Operations {
		typ := ops.Type
		console.Println("-------------------")
		if ops.Skipped {
			console.Println("Skipping", typ, "too few samples.")
			continue
		}

		opo := ops.ObjectsPerOperation
		console.SetColor("Print", color.New(color.FgHiWhite))
		if opo > 1 {
			console.Printf("Operation: %v (%d). Objects per operation: %d. Concurrency: %d. Hosts: %d.\n", typ, ops.N, opo, ops.Concurrency, ops.Hosts)
		} else {
			console.Printf("Operation: %v (%d). Concurrency: %d. Hosts: %d.\n", typ, ops.N, ops.Concurrency, ops.Hosts)
		}
		if ops.Errors > 0 {
			console.SetColor("Print", color.New(color.FgHiRed))
			console.Println("Errors:", ops.Errors)
			if ctx.Bool("analyze.errors") {
				for _, err := range ops.FirstErrors {
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
		console.Println("* Average:", ops.Throughput)

		if eps := ops.ThroughputByHost; len(eps) > 1 {
			console.SetColor("Print", color.New(color.FgHiWhite))
			console.Println("\nThroughput by host:")

			for ep, ops := range eps {
				console.SetColor("Print", color.New(color.FgWhite))
				console.Print(" * ", ep, ": Avg: ", ops, "\n")
				if ops.Errors > 0 {
					console.SetColor("Print", color.New(color.FgHiRed))
					console.Println("Errors:", ops.Errors)
				}
				if hostDetails {
					seg := ops.Segmented
					console.SetColor("Print", color.New(color.FgWhite))
					if seg == nil || len(seg.Segments) <= 1 {
						console.Println("Skipping", typ, "host:", ep, " - Too few samples.")
						continue
					}
					console.SetColor("Print", color.New(color.FgWhite))
					console.Println("\t- Fastest:", aggregate.BPSorOPS(seg.FastestBPS, seg.FastestOPS))
					console.Println("\t- 50% Median:", aggregate.BPSorOPS(seg.MedianBPS, seg.MedianOPS))
					console.Println("\t- Slowest:", aggregate.BPSorOPS(seg.SlowestBPS, seg.SlowestOPS))
				}
			}
		}
		segs := ops.Throughput.Segmented
		dur := time.Millisecond * time.Duration(segs.SegmentDurationMillis)
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Println("\nAggregated Throughput, split into", len(segs.Segments), "x", dur, "time segments:")
		console.SetColor("Print", color.New(color.FgWhite))
		console.Println(" * Fastest:", aggregate.SegmentSmall{BPS: segs.FastestBPS, OPS: segs.FastestOPS, Start: segs.FastestStart}.StringLong(dur))
		console.Println(" * 50% Median:", aggregate.SegmentSmall{BPS: segs.MedianBPS, OPS: segs.MedianOPS, Start: segs.MedianStart}.StringLong(dur))
		console.Println(" * Slowest:", aggregate.SegmentSmall{BPS: segs.SlowestBPS, OPS: segs.SlowestOPS, Start: segs.SlowestStart}.StringLong(dur))
	}
}

func writeSegs(ctx *cli.Context, wrSegs io.Writer, ops bench.Operations, allThreads bool) {
	if wrSegs == nil {
		return
	}
	totalDur := ops.Duration()
	segs := ops.Segment(bench.SegmentOptions{
		From:           time.Time{},
		PerSegDuration: analysisDur(ctx, totalDur),
		AllThreads:     allThreads,
	})

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
				PerSegDuration: analysisDur(ctx, totalDur),
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

func printRequestAnalysis(ctx *cli.Context, ops aggregate.Operation) {
	console.SetColor("Print", color.New(color.FgHiWhite))
	hostDetails := ctx.Bool("analyze.hostdetails")

	if ops.SingleSizedRequests != nil {
		reqs := *ops.SingleSizedRequests
		// Single type, require one operation per thread.

		console.Print("\nRequests considered: ", reqs.Requests, ":\n")
		console.SetColor("Print", color.New(color.FgWhite))

		if reqs.Skipped {
			fmt.Println(reqs)
			console.Println("Not enough requests")
			return
		}

		console.Println(
			" * Avg:", time.Duration(reqs.DurAvgMillis)*time.Millisecond,
			"50%:", time.Duration(reqs.DurMedianMillis)*time.Millisecond,
			"90%:", time.Duration(reqs.Dur90Millis)*time.Millisecond,
			"99%:", time.Duration(reqs.Dur99Millis)*time.Millisecond,
			"Fastest:", time.Duration(reqs.FastestMillis)*time.Millisecond,
			"Slowest:", time.Duration(reqs.SlowestMillis)*time.Millisecond,
		)

		if reqs.FirstByte != nil {
			console.Println(" * First Byte:", reqs.FirstByte)
		}
		if eps := reqs.ByHost; len(eps) > 1 && hostDetails {
			console.SetColor("Print", color.New(color.FgHiWhite))
			console.Println("\nRequests by host:")

			for ep, reqs := range eps {
				if reqs.Requests <= 1 {
					continue
				}
				console.SetColor("Print", color.New(color.FgWhite))
				console.Println(" *", ep, "-", reqs.Requests, "requests:",
					"\n\t- Avg:", time.Duration(reqs.DurAvgMillis)*time.Millisecond,
					"Fastest:", time.Duration(reqs.FastestMillis)*time.Millisecond,
					"Slowest:", time.Duration(reqs.SlowestMillis)*time.Millisecond,
					"50%:", time.Duration(reqs.DurMedianMillis)*time.Millisecond,
					"90%:", time.Duration(reqs.Dur90Millis)*time.Millisecond)
				if reqs.FirstByte != nil {
					console.Println("\t- First Byte:", reqs.FirstByte)
				}
			}
		}
		return
	}

	// Multi sized
	if ops.MultiSizedRequests == nil {
		console.Fatalln("Neither single-sized nor multi-sized requests found")
	}
	reqs := *ops.MultiSizedRequests
	console.Print("\nRequests considered: ", reqs.Requests, ". Multiple sizes, average ", reqs.AvgObjSize, " bytes:\n")
	console.SetColor("Print", color.New(color.FgWhite))

	if reqs.Skipped {
		console.Println("Not enough requests")
	}

	sizes := reqs.BySize
	for _, s := range sizes {

		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Print("\nRequest size ", s.MinSizeString, " -> ", s.MaxSizeString, ". Requests - ", s.Requests, ":\n")
		console.SetColor("Print", color.New(color.FgWhite))

		console.Print(""+
			" * Throughput: Average: ", bench.Throughput(s.BpsAverage),
			", 50%: ", bench.Throughput(s.BpsMedian),
			", 90%: ", bench.Throughput(s.Bps90),
			", 99%: ", bench.Throughput(s.Bps99),
			", Fastest: ", bench.Throughput(s.BpsFastest),
			", Slowest: ", bench.Throughput(s.BpsSlowest),
			"\n")
		if s.FirstByte != nil {
			console.Println(" * First Byte:", s.FirstByte)
		}
	}
	if eps := reqs.ByHost; len(eps) > 1 && hostDetails {
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Println("\nRequests by host:")

		for ep, s := range eps {
			if s.Requests <= 1 {
				continue
			}
			console.SetColor("Print", color.New(color.FgWhite))
			console.Println(" *", ep, "-", s.Requests, "requests:",
				"\n\t- Avg:", bench.Throughput(s.BpsAverage),
				"Fastest:", bench.Throughput(s.BpsFastest),
				"Slowest:", bench.Throughput(s.BpsSlowest),
				"50%:", bench.Throughput(s.BpsMedian),
				"90%:", bench.Throughput(s.Bps90))
			if s.FirstByte != nil {
				console.Println(" * First Byte:", s.FirstByte)
			}
		}
	}
}

// analysisDur returns the analysis duration or 0 if un-parsable.
func analysisDur(ctx *cli.Context, total time.Duration) time.Duration {
	dur := ctx.String("analyze.dur")
	if dur == "" {
		if total == 0 {
			return 0
		}
		// Find appropriate duration
		// We want the smallest segmentation duration that produces at most this number of segments.
		const wantAtMost = 400

		// Standard durations to try:
		stdDurations := []time.Duration{time.Second, 5 * time.Second, 15 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour, 3 * time.Hour}
		for _, d := range stdDurations {
			dur = d.String()
			if total/d <= wantAtMost {
				break
			}
		}
	}
	d, err := time.ParseDuration(dur)
	fatalIf(probe.NewError(err), "Invalid -analyze.dur value")
	return d
}

func checkAnalyze(ctx *cli.Context) {
	if analysisDur(ctx, time.Minute) == 0 {
		err := errors.New("-analyze.dur cannot be 0")
		fatal(probe.NewError(err), "Invalid -analyze.dur value")
	}
}
