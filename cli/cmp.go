/*
 * Warp (C) 2019 MinIO, Inc.
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
	"io"
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var cmpFlags = []cli.Flag{}

var cmpCmd = cli.Command{
	Name:   "cmp",
	Usage:  "compare existing benchmark data",
	Action: mainCmp,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, analyzeFlags, cmpFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] before-benchmark-data-file after-benchmark-data-file
  -> see https://github.com/minio/warp#comparing-benchmarks

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainAnalyze is the entry point for analyze command.
func mainCmp(ctx *cli.Context) error {
	checkAnalyze(ctx)
	checkCmp(ctx)
	args := ctx.Args()
	zstdDec, _ := zstd.NewReader(nil)
	defer zstdDec.Close()
	log := console.Printf
	if globalQuiet {
		log = nil
	}
	readOps := func(s string) bench.Operations {
		f, err := os.Open(s)
		fatalIf(probe.NewError(err), "Unable to open input file")
		defer f.Close()
		err = zstdDec.Reset(f)
		fatalIf(probe.NewError(err), "Unable to read input")
		ops, err := bench.OperationsFromCSV(zstdDec, true, ctx.Int("analyze.offset"), ctx.Int("analyze.limit"), log)
		fatalIf(probe.NewError(err), "Unable to parse input")
		return ops
	}
	printCompare(ctx, readOps(args[0]), readOps(args[1]))
	return nil
}

func printCompare(ctx *cli.Context, before, after bench.Operations) {
	var wrSegs io.Writer

	if fn := ctx.String("compare.out"); fn != "" {
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
	_ = wrSegs
	isMultiOp := before.IsMixed()
	if isMultiOp != after.IsMixed() {
		console.Fatal("Cannot compare multi-operation to single operation.")
	}
	timeDur := func(ops bench.Operations) time.Duration {
		start, end := ops.ActiveTimeRange(!isMultiOp)
		return end.Sub(start).Round(time.Second)
	}

	for _, typ := range before.OpTypes() {
		if wantOp := ctx.String("analyze.op"); wantOp != "" {
			if wantOp != typ {
				continue
			}
		}
		before := before.FilterByOp(typ)
		after := after.FilterByOp(typ)
		console.Println("-------------------")
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Println("Operation:", typ)
		console.SetColor("Print", color.New(color.FgWhite))

		cmp, err := bench.Compare(before, after, analysisDur(ctx, before.Duration()), !isMultiOp)
		if err != nil {
			console.Println(err)
			continue
		}

		if len(before) != len(after) {
			console.Println("Operations:", len(before), "->", len(after))
		}
		if before.Threads() != after.Threads() {
			console.Println("Concurrency:", before.Threads(), "->", after.Threads())
		}
		if len(before.Endpoints()) != len(after.Endpoints()) {
			console.Println("Endpoints:", len(before.Endpoints()), "->", len(after.Endpoints()))
		}
		if !isMultiOp {
			if before.FirstObjPerOp() != after.FirstObjPerOp() {
				console.Println("Objects per operation:", before.FirstObjPerOp(), "->", after.FirstObjPerOp())
			}
		}
		if timeDur(before) != timeDur(after) {
			console.Println("Duration:", timeDur(before), "->", timeDur(after))
		}
		if cmp.Reqs.Before.AvgObjSize != cmp.Reqs.After.AvgObjSize {
			console.Println("Object size: %d->%d, ", cmp.Reqs.Before.AvgObjSize, cmp.Reqs.After.AvgObjSize)
		}
		console.Println("* Average:", cmp.Average)
		console.Println("* Requests:", cmp.Reqs.String())

		if cmp.TTFB != nil {
			console.Println("* TTFB:", cmp.TTFB)
		}
		if !isMultiOp {
			console.SetColor("Print", color.New(color.FgWhite))
			console.Println("* Fastest:", cmp.Fastest)
			console.Println("* 50% Median:", cmp.Median)
			console.Println("* Slowest:", cmp.Slowest)
		}
	}
}

func checkCmp(ctx *cli.Context) {
	if ctx.NArg() != 2 {
		console.Fatal("Two data sources must be supplied")
	}
}
