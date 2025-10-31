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
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/v3/console"

	"github.com/minio/warp/pkg/aggregate"
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
	log := console.Printf
	if globalQuiet {
		log = nil
	}
	// Open "before" and "after" files
	rc, agg := openInput(args[0])
	defer rc.Close()
	rc2, agg2 := openInput(args[1])
	defer rc2.Close()

	if agg != agg2 {
		fatalIf(probe.NewError(errors.New("mixed input types")), "mixed input types (aggregated and non-aggregated)")
	}

	if agg {
		var before, after aggregate.Realtime
		if err := json.NewDecoder(rc).Decode(&before); err != nil {
			fatalIf(probe.NewError(err), "Unable to parse input")
		}
		if err := json.NewDecoder(rc2).Decode(&after); err != nil {
			fatalIf(probe.NewError(err), "Unable to parse input")
		}
		printCompare(ctx, before, after)
		return nil
	}

	if log != nil {
		log("Loading %q", args[0])
	}

	beforeOps, err := bench.OperationsFromCSV(rc, true, ctx.Int("analyze.offset"), ctx.Int("analyze.limit"), log)
	fatalIf(probe.NewError(err), "Unable to parse input")

	if log != nil {
		log("Loading %q", args[1])
	}
	afterOps, err := bench.OperationsFromCSV(rc, true, ctx.Int("analyze.offset"), ctx.Int("analyze.limit"), log)
	fatalIf(probe.NewError(err), "Unable to parse input")

	printCompareLegacy(ctx, beforeOps, afterOps)
	return nil
}

func printCompare(ctx *cli.Context, before, after aggregate.Realtime) {
	afterOps := after.ByOpType
	ops := mapKeys(before.ByOpType)
	sort.Strings(ops)
	for _, typ := range ops {
		before := before.ByOpType[typ]
		if wantOp := ctx.String("analyze.op"); wantOp != "" {
			if strings.ToUpper(wantOp) != typ {
				continue
			}
		}

		console.Println("-------------------")
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Println("Operation:", typ)
		console.SetColor("Print", color.New(color.FgWhite))

		after := afterOps[typ]
		cmp, err := aggregate.Compare(before, after, typ)
		if err != nil {
			console.Println(err)
			continue
		}

		if bErrs, aErrs := before.TotalErrors, after.TotalErrors; bErrs+aErrs > 0 {
			console.SetColor("Print", color.New(color.FgHiRed))
			console.Println("Errors:", bErrs, "->", aErrs)
			console.SetColor("Print", color.New(color.FgWhite))
		}
		if before.TotalRequests != after.TotalRequests {
			console.Println("Operations:", before.TotalRequests, "->", after.TotalRequests)
		}
		if before.Concurrency != after.Concurrency {
			console.Println("Concurrency:", before.Concurrency, "->", after.Concurrency)
		}
		if len(before.ThroughputByHost) != len(after.ThroughputByHost) {
			console.Println("Endpoints:", len(before.ThroughputByHost), "->", len(after.ThroughputByHost))
		}
		opoB := before.TotalObjects / before.TotalRequests
		opoA := after.TotalObjects / after.TotalRequests
		if opoB != opoA {
			console.Println("Objects per operation:", opoB, "->", opoA)
		}
		bDur := before.EndTime.Sub(before.StartTime).Round(time.Second)
		aDur := after.EndTime.Sub(after.StartTime).Round(time.Second)
		if bDur != aDur {
			console.Println("Duration:", bDur, "->", aDur)
		}
		if cmp.Reqs.Before.AvgObjSize != cmp.Reqs.After.AvgObjSize {
			console.Printf("Object size: %v -> %v\n",
				humanize.Bytes(uint64(cmp.Reqs.Before.AvgObjSize)),
				humanize.Bytes(uint64(cmp.Reqs.After.AvgObjSize)))
		}
		console.Println("* Average:", cmp.Average)
		console.Println("* Requests:", cmp.Reqs.String())

		if cmp.TTFB != nil {
			console.Println("* TTFB:", cmp.TTFB)
		}
		if cmp.Fastest.ObjPerSec > 0 {
			console.SetColor("Print", color.New(color.FgWhite))
			console.Println("* Fastest:", cmp.Fastest)
			console.Println("* 50% Median:", cmp.Median)
			console.Println("* Slowest:", cmp.Slowest)
		}
	}
}

func printCompareLegacy(ctx *cli.Context, before, after bench.Operations) {
	isMultiOp := before.IsMixed()
	if isMultiOp != after.IsMixed() {
		console.Fatal("Cannot compare multi-operation to single operation.")
	}
	timeDur := func(ops bench.Operations) time.Duration {
		start, end := ops.ActiveTimeRange(!isMultiOp)
		return end.Sub(start).Round(time.Second)
	}
	afterOps := after.SortSplitByOpType()
	for typ, before := range before.SortSplitByOpType() {
		if wantOp := ctx.String("analyze.op"); wantOp != "" {
			if strings.ToUpper(wantOp) != typ {
				continue
			}
		}

		after := afterOps[typ]
		console.Println("-------------------")
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Println("Operation:", typ)
		console.SetColor("Print", color.New(color.FgWhite))

		cmp, err := bench.Compare(before, after, analysisDur(ctx, before.Duration()), !isMultiOp)
		if err != nil {
			console.Println(err)
			continue
		}
		if bErrs, aErrs := before.NErrors(), after.NErrors(); bErrs+aErrs > 0 {
			console.SetColor("Print", color.New(color.FgHiRed))
			console.Println("Errors:", bErrs, "->", aErrs)
			console.SetColor("Print", color.New(color.FgWhite))
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
			console.Printf("Object size: %d->%d, \n", cmp.Reqs.Before.AvgObjSize, cmp.Reqs.After.AvgObjSize)
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

func mapKeys[Map ~map[K]V, K comparable, V any](m Map) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
