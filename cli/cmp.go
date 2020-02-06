/*
 * Warp (C) 2019 MinIO, Inc.
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

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
...
 `,
}

// mainAnalyze is the entry point for analyze command.
func mainCmp(ctx *cli.Context) error {
	checkAnalyze(ctx)
	checkCmp(ctx)
	args := ctx.Args()
	var zstdDec, _ = zstd.NewReader(nil)
	defer zstdDec.Close()
	readOps := func(s string) bench.Operations {
		f, err := os.Open(s)
		fatalIf(probe.NewError(err), "Unable to open input file")
		defer f.Close()
		err = zstdDec.Reset(f)
		fatalIf(probe.NewError(err), "Unable to decompress input")
		b, err := ioutil.ReadAll(zstdDec)
		fatalIf(probe.NewError(err), "Unable to read input")
		ops, err := bench.OperationsFromCSV(bytes.NewBuffer(b))
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

		cmp, err := bench.Compare(before, after, analysisDur(ctx), !isMultiOp)
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
		console.Println("* Average:", cmp.Average)
		if cmp.TTFB != nil {
			console.Println("* First Byte:", cmp.TTFB)
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
