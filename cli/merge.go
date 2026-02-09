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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"
)

var mergeFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "benchdata",
		Value: "",
		Usage: "Output combined data to this file. By default unique filename is generated.",
	},
}

var mergeCmd = cli.Command{
	Name:   "merge",
	Usage:  "merge existing benchmark data",
	Action: mainMerge,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, mergeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] benchmark-data-file1 benchmark-data-file2 ...
  -> see https://github.com/minio/warp#merging-benchmarks

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

func mainMerge(ctx *cli.Context) error {
	checkMerge(ctx)
	args := ctx.Args()
	if len(args) <= 1 {
		console.Fatal("Two or more benchmark data files must be supplied")
	}

	rc, isJSON := openInput(args[0])
	rc.Close()

	if isJSON {
		return mergeJSON(ctx, args)
	}
	return mergeCSV(ctx, args)
}

func timeOverlaps(a, b aggregate.LiveAggregate) bool {
	return a.StartTime.Before(b.EndTime) && b.StartTime.Before(a.EndTime)
}

func mergeJSON(ctx *cli.Context, args []string) error {
	var merged aggregate.Realtime
	for i, arg := range args {
		rc, isJSON := openInput(arg)
		if !isJSON {
			rc.Close()
			fatalIf(probe.NewError(errors.New("mixed input types")), "mixed input types (JSON and CSV)")
		}
		var rt aggregate.Realtime
		if err := json.NewDecoder(rc).Decode(&rt); err != nil {
			rc.Close()
			fatalIf(probe.NewError(err), "Unable to parse input")
		}
		rc.Close()
		if i > 0 && !timeOverlaps(merged.Total, rt.Total) {
			fatalIf(probe.NewError(fmt.Errorf(
				"file %q (%s - %s) does not overlap with merged range (%s - %s)",
				arg, rt.Total.StartTime.Format(time.RFC3339), rt.Total.EndTime.Format(time.RFC3339),
				merged.Total.StartTime.Format(time.RFC3339), merged.Total.EndTime.Format(time.RFC3339),
			)), "time ranges do not overlap")
		}
		merged.Merge(&rt)
	}
	merged.Final = true

	fileName := ctx.String("benchdata")
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s", appName, ctx.Command.Name, time.Now().Format("2006-01-02[150405]"))
	}

	f, err := os.Create(fileName + ".json.zst")
	if err != nil {
		console.Error("Unable to write benchmark data:", err)
		return nil
	}
	defer f.Close()
	enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	fatalIf(probe.NewError(err), "Unable to compress benchmark output")
	defer enc.Close()

	js := json.NewEncoder(enc)
	js.SetIndent("", "  ")
	err = js.Encode(merged)
	fatalIf(probe.NewError(err), "Unable to write benchmark output")

	console.Infof("Benchmark data written to %q\n", fileName+".json.zst")
	return nil
}

func mergeCSV(ctx *cli.Context, args []string) error {
	zstdDec, _ := zstd.NewReader(nil)
	defer zstdDec.Close()
	var allOps bench.Operations
	threads := uint32(0)
	log := console.Printf
	if globalQuiet {
		log = nil
	}
	for _, arg := range args {
		rc, isJSON := openInput(arg)
		if isJSON {
			rc.Close()
			fatalIf(probe.NewError(errors.New("mixed input types")), "mixed input types (JSON and CSV)")
		}
		rc.Close()

		f, err := os.Open(arg)
		fatalIf(probe.NewError(err), "Unable to open input file")
		defer f.Close()
		err = zstdDec.Reset(f)
		fatalIf(probe.NewError(err), "Unable to decompress input")
		ops, err := bench.OperationsFromCSV(zstdDec, false, ctx.Int("analyze.offset"), ctx.Int("analyze.limit"), log)
		fatalIf(probe.NewError(err), "Unable to parse input")

		threads = ops.OffsetThreads(threads)
		allOps = append(allOps, ops...)
	}
	if len(allOps) == 0 {
		return errors.New("benchmark files contains no data")
	}
	fileName := ctx.String("benchdata")
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s", appName, ctx.Command.Name, time.Now().Format("2006-01-02[150405]"))
	}
	if len(allOps) > 0 {
		allOps.SortByStartTime()
		f, err := os.Create(fileName + ".csv.zst")
		if err != nil {
			console.Error("Unable to write benchmark data:", err)
		} else {
			func() {
				defer f.Close()
				enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
				fatalIf(probe.NewError(err), "Unable to compress benchmark output")

				defer enc.Close()
				err = allOps.CSV(enc, commandLine(ctx))
				fatalIf(probe.NewError(err), "Unable to write benchmark output")

				console.Infof("Benchmark data written to %q\n", fileName+".csv.zst")
			}()
		}
	}
	for typ, ops := range allOps.SortSplitByOpType() {
		start, end := ops.ActiveTimeRange(true)
		if !start.Before(end) {
			console.Errorf("Type %v contains no overlapping items", typ)
		}
	}
	return nil
}

func checkMerge(_ *cli.Context) {
}
