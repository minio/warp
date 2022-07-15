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
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/console"
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

// mainAnalyze is the entry point for analyze command.
func mainMerge(ctx *cli.Context) error {
	checkMerge(ctx)
	args := ctx.Args()
	if len(args) <= 1 {
		console.Fatal("Two or more benchmark data files must be supplied")
	}
	zstdDec, _ := zstd.NewReader(nil)
	defer zstdDec.Close()
	var allOps bench.Operations
	threads := uint16(0)
	log := console.Printf
	if globalQuiet {
		log = nil
	}
	for _, arg := range args {
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
	for typ, ops := range allOps.ByOp() {
		start, end := ops.ActiveTimeRange(true)
		if !start.Before(end) {
			console.Errorf("Type %v contains no overlapping items", typ)
		}
	}
	return nil
}

func checkMerge(ctx *cli.Context) {
}
