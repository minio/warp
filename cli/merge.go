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
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/mc/pkg/probe"
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

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
...
 `,
}

// mainAnalyze is the entry point for analyze command.
func mainMerge(ctx *cli.Context) error {
	checkMerge(ctx)
	args := ctx.Args()
	if len(args) <= 1 {
		console.Fatal("Two or more benchmark data files must be supplied")
	}
	var zstdDec, _ = zstd.NewReader(nil)
	defer zstdDec.Close()
	var allOps bench.Operations
	threads := uint16(0)
	for _, arg := range args {
		f, err := os.Open(arg)
		fatalIf(probe.NewError(err), "Unable to open input file")
		defer f.Close()
		err = zstdDec.Reset(f)
		fatalIf(probe.NewError(err), "Unable to decompress input")
		b, err := ioutil.ReadAll(zstdDec)
		fatalIf(probe.NewError(err), "Unable to read input")
		ops, err := bench.OperationsFromCSV(bytes.NewBuffer(b))
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
			enc, err := zstd.NewWriter(f)
			fatalIf(probe.NewError(err), "Unable to compress benchmark output")

			defer enc.Close()
			err = allOps.CSV(enc)
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
