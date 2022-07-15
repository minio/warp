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
	"github.com/minio/cli"
	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var deleteFlags = []cli.Flag{
	cli.IntFlag{
		Name:  "objects",
		Value: 25000,
		Usage: "Number of objects to upload.",
	},
	cli.StringFlag{
		Name:  "obj.size",
		Value: "1KiB",
		Usage: "Size of each generated object. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
	},
	cli.IntFlag{
		Name:  "batch",
		Value: 100,
		Usage: "Number of DELETE operations per batch.",
	},
}

var deleteCmd = cli.Command{
	Name:   "delete",
	Usage:  "benchmark delete objects",
	Action: mainDelete,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, deleteFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

  The benchmark will end when either all objects have been deleted or the durations specified with -duration has been reached. 
USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#delete

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainDelete is the entry point for get command.
func mainDelete(ctx *cli.Context) error {
	checkDeleteSyntax(ctx)
	src := newGenSource(ctx, "obj.size")

	b := bench.Delete{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     putOpts(ctx),
		},
		CreateObjects: ctx.Int("objects"),
		BatchSize:     ctx.Int("batch"),
	}
	return runBench(ctx, &b)
}

func checkDeleteSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}
	checkAnalyze(ctx)
	checkBenchmark(ctx)
	if ctx.Int("batch") < 1 {
		console.Fatal("batch size much be 1 or bigger")
	}
	wantO := ctx.Int("batch") * ctx.Int("concurrent") * 4
	if ctx.Int("objects") < wantO {
		console.Fatalf("Too few objects: With current --batch  and --concurrent settings, at least %d objects should be used for a valid benchmark. Use --objects=%d", wantO, wantO)
	}
}
