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
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var getFlags = []cli.Flag{
	cli.BoolFlag{
		Name:  "list-existing",
		Usage: "Instead of preparing the bench by PUTing some objects, only use objects already in the bucket",
	},
	cli.BoolFlag{
		Name:  "list-flat",
		Usage: "When using --list-existing, do not use recursive listing",
	},
	cli.IntFlag{
		Name:  "objects",
		Value: 2500,
		Usage: "Number of objects to upload.",
	},
	cli.StringFlag{
		Name:  "obj.size",
		Value: "10MiB",
		Usage: "Size of each generated object. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
	},
	cli.BoolFlag{
		Name:  "range",
		Usage: "Do ranged get operations. Will request with random offset and length.",
	},
	cli.IntFlag{
		Name:  "versions",
		Value: 1,
		Usage: "Number of versions to upload. If more than 1, versioned listing will be benchmarked",
	},
}

var getCmd = cli.Command{
	Name:   "get",
	Usage:  "benchmark get objects",
	Action: mainGet,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, getFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#get

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainGet is the entry point for get command.
func mainGet(ctx *cli.Context) error {
	checkGetSyntax(ctx)
	src := newGenSource(ctx, "obj.size")
	sse := newSSE(ctx)
	b := bench.Get{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     putOpts(ctx),
		},
		Versions:      ctx.Int("versions"),
		RandomRanges:  ctx.Bool("range"),
		CreateObjects: ctx.Int("objects"),
		GetOpts:       minio.GetObjectOptions{ServerSideEncryption: sse},
		ListExisting:  ctx.Bool("list-existing"),
		ListFlat:      ctx.Bool("list-flat"),
		ListPrefix:    ctx.String("prefix"),
	}
	return runBench(ctx, &b)
}

func checkGetSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}
	if ctx.Int("versions") < 1 {
		console.Fatal("At least one version must be tested")
	}
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
