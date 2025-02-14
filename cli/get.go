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
	"github.com/minio/pkg/v2/console"
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
	cli.StringFlag{
		Name:   "part.size",
		Value:  "",
		Usage:  "Multipart part size. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:  "range",
		Usage: "Do ranged get operations. Will request with random offset and length.",
	},
	cli.StringFlag{
		Name:  "range-size",
		Usage: "Use a fixed range size while doing random range offsets, --range is implied",
	},
	cli.IntFlag{
		Name:  "versions",
		Value: 1,
		Usage: "Number of versions to upload. If more than 1, versioned listing will be benchmarked",
	},
	cli.BoolFlag{
		Name:  "extra-head",
		Usage: "Many apps HEAD before GET. Add an extra HEAD operation to emulate this behavior.",
	},
	cli.BoolFlag{
		Name:  "md5",
		Usage: "Verify MD5 hash of downloaded objects against ETag",
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

	var rangeSize int64
	if rs := ctx.String("range-size"); rs != "" {
		s, err := toSize(rs)
		if err != nil {
			return err
		}
		rangeSize = int64(s)
	}

	sse := newSSE(ctx)
	b := bench.Get{
		Common:        getCommon(ctx, newGenSource(ctx, "obj.size")),
		Versions:      ctx.Int("versions"),
		RandomRanges:  ctx.Bool("range") || ctx.IsSet("range-size"),
		RangeSize:     rangeSize,
		CreateObjects: ctx.Int("objects"),
		GetOpts:       minio.GetObjectOptions{ServerSideEncryption: sse},
		ListExisting:  ctx.Bool("list-existing"),
		ListFlat:      ctx.Bool("list-flat"),
		ListPrefix:    ctx.String("prefix"),
		ExtraHead:     ctx.Bool("extra-head"),
		VerifyMD5:     ctx.Bool("md5"),
	}
	if b.ListExisting && !ctx.IsSet("objects") {
		b.CreateObjects = 0
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
	if ctx.Int("objects") < 1 {
		console.Fatal("At least one object must be tested")
	}
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
