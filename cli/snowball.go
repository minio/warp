/*
 * Warp (C) 2019-2023 MinIO, Inc.
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

var snowballFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "obj.size",
		Value: "512KiB",
		Usage: "Size of each generated object inside the snowball. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
	},
	cli.IntFlag{
		Name:  "objs.per",
		Value: 50,
		Usage: "Number of objects per snowball upload.",
	},
	cli.BoolFlag{
		Name:  "compress",
		Usage: "Compress each snowball file. Available for MinIO servers only.",
	},
}

// Put command.
var snowballCmd = cli.Command{
	Name:   "snowball",
	Usage:  "benchmark put objects in snowball tar files",
	Action: mainSnowball,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, snowballFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#snowball

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainPut is the entry point for cp command.
func mainSnowball(ctx *cli.Context) error {
	checkSnowballSyntax(ctx)
	src := newGenSource(ctx, "obj.size")
	b := bench.Snowball{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     snowballOpts(ctx),
		},
		Compress:  ctx.Bool("compress"),
		Duplicate: ctx.Bool("compress"),
		NumObjs:   ctx.Int("objs.per"),
	}
	if b.Compress {
		sz, err := toSize(ctx.String("obj.size"))
		if err != nil {
			return err
		}
		b.WindowSize = int(sz) * 2
		if b.WindowSize < 128<<10 {
			b.WindowSize = 128 << 10
		}
		if b.WindowSize > 16<<20 {
			b.WindowSize = 16 << 20
		}
	}
	return runBench(ctx, &b)
}

// putOpts retrieves put options from the context.
func snowballOpts(ctx *cli.Context) minio.PutObjectOptions {
	return minio.PutObjectOptions{
		ServerSideEncryption: newSSE(ctx),
		DisableMultipart:     ctx.Bool("disable-multipart"),
		SendContentMd5:       ctx.Bool("md5"),
		StorageClass:         ctx.String("storage-class"),
	}
}

func checkSnowballSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}

	// 1GB max total.
	const maxSize = 1 << 30
	sz, err := toSize(ctx.String("obj.size"))
	if err != nil {
		console.Fatalf("Unable to parse --obj.size: %v", err)
	}
	gotTotal := int64(sz) * int64(ctx.Int("concurrent"))
	compress := ctx.Bool("compress")
	if compress && sz > 10<<20 {
		console.Fatal("--obj.size must be <= 10MiB when compression is enabled")
	}
	if !compress {
		gotTotal *= int64(ctx.Int("objs.per"))
	}
	if gotTotal > maxSize {
		console.Fatalf("total size (%d) exceeds 1GiB", gotTotal)
	}
	if gotTotal <= 0 {
		console.Fatalf("Parameters results in no expected output")
	}
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
