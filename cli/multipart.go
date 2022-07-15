/*
 * Warp (C) 2019-2022 MinIO, Inc.
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
	"context"

	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var multipartFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "part.size",
		Value: "5MiB",
		Usage: "Size of each part. Can be a number or MiB/GiB. Must be >= 5MiB",
	},
	cli.IntFlag{
		Name:  "parts",
		Value: 200,
		Usage: "Parts to add per client",
	},
	cli.StringFlag{
		Name:  "obj.name",
		Value: "warp-multipart.bin",
		Usage: "Object name.",
	},
	cli.StringFlag{
		Name:   "_upload-id",
		Value:  "",
		Usage:  "(internal)",
		Hidden: true,
	},
	cli.IntFlag{
		Name:   "_part-start",
		Value:  1,
		Usage:  "(internal)",
		Hidden: true,
	},
}

// Put command.
var multipartCmd = cli.Command{
	Name:   "multipart",
	Usage:  "benchmark multipart object",
	Action: mainMultipart,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, multipartFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#put

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainPut is the entry point for cp command.
func mainMultipart(ctx *cli.Context) error {
	checkMultipartSyntax(ctx)
	src := newGenSource(ctx, "part.size")
	b := bench.Multipart{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     multipartOpts(ctx),
		},
		ObjName:     ctx.String("obj.name"),
		PartStart:   ctx.Int("_part-start"),
		UploadID:    ctx.String("_upload-id"),
		CreateParts: ctx.Int("parts"),
	}
	if b.UploadID == "" {
		err := b.InitOnce(context.Background())
		if err != nil {
			console.Fatal(err)
		}
		b.ExtraFlags = map[string]string{"_upload-id": b.UploadID, "noprefix": "true"}
	}
	return runBench(ctx, &b)
}

// putOpts retrieves put options from the context.
func multipartOpts(ctx *cli.Context) minio.PutObjectOptions {
	return minio.PutObjectOptions{
		ServerSideEncryption: newSSE(ctx),
		DisableMultipart:     false,
		SendContentMd5:       ctx.Bool("md5"),
		StorageClass:         ctx.String("storage-class"),
	}
}

func checkMultipartSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}
	if ctx.Bool("disable-multipart") {
		console.Fatal("Cannot disable multipart for multipart test")
	}
	if ctx.Int("parts") <= 0 {
		console.Fatal("part.count must be > 1")
	}
	if sz, err := toSize(ctx.String("part.size")); sz < 5<<20 {
		if err != nil {
			console.Fatal("error parsing part.size:", err)
		}
		if sz < 5<<20 {
			console.Fatal("part.size must be >= 5MiB")
		}
	}
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
