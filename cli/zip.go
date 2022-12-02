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
	"fmt"
	"time"

	"github.com/minio/cli"
	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var zipFlags = []cli.Flag{
	cli.IntFlag{
		Name:  "files",
		Value: 10000,
		Usage: "Number of files to upload in the zip file.",
	},
	cli.StringFlag{
		Name:  "obj.size",
		Value: "10KiB",
		Usage: "Size of each generated object. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
	},
	cli.StringFlag{
		Name:   "part.size",
		Value:  "",
		Usage:  "Multipart part size. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
		Hidden: true,
	},
}

var zipCmd = cli.Command{
	Name:   "zip",
	Usage:  "benchmark minio s3zip",
	Action: mainZip,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, zipFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#zip

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainGet is the entry point for get command.
func mainZip(ctx *cli.Context) error {
	checkZipSyntax(ctx)
	ctx.Set("noprefix", "true")
	src := newGenSource(ctx, "obj.size")
	b := bench.S3Zip{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     putOpts(ctx),
			Locking:     true,
		},
		CreateFiles: ctx.Int("files"),
		ZipObjName:  fmt.Sprintf("%d.zip", time.Now().UnixNano()),
	}
	return runBench(ctx, &b)
}

func checkZipSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}
	if ctx.Int("files") <= 0 {
		console.Fatal("There must be more than 0 objects.")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
