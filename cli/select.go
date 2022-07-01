/*
 * Warp (C) 2020 MinIO, Inc.
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
	"github.com/minio/warp/pkg/bench"
)

var selectFlags = []cli.Flag{
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
		Name:  "query",
		Value: "select * from s3object",
		Usage: "select query expression",
	},
}

var selectCmd = cli.Command{
	Name:   "select",
	Usage:  "benchmark select objects",
	Action: mainSelect,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, selectFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainSelect is the entry point for select command.
func mainSelect(ctx *cli.Context) error {
	checkSelectSyntax(ctx)
	src := newGenSourceCSV(ctx)
	sse := newSSE(ctx)
	b := bench.Select{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     putOpts(ctx),
		},
		CreateObjects: ctx.Int("objects"),
		SelectOpts: minio.SelectObjectOptions{
			Expression:     ctx.String("query"),
			ExpressionType: minio.QueryExpressionTypeSQL,
			// Set any encryption headers
			ServerSideEncryption: sse,
			// TODO: support all variations including, json/parquet
			InputSerialization: minio.SelectObjectInputSerialization{
				CSV: &minio.CSVInputOptions{
					RecordDelimiter: "\n",
					FieldDelimiter:  ",",
					FileHeaderInfo:  minio.CSVFileHeaderInfoUse,
				},
			},
			OutputSerialization: minio.SelectObjectOutputSerialization{
				CSV: &minio.CSVOutputOptions{
					RecordDelimiter: "\n",
					FieldDelimiter:  ",",
				},
			},
		},
	}
	return runBench(ctx, &b)
}

func checkSelectSyntax(ctx *cli.Context) {
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
