/*
 * Warp (C) 2020 MinIO, Inc.
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
	"github.com/minio/cli"
	"github.com/minio/minio-go/v6"
	"github.com/minio/warp/pkg/bench"
)

var (
	selectFlags = []cli.Flag{
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
)

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

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
...
 `,
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
			PutOpts: minio.PutObjectOptions{
				ServerSideEncryption: sse,
			},
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
