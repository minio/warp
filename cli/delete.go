/*
 * Warp (C) 2019-2020 MinIO, Inc.
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
	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio-go/v6"
	"github.com/minio/warp/pkg/bench"
)

var (
	deleteFlags = []cli.Flag{
		cli.IntFlag{
			Name:  "objects",
			Value: 25000,
			Usage: "Number of objects to upload.",
		},
		cli.StringFlag{
			Name:  "obj.size",
			Value: "1KB",
			Usage: "Size of each generated object. Can be a number or 10KB/MB/GB. All sizes are base 2 binary.",
		},
		cli.IntFlag{
			Name:  "batch",
			Value: 100,
			Usage: "Number of DELETE operations per batch.",
		},
	}
)

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

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
...
 `,
}

// mainDelete is the entry point for get command.
func mainDelete(ctx *cli.Context) error {
	checkDeleteSyntax(ctx)
	src := newGenSource(ctx)

	b := bench.Delete{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts: minio.PutObjectOptions{
				ServerSideEncryption: newSSE(ctx),
			},
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
}
