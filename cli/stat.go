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
	"github.com/minio/minio-go/v6"
	"github.com/minio/warp/pkg/bench"
)

var (
	statFlags = []cli.Flag{
		cli.IntFlag{
			Name:  "objects",
			Value: 10000,
			Usage: "Number of objects to upload. Rounded to have equal concurrent objects.",
		},
		cli.StringFlag{
			Name:  "obj.size",
			Value: "1KB",
			Usage: "Size of each generated object. Can be a number or 10KB/MB/GB. All sizes are base 2 binary.",
		},
	}
)

var statCmd = cli.Command{
	Name:   "stat",
	Usage:  "benchmark stat objects",
	Action: mainStat,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, statFlags, genFlags, benchFlags, analyzeFlags),
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

// mainDelete is the entry point for get command.
func mainStat(ctx *cli.Context) error {
	checkStatSyntax(ctx)
	src := newGenSource(ctx)

	b := bench.Stat{
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
	}
	return runBench(ctx, &b)
}

func checkStatSyntax(ctx *cli.Context) {
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
