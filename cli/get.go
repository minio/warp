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
	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var (
	getFlags = []cli.Flag{
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
	}
)

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

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
...
 `,
}

// mainGet is the entry point for get command.
func mainGet(ctx *cli.Context) error {
	checkGetSyntax(ctx)
	src := newGenSource(ctx)
	sse := newSSE(ctx)
	b := bench.Get{
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
		GetOpts:       minio.GetObjectOptions{ServerSideEncryption: sse},
	}
	return runBench(ctx, &b)
}

func checkGetSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
