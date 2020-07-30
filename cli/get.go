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
	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var (
	getFlags = []cli.Flag{
		cli.StringFlag{
			Name:  "object-size",
			Value: "10MiB",
			Usage: "size of each generated object, can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
		},
	}
)

var getCmd = cli.Command{
	Name:   "get",
	Usage:  "benchmark get objects",
	Action: mainGet,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, getFlags, genFlags, benchFlags),
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
			PutOpts:     putOpts(ctx),
		},
		GetOpts: minio.GetObjectOptions{ServerSideEncryption: sse},
	}
	return runBench(ctx, &b)
}

func checkGetSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}
	checkBenchmark(ctx)
}
