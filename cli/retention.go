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
	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var retentionFlags = []cli.Flag{
	cli.IntFlag{
		Name:  "objects",
		Value: 25000,
		Usage: "Number of objects to upload.",
	},
	cli.IntFlag{
		Name:  "versions",
		Value: 5,
		Usage: "Number of versions to upload to each object",
	},
	cli.StringFlag{
		Name:  "obj.size",
		Value: "1KiB",
		Usage: "Size of each generated object. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
	},
}

var retentionCmd = cli.Command{
	Name:   "retention",
	Usage:  "benchmark PutObjectRetention",
	Action: mainRetention,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, retentionFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#retention

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainGet is the entry point for get command.
func mainRetention(ctx *cli.Context) error {
	checkRetentionSyntax(ctx)
	src := newGenSource(ctx, "obj.size")
	b := bench.Retention{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     putOpts(ctx),
			Locking:     true,
		},
		CreateObjects: ctx.Int("objects"),
		Versions:      ctx.Int("versions"),
	}
	return runBench(ctx, &b)
}

func checkRetentionSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}
	if ctx.Int("objects") <= 0 {
		console.Fatal("There must be more than 0 objects.")
	}
	if ctx.Int("versions") <= 0 {
		console.Fatal("There must be more than 0 versions per object.")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
