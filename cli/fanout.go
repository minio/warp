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
	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var fanoutFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "obj.size",
		Value: "1MiB",
		Usage: "Size of each generated object. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
	},
	cli.IntFlag{
		Name:   "copies",
		Value:  100,
		Usage:  "Number of copies per uploaded object",
		Hidden: true,
	},
}

// Fanout command.
var fanoutCmd = cli.Command{
	Name:   "fanout",
	Usage:  "benchmark fan-out of objects on MinIO servers",
	Action: mainFanout,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, fanoutFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#fanout

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainFanout is the entry point for cp command.
func mainFanout(ctx *cli.Context) error {
	checkFanoutSyntax(ctx)
	b := bench.Fanout{
		Copies: ctx.Int("copies"),
		Common: getCommon(ctx, newGenSource(ctx, "obj.size")),
	}
	return runBench(ctx, &b)
}

func checkFanoutSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}
	if ctx.Int("copies") <= 0 {
		console.Fatal("Copies must be bigger than 0")
	}
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
