/*
 * Warp (C) 2019-2025 MinIO, Inc.
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

	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
)

var appendFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "obj.size",
		Value: "10MiB",
		Usage: "Size of each append operation. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
	},
}

var AppendCombinedFlags = combineFlags(globalFlags, ioFlags, appendFlags, genFlags, benchFlags, analyzeFlags)

// Put command.
var appendCmd = cli.Command{
	Name:   "append",
	Usage:  "benchmark appen objects (s3 express)",
	Action: mainAppend,
	Before: setGlobalsFromContext,
	Flags:  AppendCombinedFlags,
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
func mainAppend(ctx *cli.Context) error {
	checkAppendSyntax(ctx)
	useTrailingHeaders.Store(true)
	b := bench.Append{
		Common: getCommon(ctx, newGenSource(ctx, "obj.size")),
	}
	if b.Common.Versioned {
		return fmt.Errorf("append versioned objects is not supported")
	}
	if !b.PutOpts.Checksum.IsSet() {
		// Set checksum to CRC64NVME if not set
		b.Common.PutOpts.Checksum = minio.ChecksumCRC64NVME
	}
	return runBench(ctx, &b)
}

func checkAppendSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
