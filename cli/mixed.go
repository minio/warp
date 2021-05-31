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
	"net/http"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var (
	mixedFlags = []cli.Flag{
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
		cli.Float64Flag{
			Name:  "get-distrib",
			Usage: "The amount of GET operations.",
			Value: 45,
		},
		cli.Float64Flag{
			Name:  "stat-distrib",
			Usage: "The amount of STAT operations.",
			Value: 30,
		},
		cli.Float64Flag{
			Name:  "put-distrib",
			Usage: "The amount of PUT operations.",
			Value: 15,
		},
		cli.Float64Flag{
			Name:  "delete-distrib",
			Usage: "The amount of DELETE operations. Must be at least the same as PUT.",
			Value: 10,
		},
	}
)

var mixedCmd = cli.Command{
	Name:   "mixed",
	Usage:  "benchmark mixed objects",
	Action: mainMixed,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, mixedFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#mixed

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainMixed is the entry point for mixed command.
func mainMixed(ctx *cli.Context) error {
	checkMixedSyntax(ctx)
	src := newGenSource(ctx)
	sse := newSSE(ctx)
	dist := bench.MixedDistribution{
		Distribution: map[string]float64{
			http.MethodGet:    ctx.Float64("get-distrib"),
			"STAT":            ctx.Float64("stat-distrib"),
			http.MethodPut:    ctx.Float64("put-distrib"),
			http.MethodDelete: ctx.Float64("delete-distrib"),
		},
	}
	err := dist.Generate(ctx.Int("objects") * 2)
	fatalIf(probe.NewError(err), "Invalid distribution")
	b := bench.Mixed{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     putOpts(ctx),
		},
		CreateObjects: ctx.Int("objects"),
		GetOpts:       minio.GetObjectOptions{ServerSideEncryption: sse},
		StatOpts: minio.StatObjectOptions{
			ServerSideEncryption: sse,
		},
		Dist: &dist,
	}
	return runBench(ctx, &b)
}

func checkMixedSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
