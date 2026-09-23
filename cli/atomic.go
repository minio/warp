/*
 * Warp (C) 2026 MinIO, Inc.
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
	"math"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/pkg/generator"
)

var atomicFlags = []cli.Flag{
	cli.IntFlag{
		Name:  "objects",
		Value: 16,
		Usage: "Number of keys overwritten concurrently. Fewer keys means more contention per key.",
	},
	cli.StringFlag{
		Name:  "obj.size",
		Value: "4MiB",
		Usage: "Size of each generated object. Combine with --obj.randsize so an overwrite changes the length. All sizes are base 2 binary.",
	},
	cli.IntFlag{
		Name:  "block.size",
		Value: 4096,
		Usage: "Every block of this many bytes carries the identity of the PUT it belongs to.",
	},
	cli.Float64Flag{
		Name:  "put-distrib",
		Usage: "The amount of PUT operations.",
		Value: 40,
	},
	cli.Float64Flag{
		Name:  "get-distrib",
		Usage: "The amount of GET operations.",
		Value: 45,
	},
	cli.Float64Flag{
		Name:  "stat-distrib",
		Usage: "The amount of STAT operations.",
		Value: 15,
	},
	cli.Float64Flag{
		Name:  "list-distrib",
		Usage: "The amount of LIST cycles. Each PUTs a new key, lists it, deletes it, lists again, and lists the overwritten keys.",
		Value: 10,
	},
	cli.BoolTFlag{
		Name:  "read-after-write",
		Usage: "Follow every acknowledged PUT with a GET of the same key, from another host when --host lists several.",
	},
	cli.Uint64Flag{
		Name:  "seed",
		Usage: "Seed for each thread's choice of key, operation and object size. 0 picks one at random. Violations report the seed.",
	},
	cli.BoolFlag{
		Name:  "no-etag-md5",
		Usage: "Do not require the ETag to be the MD5 of the body, for servers that use opaque ETags.",
	},
}

var AtomicCombinedFlags = combineFlags(globalFlags, ioFlags, atomicFlags, genFlags, benchFlags, analyzeFlags)

var atomicCmd = cli.Command{
	Name:   "atomic",
	Usage:  "verify that concurrent overwrites are atomic and reads are not stale",
	Action: mainAtomic,
	Before: setGlobalsFromContext,
	Flags:  AtomicCombinedFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#atomic

Every PUT writes a body in which each block names the PUT it belongs to,
and records that name in object metadata. Every GET and STAT checks that
body, length, metadata and ETag all belong to one PUT, and that the PUT
had not already been overwritten when the read started. With
--list-distrib, LIST cycles check list-after-write and list-after-delete
on new keys, and that listings of the overwritten keys are not stale.
Violations are reported as errors prefixed with "atomic <category>:".

Staleness is judged only against PUTs issued by the same warp process;
body, metadata and ETag checks apply to every object read.

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

func mainAtomic(ctx *cli.Context) error {
	checkAtomicSyntax(ctx)
	sse := newSSE(ctx)
	sizeFn, err := generator.SizeFn(genOptions(ctx, "obj.size")...)
	fatalIf(probe.NewError(err), "Invalid object size")
	b := bench.Atomic{
		Common:         getCommon(ctx, newGenSource(ctx, "obj.size")),
		Keys:           ctx.Int("objects"),
		BlockSize:      ctx.Int("block.size"),
		CheckMD5:       !ctx.Bool("no-etag-md5"),
		ReadAfterWrite: ctx.BoolT("read-after-write"),
		Prefix:         ctx.String("prefix"),
		PutWeight:      ctx.Float64("put-distrib"),
		GetWeight:      ctx.Float64("get-distrib"),
		StatWeight:     ctx.Float64("stat-distrib"),
		ListWeight:     ctx.Float64("list-distrib"),
		Seed:           ctx.Uint64("seed"),
		SizeFn:         sizeFn,
		GetOpts:        minio.GetObjectOptions{ServerSideEncryption: sse},
		StatOpts:       minio.StatObjectOptions{ServerSideEncryption: sse},
	}
	return runBench(ctx, &b)
}

func checkAtomicSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}
	if ctx.Int("objects") < 1 {
		console.Fatal("At least one object must be tested")
	}
	if ctx.Int("block.size") < bench.AtomicMinBlockSize {
		console.Fatalf("--block.size must be at least %d\n", bench.AtomicMinBlockSize)
	}
	total := 0.0
	for _, f := range []string{"put-distrib", "get-distrib", "stat-distrib", "list-distrib"} {
		w := ctx.Float64(f)
		if math.IsNaN(w) || math.IsInf(w, 0) || w < 0 {
			console.Fatalf("--%s must be a finite number of at least 0\n", f)
		}
		total += w
	}
	if math.IsInf(total, 0) {
		console.Fatal("the --*-distrib values add up to more than can be represented")
	}
	if ctx.Float64("list-distrib") == 0 && (ctx.Float64("put-distrib") == 0 || ctx.Float64("get-distrib")+ctx.Float64("stat-distrib") == 0) {
		console.Fatal("set --list-distrib, or both --put-distrib and one of --get-distrib or --stat-distrib")
	}
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
