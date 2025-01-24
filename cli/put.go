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
	"fmt"
	"math/rand"
	"strings"

	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
)

var putFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "obj.size",
		Value: "10MiB",
		Usage: "Size of each generated object. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
	},
	cli.StringFlag{
		Name:   "part.size",
		Value:  "",
		Usage:  "Multipart part size. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:  "post",
		Usage: "Use PostObject for upload. Will force single part upload",
	},
}

var PutCombinedFlags = combineFlags(globalFlags, ioFlags, putFlags, genFlags, benchFlags, analyzeFlags)

// Put command.
var putCmd = cli.Command{
	Name:   "put",
	Usage:  "benchmark put objects",
	Action: mainPut,
	Before: setGlobalsFromContext,
	Flags:  PutCombinedFlags,
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
func mainPut(ctx *cli.Context) error {
	checkPutSyntax(ctx)
	pSize, _ := toSize(ctx.String("part.size"))
	oSize, _ := toSize(ctx.String("obj.size"))
	b := bench.Put{
		Common:      getCommon(ctx, newGenSource(ctx, "part.size")),
		PostObject:  ctx.Bool("post"),
		CreateParts: int(oSize / pSize),
	}
	return runBench(ctx, &b)
}

const metadataChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-_."

// putOpts retrieves put options from the context.
func putOpts(ctx *cli.Context) minio.PutObjectOptions {
	pSize, _ := toSize(ctx.String("part.size"))
	options := minio.PutObjectOptions{
		ServerSideEncryption: newSSE(ctx),
		DisableMultipart:     ctx.Bool("disable-multipart"),
		DisableContentSha256: ctx.Bool("disable-sha256-payload"),
		SendContentMd5:       ctx.Bool("md5"),
		StorageClass:         ctx.String("storage-class"),
		PartSize:             pSize,
	}

	for _, flag := range []string{"add-metadata", "tag"} {
		values := make(map[string]string)

		for _, v := range ctx.StringSlice(flag) {
			idx := strings.Index(v, "=")
			if idx <= 0 {
				console.Fatalf("--%s takes `key=value` argument", flag)
			}
			key := v[:idx]
			value := v[idx+1:]
			if len(value) == 0 {
				console.Fatal("--%s value can't be empty", flag)
			}
			var randN int
			if _, err := fmt.Sscanf(value, "rand:%d", &randN); err == nil {
				rng := rand.New(rand.NewSource(int64(rand.Uint64())))
				value = ""
				for i := 0; i < randN; i++ {
					value += string(metadataChars[rng.Int()%len(metadataChars)])
				}
			}
			values[key] = value
		}

		switch flag {
		case "metadata":
			options.UserMetadata = values
		case "tag":
			options.UserTags = values
		}
	}

	return options
}

func checkPutSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
