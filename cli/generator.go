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
	"errors"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/mc/pkg/probe"

	"github.com/minio/cli"
	"github.com/minio/warp/pkg/generator"
)

var genFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "obj.generator",
		Value: "random",
		Usage: "Use specific data generator",
	},
	cli.BoolFlag{
		Name:  "obj.randsize",
		Usage: "Randomize size of objects so they will be up to the specified size",
	},
}

// newGenSource returns a new generator
func newGenSource(ctx *cli.Context) func() generator.Source {
	prefixSize := 8
	if ctx.Bool("noprefix") {
		prefixSize = 0
	}

	var g generator.OptionApplier
	switch ctx.String("obj.generator") {
	case "random":
		g = generator.WithRandomData()
	case "csv":
		g = generator.WithCSV().Size(25, 1000)
	default:
		err := errors.New("unknown generator type:" + ctx.String("generator"))
		fatal(probe.NewError(err), "Invalid -generator parameter")
		return nil
	}
	size, err := toSize(ctx.String("obj.size"))
	fatalIf(probe.NewError(err), "Invalid obj.size specified")
	src, err := generator.NewFn(g.Apply(),
		generator.WithPrefixSize(prefixSize),
		generator.WithSize(int64(size)),
		generator.WithRandomSize(ctx.Bool("obj.randsize")),
	)
	fatalIf(probe.NewError(err), "Unable to create data generator")
	return src
}

// toSize converts a size indication to bytes.
func toSize(size string) (uint64, error) {
	return humanize.ParseBytes(size)
}
