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
	"errors"
	"fmt"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/minio/mc/pkg/probe"

	"github.com/minio/cli"
	"github.com/minio/warp/pkg/generator"

	hist "github.com/jfsmig/prng/histogram"
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
func newGenSource(ctx *cli.Context, sizeField string) func() generator.Source {
	prefixSize := 8
	if ctx.Bool("noprefix") {
		prefixSize = 0
	}

	var g generator.OptionApplier
	switch ctx.String("obj.generator") {
	case "random":
		g = generator.WithRandomData()
	default:
		err := errors.New("unknown generator type:" + ctx.String("obj.generator"))
		fatal(probe.NewError(err), "Invalid -generator parameter")
		return nil
	}
	opts := []generator.Option{
		generator.WithCustomPrefix(ctx.String("prefix")),
		generator.WithPrefixSize(prefixSize),
	}
	if strings.IndexRune(ctx.String(sizeField), ':') > 0 {
		if _, err := hist.ParseCSV(ctx.String(sizeField)); err != nil {
			fatalIf(probe.NewError(err), "Invalid histogram format for the size parameter")
		} else {
			opts = append(opts, generator.WithSizeHistograms(ctx.String(sizeField)))
		}
	} else {
		tokens := strings.Split(ctx.String(sizeField), ",")
		switch len(tokens) {
		case 1:
			size, err := toSize(tokens[0])
			if err != nil {
				fatalIf(probe.NewError(err), "Invalid obj.size specified")
			}
			opts = append(opts, generator.WithSize(int64(size)))
		case 2:
			minSize, err := toSize(tokens[0])
			if err != nil {
				fatalIf(probe.NewError(err), "Invalid min obj.size specified")
			}
			maxSize, err := toSize(tokens[1])
			if err != nil {
				fatalIf(probe.NewError(err), "Invalid max obj.size specified")
			}
			opts = append(opts, generator.WithMinMaxSize(int64(minSize), int64(maxSize)))
		default:
			fatalIf(probe.NewError(fmt.Errorf("unexpected obj.size specified: %s", ctx.String(sizeField))), "Invalid obj.size parameter")
		}

		opts = append([]generator.Option{g.Apply()}, append(opts, generator.WithRandomSize(ctx.Bool("obj.randsize")))...)
	}

	src, err := generator.NewFn(opts...)
	fatalIf(probe.NewError(err), "Unable to create data generator")
	return src
}

// toSize converts a size indication to bytes.
func toSize(size string) (uint64, error) {
	return humanize.ParseBytes(size)
}
