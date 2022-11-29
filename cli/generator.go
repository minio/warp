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
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/minio/mc/pkg/probe"

	"github.com/minio/cli"
	"github.com/minio/warp/pkg/generator"
)

var genFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "obj.generator",
		Value: "random",
		Usage: "Use specific data generator. Options: text, random, csv",
	},
	cli.BoolFlag{
		Name:  "obj.randsize",
		Usage: "Randomize size of objects so they will be up to the specified size",
	},
	cli.StringFlag{
		Name: "obj.dist",
		Usage: "Specify a CSV string containing object size distributions such that all percentages add up to 100." +
			"\n\tFormat: size1:percent1,size2:percent2,etc." +
			"\n\tExample: 1KiB:10,4KiB:15,8KiB:15,16KiB:15,32KiB:15,64KiB:10,128KiB:5,256KiB:10,1MiB:5",
	},
	cli.StringFlag{
		Name: "obj.comp",
		Usage: "Integer value for the compression ratio desired on the generated data." +
			"\n\tFor instance, a value of 2 will generate data that is 50% compressible." +
			"\n\tExample: 2",
	},
}

func newGenSourceCSV(ctx *cli.Context) func() generator.Source {
	prefixSize := 8
	if ctx.Bool("noprefix") {
		prefixSize = 0
	}

	g := generator.WithCSV().Size(25, 1000)

	size, err := toSize(ctx.String("obj.size"))
	fatalIf(probe.NewError(err), "Invalid obj.size specified")

	validateGeneratorFlags(ctx)

	src, err := applyGenerators(g, ctx, prefixSize, size)

	fatalIf(probe.NewError(err), "Unable to create data generator")
	return src
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
	case "csv":
		g = generator.WithCSV().Size(25, 1000)
	case "text":
		g = generator.WithTextData()
	default:
		err := errors.New("unknown generator type:" + ctx.String("obj.generator"))
		fatal(probe.NewError(err), "Invalid -generator parameter")
		return nil
	}

	size, err := toSize(ctx.String(sizeField))
	fatalIf(probe.NewError(err), "Invalid obj.size specified")

	validateGeneratorFlags(ctx)

	src, err := applyGenerators(g, ctx, prefixSize, size)

	fatalIf(probe.NewError(err), "Unable to create data generator")
	return src
}

// toSize converts a size indication to bytes.
func toSize(size string) (uint64, error) {
	return humanize.ParseBytes(size)
}

// validates whether generator flags are compatible.
func validateGeneratorFlags(ctx *cli.Context) {
	if ctx.Bool("obj.randsize") && ctx.String("obj.dist") != "" {
		err := errors.New("specify either 'obj.randsize' or 'obj.dist' options, not both")
		fatalIf(probe.NewError(err), "Incompatible generator parameters.")
	}

	if ctx.String("obj.comp") != "" && ctx.String("obj.generator") != "text" {
		err := errors.New("compression is only applicable to generator type 'text'. Specify the option: '--obj.generator text'")
		fatalIf(probe.NewError(err), "Incompatible generator parameters.")
	}
}

// applies generators based on the randomization option provided.
func applyGenerators(g generator.OptionApplier, ctx *cli.Context, prefixSize int, size uint64) (func() generator.Source, error) {
	compRatio := 0
	var err error
	if ctx.String("obj.comp") != "" {
		compRatio, err = strconv.Atoi(ctx.String("obj.comp"))
		fatalIf(probe.NewError(err), "obj.comp should be an integer")
	}

	if ctx.String("obj.dist") != "" {
		sizesArr := parseDisrtibutionSizes(ctx)

		// make sure the min obj size from distribution is greater than compRatio.
		validateCompRatio(compRatio, getMinObjSize(sizesArr))

		src, err := generator.NewFn(g.Apply(),
			generator.WithCustomPrefix(ctx.String("prefix")),
			generator.WithPrefixSize(prefixSize),
			generator.WithSizeDistribution(sizesArr),
			generator.WithCompression(compRatio),
		)
		return src, err
	} else {
		if ctx.Bool("obj.randsize") {
			validateCompRatio(compRatio, generator.MIN_RAND_SIZE)
		} else{
			validateCompRatio(compRatio, int64(size))
		}

		src, err := generator.NewFn(g.Apply(),
			generator.WithCustomPrefix(ctx.String("prefix")),
			generator.WithPrefixSize(prefixSize),
			generator.WithSize(int64(size)),
			generator.WithRandomSize(ctx.Bool("obj.randsize")),
			generator.WithCompression(compRatio),
		)
		return src, err
	}
}

// validates the compression ratio provided.
func validateCompRatio(compRatio int, size int64) {
	if int64(compRatio) > size {
		err := errors.New("compression ratio (" + strconv.Itoa(compRatio) + ") cannot be greater than the minimum object size (" + strconv.FormatInt(size, 10) + ").")
		fatalIf(probe.NewError(err), "Invalid compression ratio provided.")
	}
}

// provides the smallest object size in an array of object sizes
func getMinObjSize(sizesArr []int64) int64 {
	smallest := sizesArr[0]
	for _, num := range sizesArr[1:] {
		if num < smallest {
			smallest = num
		}
	}
	return smallest
}

/*
generates an array of sizes based on the distribution percentages provided.

sample:

	input: 1KiB:10,4KiB:15,8KiB:15,16KiB:15,32KiB:15,64KiB:10,128KiB:5,256KiB:10,1MiB:5
	output: [
		1024, 1024, 1024, 1024, 1024, 1024, 1024, 1024, 1024, 1024,
		4096, 4096, 4096, 4096, 4096, 4096, 4096, 4096, 4096, 4096, 4096, 4096, 4096, 4096, 4096,
		8192, 8192, 8192, 8192, 8192, 8192, 8192, 8192, 8192, 8192, 8192, 8192, 8192, 8192, 8192,
		16384, 16384, 16384, 16384, 16384, 16384, 16384, 16384, 16384, 16384, 16384, 16384, 16384, 16384, 16384,
		32768, 32768, 32768, 32768, 32768, 32768, 32768, 32768, 32768, 32768, 32768, 32768, 32768, 32768, 32768,
		65536, 65536, 65536, 65536, 65536, 65536, 65536, 65536, 65536, 65536,
		131072, 131072, 131072, 131072, 131072, 262144, 262144, 262144, 262144, 262144, 262144, 262144, 262144, 262144, 262144,
		1048576, 1048576, 1048576, 1048576, 1048576
	]
*/
func parseDisrtibutionSizes(ctx *cli.Context) []int64 {
	sizesArr := []int64{}

	distArr := strings.Split(ctx.String("obj.dist"), ",")
	for i := 0; i < len(distArr); i++ {
		distElement := strings.Split(distArr[i], ":")

		if len(distElement) != 2 {
			err := errors.New("distribution should be of the format 'size:percent' (Ex: 4KiB:10). Received: " + distArr[i])
			fatalIf(probe.NewError(err), "Invalid size distribution.")
		}

		percentInt, err := strconv.Atoi(distElement[1])
		fatalIf(probe.NewError(err), "Failed to convert distribution percentage to an integer. Received: "+distElement[1])

		if int(percentInt) <= 0 || int(percentInt) >= 100 {
			err := errors.New("distribution percentage should be an integer value greater than 0 and less than 100. Received: " + distElement[1])
			fatalIf(probe.NewError(err), "Invalid distribution percentage.")
		}

		sizeInBytes, err := toSize(distElement[0])
		fatalIf(probe.NewError(err), "Failed to convert human readable size to bytes.")

		for j := 0; j < int(percentInt); j++ {
			sizesArr = append(sizesArr, int64(sizeInBytes))
		}
	}

	if len(sizesArr) != 100 {
		err := errors.New("distribution percentages should add up to 100. Received: " + strconv.Itoa(len(sizesArr)))
		fatalIf(probe.NewError(err), "Invalid distribution percentage.")
	}

	return sizesArr
}
