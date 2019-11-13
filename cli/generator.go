package cli

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"

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
	cli.StringFlag{
		Name:  "obj.size",
		Value: "10MB",
		Usage: "Size of each generated object. Can be a number or 10KB/MB/GB. All sizes are base 2 binary.",
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
	)
	fatalIf(probe.NewError(err), "Unable to create data generator")
	return src
}

// toSize converts a size indication to bytes.
func toSize(size string) (uint64, error) {
	size = strings.ToUpper(strings.TrimSpace(size))
	firstLetter := strings.IndexFunc(size, unicode.IsLetter)
	if firstLetter == -1 {
		firstLetter = len(size)
	}

	bytesString, multiple := size[:firstLetter], size[firstLetter:]
	bytes, err := strconv.ParseUint(bytesString, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unable to parse size: %v", err)
	}

	switch multiple {
	case "M", "MB", "MIB":
		return bytes * 1 << 20, nil
	case "K", "KB", "KIB":
		return bytes * 1 << 10, nil
	case "B", "":
		return bytes, nil
	default:
		return 0, fmt.Errorf("unknown size suffix: %v", multiple)
	}
}
