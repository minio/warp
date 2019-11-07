package cli

import (
	"fmt"
	"log"
	"os"

	"github.com/minio/cli"
	"github.com/minio/warp/pkg/generator"
)

var genFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "generator",
		Value: "random",
		Usage: "Use specific data generator",
	},
	cli.Int64Flag{
		Name:  "obj-size",
		Value: 10 << 20,
		Usage: "Object Size",
	},
}

// newGenSource returns a new generator
func newGenSource(ctx *cli.Context) func() generator.Source {
	prefixSize := 8
	if ctx.Bool("no-prefix") {
		prefixSize = 0
		fmt.Println("no prefix")
		os.Exit(0)
	}

	var g generator.OptionApplier
	switch ctx.String("generator") {
	case "random":
		g = generator.WithRandomData()
	case "csv":
		g = generator.WithCSV().Size(10, 1000)
	default:
		log.Fatal("unknown generator type:", ctx.String("generator"))
	}
	src, err := generator.NewFn(g.Apply(),
		generator.WithPrefixSize(prefixSize),
		generator.WithSize(ctx.Int64("obj-size")),
	)
	if err != nil {
		log.Fatal(err)
	}
	return src
}
