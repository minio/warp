package cli

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/warp/pkg/bench"
)

var analyzeFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "analyse.dur",
		Value: "1s",
		Usage: "Split analysis into durations of this length",
	},
	cli.StringFlag{
		Name:  "csv",
		Value: "",
		Usage: "Output aggregated data as CSV",
	},
}

var analyzeCmd = cli.Command{
	Name:   "analyze",
	Usage:  "analyze existing benchmark data",
	Action: mainAnalyze,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] benchmark-data-file

Use - as input to read from stdin.

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
...
 `,
}

// mainGet is the entry point for get command.
func mainAnalyze(ctx *cli.Context) error {
	checkAnalyze(ctx)
	args := ctx.Args()
	if len(args) == 0 {
		log.Fatal("No benchmark data file supplied")
	}
	var zstdDec, _ = zstd.NewReader(nil)
	defer zstdDec.Close()
	for _, arg := range args {
		var input io.Reader
		if arg == "-" {
			input = os.Stdin
		} else {
			f, err := os.Open(arg)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			input = f
		}
		err := zstdDec.Reset(input)
		if err != nil {
			log.Fatal(err)
		}
		b, err := ioutil.ReadAll(zstdDec)
		if err != nil {
			log.Fatal(err)
		}
		ops, err := bench.OperationsFromCSV(bytes.NewBuffer(b))
		if err != nil {
			log.Fatal(err)
		}
		printAnalysis(ctx, ops)
	}
	return nil
}

func printAnalysis(ctx *cli.Context, ops bench.Operations) {
	for typ, ops := range ops.ByOp() {
		segs := ops.Segment(bench.SegmentOptions{
			From:           time.Time{},
			PerSegDuration: analysisDur(ctx),
		})
		fmt.Println("")
		if len(segs) <= 1 {
			fmt.Println("Skipping", typ, "too few samples.")
			continue
		}
		fmt.Println("Operation type:", typ)
		//segs.Print(os.Stdout)
		segs.SortByThroughput()
		fmt.Println("Errors:", len(ops.Errors()))
		fmt.Println("Fastest:", segs.Median(1))
		fmt.Println("Average:", ops.Total())
		fmt.Println("50% Median:", segs.Median(0.5))
		fmt.Println("Slowest:", segs.Median(0.0))
	}
}

// analysisDur returns the analysis duration or 0 if un-parsable.
func analysisDur(ctx *cli.Context) time.Duration {
	d, err := time.ParseDuration(ctx.String("analyse.dur"))
	if err != nil {
		return 0
	}
	return d
}

func checkAnalyze(ctx *cli.Context) {
	if analysisDur(ctx) == 0 {
		log.Fatal("invalid -analyse.dur value.")
	}
}
