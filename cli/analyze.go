package cli

import (
	"github.com/minio/cli"
)

var analyzeFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "analyse.dur",
		Value: "",
		Usage: "Split analysis into durations of this length",
	},
	cli.StringFlag{
		Name:  "csv",
		Value: "",
		Usage: "Output aggregated data as CSV",
	},
}

func checkAnalyze(ctx *cli.Context) {

}
