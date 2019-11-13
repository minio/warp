package cli

import (
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio-go/v6"
	"github.com/minio/warp/pkg/bench"
)

// cp command flags.
var (
	putFlags = []cli.Flag{}
)

// Put command.
var putCmd = cli.Command{
	Name:   "put",
	Usage:  "benchmark put objects",
	Action: mainPut,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, putFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
...
 `,
}

// mainPut is the entry point for cp command.
func mainPut(ctx *cli.Context) error {
	checkPutSyntax(ctx)
	src := newGenSource(ctx)
	cl, err := minio.New(ctx.String("host"), ctx.String("access-key"), ctx.String("secret-key"), false)
	fatalIf(probe.NewError(err), "Unable to create MinIO client")
	b := bench.Put{
		Common: bench.Common{
			Client:      cl,
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     minio.PutObjectOptions{},
		},
	}
	return runBench(ctx, &b)
}

func checkPutSyntax(ctx *cli.Context) {
	checkAnalyze(ctx)

}
