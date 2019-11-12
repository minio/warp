package cli

import (
	"log"

	"github.com/minio/cli"
	"github.com/minio/minio-go/v6"
	"github.com/minio/warp/pkg/bench"
)

// cp command flags.
var (
	getFlags = []cli.Flag{
		cli.IntFlag{
			Name:  "objects",
			Value: 1000,
			Usage: "Number of objects to upload.",
		},
	}
)

var getCmd = cli.Command{
	Name:   "get",
	Usage:  "benchmark get objects",
	Action: mainGet,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, genFlags, getFlags, benchFlags, analyzeFlags),
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

// mainGet is the entry point for get command.
func mainGet(ctx *cli.Context) error {
	checkGetSyntax(ctx)
	src := newGenSource(ctx)
	cl, err := minio.New(ctx.String("host"), ctx.String("access-key"), ctx.String("secret-key"), false)
	if err != nil {
		log.Fatal(err)
	}
	b := bench.Get{
		Common: bench.Common{
			Client:      cl,
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts: minio.PutObjectOptions{
				ServerSideEncryption: newSSE(ctx),
			},
		},
		CreateObjects: ctx.Int("objects"),
		GetOpts:       minio.GetObjectOptions{ServerSideEncryption: newSSE(ctx)},
	}
	return runBench(ctx, &b)
}

func checkGetSyntax(ctx *cli.Context) {
	//if len(ctx.Args()) < 2 {
	//	cli.ShowCommandHelpAndExit(ctx, "put", 1) // last argument is exit code.
	//}
	// extract URLs.
	//URLs := ctx.Args()
	//if len(URLs) < 2 {
	//	fatalIf(errDummy().Trace(ctx.Args()...), fmt.Sprintf("Unable to parse source and target arguments."))
	//}
}
