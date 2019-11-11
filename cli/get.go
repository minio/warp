package cli

import (
	"log"

	"github.com/minio/cli"
	"github.com/minio/minio-go/v6"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/pkg/sse"
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

// Put command.
var getCmd = cli.Command{
	Name:   "get",
	Usage:  "get objects",
	Action: mainGet,
	Before: setGlobalsFromContext,
	Flags:  append(append(append(getFlags, ioFlags...), globalFlags...), genFlags...),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
ENVIRONMENT VARIABLES:
  ` + appNameUC + `_ENCRYPT:      list of comma delimited prefixes
  ` + appNameUC + `_ENCRYPT_KEY:  list of comma delimited prefix=secret values

EXAMPLES:
...
 `,
}

// mainPut is the entry point for cp command.
func mainGet(ctx *cli.Context) error {
	// Parse encryption keys per command.
	encKeyDB, perr := sse.EncKeys(ctx)
	fatalIf(perr, "Unable to parse encryption keys.")

	checkPutSyntax(ctx, encKeyDB)
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
			PutOpts:     minio.PutObjectOptions{
				//ServerSideEncryption: sse.GetSSE(encKeyDB),
			},
		},
		CreateObjects: ctx.Int("objects"),
	}
	return runBench(ctx, &b)
}

func checkGetSyntax(ctx *cli.Context, encKeyDB map[string][]sse.PrefixSSEPair) {
	//if len(ctx.Args()) < 2 {
	//	cli.ShowCommandHelpAndExit(ctx, "put", 1) // last argument is exit code.
	//}
	// extract URLs.
	//URLs := ctx.Args()
	//if len(URLs) < 2 {
	//	fatalIf(errDummy().Trace(ctx.Args()...), fmt.Sprintf("Unable to parse source and target arguments."))
	//}
}
