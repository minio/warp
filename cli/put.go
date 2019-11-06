package cli

import (
	"context"
	"log"
	"time"

	"github.com/minio/cli"
	"github.com/minio/minio-go/v6"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/pkg/generator"
	"github.com/minio/warp/pkg/sse"
)

// cp command flags.
var (
	putFlags = []cli.Flag{}
)

// Copy command.
var putCmd = cli.Command{
	Name:   "put",
	Usage:  "put objects",
	Action: mainPut,
	Before: setGlobalsFromContext,
	Flags:  append(append(putFlags, ioFlags...), globalFlags...),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] HOSTS

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
ENVIRONMENT VARIABLES:
  MC_ENCRYPT:      list of comma delimited prefixes
  MC_ENCRYPT_KEY:  list of comma delimited prefix=secret values

EXAMPLES:
...
 `,
}

// mainPut is the entry point for cp command.
func mainPut(ctx *cli.Context) error {
	// Parse encryption keys per command.
	encKeyDB, perr := sse.EncKeys(ctx)
	fatalIf(perr, "Unable to parse encryption keys.")

	checkPutSyntax(ctx, encKeyDB)
	prefixSize := 8
	if ctx.Bool("no-prefix") {
		prefixSize = 0
	}
	src, err := generator.NewFn(generator.WithCSV().Size(10, 1000).Apply(), generator.WithPrefixSize(prefixSize))
	if err != nil {
		log.Fatal(err)
	}
	cl, err := minio.New("127.0.0.1:9000", "minio", "minio123", false)
	if err != nil {
		log.Fatal(err)
	}
	b := bench.Upload{
		Common: bench.Common{
			Client:      cl,
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     minio.PutObjectOptions{},
		},
	}
	b.Prepare(context.Background())

	tStart := time.Now().Add(time.Second)
	ctx2, cancel := context.WithDeadline(context.Background(), tStart.Add(time.Minute))
	defer cancel()
	start := make(chan struct{})
	go func() {
		<-time.After(time.Until(tStart))
		close(start)
	}()
	b.Start(ctx2, start)
	b.Cleanup(context.Background())

	return nil
}

func checkPutSyntax(ctx *cli.Context, encKeyDB map[string][]sse.PrefixSSEPair) {
	//if len(ctx.Args()) < 2 {
	//	cli.ShowCommandHelpAndExit(ctx, "put", 1) // last argument is exit code.
	//}
	// extract URLs.
	//URLs := ctx.Args()
	//if len(URLs) < 2 {
	//	fatalIf(errDummy().Trace(ctx.Args()...), fmt.Sprintf("Unable to parse source and target arguments."))
	//}
}
