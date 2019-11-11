package cli

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/minio/cli"
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
	Usage:  "put objects",
	Action: mainPut,
	Before: setGlobalsFromContext,
	Flags:  append(append(append(putFlags, ioFlags...), globalFlags...), genFlags...),
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
func mainPut(ctx *cli.Context) error {
	checkPutSyntax(ctx)
	src := newGenSource(ctx)
	cl, err := minio.New(ctx.String("host"), ctx.String("access-key"), ctx.String("secret-key"), false)
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
	log.Println("Preparing server.")
	b.Prepare(context.Background())

	tStart := time.Now().Add(time.Second)
	ctx2, cancel := context.WithDeadline(context.Background(), tStart.Add(time.Minute))
	defer cancel()
	start := make(chan struct{})
	go func() {
		<-time.After(time.Until(tStart))
		close(start)
	}()
	log.Println("Done. Starting benchmark")
	ops := b.Start(ctx2, start)
	ops.SortByStartTime()
	log.Println("Done. Starting cleanup")
	b.Cleanup(context.Background())

	return ops.CSV(os.Stdout)
}

func checkPutSyntax(ctx *cli.Context) {
	//if len(ctx.Args()) < 2 {
	//	cli.ShowCommandHelpAndExit(ctx, "put", 1) // last argument is exit code.
	//}
	// extract URLs.
	//URLs := ctx.Args()
	//if len(URLs) < 2 {
	//	fatalIf(errDummy().Trace(ctx.Args()...), fmt.Sprintf("Unable to parse source and target arguments."))
	//}
}
