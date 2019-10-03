package cli

import (
	"fmt"

	"github.com/minio/cli"
	"github.com/minio/warp/pkg/sse"
)

// cp command flags.
var (
	putFlags = []cli.Flag{}
)

// Copy command.
var putCmd = cli.Command{
	Name:   "cp",
	Usage:  "copy objects",
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
	encKeyDB, err := sse.EncKeys(ctx)
	fatalIf(err, "Unable to parse encryption keys.")

	checkPutSyntax(ctx, encKeyDB)
	return nil
}

func checkPutSyntax(ctx *cli.Context, encKeyDB map[string][]sse.PrefixSSEPair) {
	if len(ctx.Args()) < 2 {
		cli.ShowCommandHelpAndExit(ctx, "cp", 1) // last argument is exit code.
	}
	// extract URLs.
	URLs := ctx.Args()
	if len(URLs) < 2 {
		fatalIf(errDummy().Trace(ctx.Args()...), fmt.Sprintf("Unable to parse source and target arguments."))
	}
}
