package cli

import (
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
)

// Collection of mc flags currently supported
var globalFlags = []cli.Flag{
	// These flags mimmic the `go test` flags.
	cli.StringFlag{
		Name:  "cpuprofile",
		Value: "",
		Usage: "Write a local CPU profile to the specified file before exiting.",
	},
	cli.StringFlag{
		Name:  "memprofile",
		Value: "",
		Usage: "Write an local allocation profile to the file after all tests have passed.",
	},
	cli.StringFlag{
		Name:  "blockprofile",
		Value: "",
		Usage: "Write a local goroutine blocking profile to the specified file when all tests are complete.",
	},
	cli.StringFlag{
		Name:  "mutexprofile",
		Value: "",
		Usage: "Write a mutex contention profile to the specified file when all tests are complete.",
	},
	cli.StringFlag{
		Name:  "trace",
		Value: "",
		Usage: "Write an local execution trace to the specified file before exiting.",
	},
	cli.BoolFlag{
		Name:  "quiet, q",
		Usage: "disable progress bar display",
	},
	cli.BoolFlag{
		Name:  "no-color",
		Usage: "disable color theme",
	},
	cli.BoolFlag{
		Name:  "json",
		Usage: "enable JSON formatted output",
	},
	cli.BoolFlag{
		Name:  "debug",
		Usage: "enable debug output",
	},
	cli.BoolFlag{
		Name:  "insecure",
		Usage: "disable SSL certificate verification",
	},
	cli.BoolFlag{
		Name:  "autocompletion",
		Usage: "install auto-completion for your shell",
	},
}

// Set global states. NOTE: It is deliberately kept monolithic to ensure we dont miss out any flags.
func setGlobalsFromContext(ctx *cli.Context) error {
	quiet := ctx.IsSet("quiet")
	debug := ctx.IsSet("debug")
	json := ctx.IsSet("json")
	noColor := ctx.IsSet("no-color")
	setGlobals(quiet, debug, json, noColor)
	return nil
}

// Set global states. NOTE: It is deliberately kept monolithic to ensure we dont miss out any flags.
func setGlobals(quiet, debug, json, noColor bool) {
	globalQuiet = globalQuiet || quiet
	globalDebug = globalDebug || debug
	globalJSON = globalJSON || json
	globalNoColor = globalNoColor || noColor

	// Enable debug messages if requested.
	if globalDebug {
		console.DebugPrint = true
	}

	// Disable colorified messages if requested.
	if globalNoColor || globalQuiet {
		console.SetColorOff()
	}
}
