package cli

import (
	"runtime"

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

// Flags common across all I/O commands such as cp, mirror, stat, pipe etc.
var ioFlags = []cli.Flag{
	cli.StringFlag{
		Name:   "host",
		Usage:  "host",
		EnvVar: appNameUC + "_HOST",
		Value:  "127.0.0.1:9000",
	},
	cli.StringFlag{
		Name:   "access-key",
		Usage:  "Specify access key",
		EnvVar: appNameUC + "_ACCESS_KEY",
		Value:  "",
	},
	cli.StringFlag{
		Name:   "secret-key",
		Usage:  "Specify secret key",
		EnvVar: appNameUC + "_SECRET_KEY",
		Value:  "",
	},
	cli.BoolFlag{
		Name:   "tls",
		Usage:  "Use TLS (HTTPS) for transport",
		EnvVar: appNameUC + "_TLS",
	},
	cli.StringFlag{
		Name:  "encrypt-key",
		Usage: "encrypt/decrypt objects (using server-side encryption with customer provided keys)",
	},
	cli.StringFlag{
		Name:  "bucket",
		Value: appName + "-benchmark-bucket",
		Usage: "Bucket to use for benchmark data. ALL DATA WILL BE DELETED IN BUCKET!",
	},
	cli.IntFlag{
		Name:  "concurrent",
		Value: runtime.GOMAXPROCS(0),
		Usage: "Run this many concurrent operations",
	},
	cli.BoolFlag{
		Name:  "no-prefix",
		Usage: "Use separate prefix for each uploader",
	},
}
