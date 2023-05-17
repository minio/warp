/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cli

import (
	"fmt"
	"os"

	"github.com/minio/cli"
	"github.com/minio/pkg/console"
)

// Collection of warp flags currently supported
var globalFlags = []cli.Flag{
	cli.BoolFlag{
		Name:   "quiet, q",
		Usage:  "disable progress bar display",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:  "no-color",
		Usage: "disable color theme",
	},
	cli.BoolFlag{
		Name:   "json",
		Usage:  "enable JSON formatted output",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:  "debug",
		Usage: "enable debug output",
	},
	cli.BoolFlag{
		Name:  "insecure",
		Usage: "disable TLS certificate verification",
	},
	cli.BoolFlag{
		Name:  "autocompletion",
		Usage: "install auto-completion for your shell",
	},
}

var profileFlags = []cli.Flag{
	cli.StringFlag{
		Name:   "pprofdir",
		Usage:  "Write profiles to this folder",
		Value:  "pprof",
		Hidden: true,
	},

	cli.BoolFlag{
		Name:   "cpu",
		Usage:  "Write a local CPU profile",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:   "mem",
		Usage:  "Write an local allocation profile",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:   "block",
		Usage:  "Write a local goroutine blocking profile",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:   "mutex",
		Usage:  "Write a mutex contention profile",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:   "threads",
		Usage:  "Write a threas create profile",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:   "trace",
		Usage:  "Write an local execution trace",
		Hidden: true,
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

	// Disable colorified messages if requested.
	if globalNoColor || globalQuiet {
		console.SetColorOff()
	}
}

// commandLine attempts to reconstruct the commandline.
func commandLine(ctx *cli.Context) string {
	s := os.Args[0] + " " + ctx.Command.Name
	for _, flag := range ctx.Command.Flags {
		val, err := flagToJSON(ctx, flag)
		if err != nil || val == "" {
			continue
		}
		name := flag.GetName()
		switch name {
		case "access-key", "secret-key":
			val = "*REDACTED*"
		}
		s += " --" + flag.GetName() + "=" + val
	}
	return s
}

// Flags common across all I/O commands such as cp, mirror, stat, pipe etc.
var ioFlags = []cli.Flag{
	cli.StringFlag{
		Name:   "host",
		Usage:  "host. Multiple hosts can be specified as a comma separated list.",
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
		Name:   "region",
		Usage:  "Specify a custom region",
		EnvVar: appNameUC + "_REGION",
	},
	cli.StringFlag{
		Name:   "signature",
		Usage:  "Specify a signature method. Available values are S3V2, S3V4",
		Value:  "S3V4",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:  "encrypt",
		Usage: "encrypt/decrypt objects (using server-side encryption with random keys)",
	},
	cli.StringFlag{
		Name:  "bucket",
		Value: appName + "-benchmark-bucket",
		Usage: "Bucket to use for benchmark data. ALL DATA WILL BE DELETED IN BUCKET!",
	},
	cli.StringFlag{
		Name:  "host-select",
		Value: string(hostSelectTypeWeighed),
		Usage: fmt.Sprintf("Host selection algorithm. Can be %q or %q", hostSelectTypeWeighed, hostSelectTypeRoundrobin),
	},
	cli.BoolFlag{
		Name:   "resolve-host",
		Usage:  "Resolve the host(s) ip(s) (including multiple A/AAAA records). This can break SSL certificates, use --insecure if so",
		Hidden: true,
	},
	cli.IntFlag{
		Name:  "concurrent",
		Value: 20,
		Usage: "Run this many concurrent operations",
	},
	cli.BoolFlag{
		Name:  "noprefix",
		Usage: "Do not use separate prefix for each thread",
	},
	cli.StringFlag{
		Name:  "prefix",
		Usage: "Use a custom prefix for each thread",
	},
	cli.BoolFlag{
		Name:  "disable-multipart",
		Usage: "disable multipart uploads",
	},
	cli.BoolFlag{
		Name:  "md5",
		Usage: "Add MD5 sum to uploads",
	},
	cli.StringFlag{
		Name:  "storage-class",
		Value: "",
		Usage: "Specify custom storage class, for instance 'STANDARD' or 'REDUCED_REDUNDANCY'.",
	},
	cli.BoolFlag{
		Name:   "disable-http-keepalive",
		Usage:  "Disable HTTP Keep-Alive",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:   "http2",
		Usage:  "enable HTTP2 support if server supports it",
		Hidden: true,
	},
}
