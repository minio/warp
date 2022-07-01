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
	"net/http"
	"strconv"
	"strings"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/console"
)

var clientFlags = []cli.Flag{}

// Put command.
var clientCmd = cli.Command{
	Name:   "client",
	Usage:  "run warp in client mode, accepting connections to run benchmarks",
	Action: mainClient,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, clientFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] [listen address]
  -> see https://github.com/minio/warp#multiple-hosts

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  1. Listen on port '6001' with ip 192.168.1.101:
     {{.Prompt}} {{.HelpName}} 192.168.1.101:6001
 `,
}

const warpServerDefaultPort = 7761

// mainPut is the entry point for cp command.
func mainClient(ctx *cli.Context) error {
	checkClientSyntax(ctx)
	addr := ":" + strconv.Itoa(warpServerDefaultPort)
	switch ctx.NArg() {
	case 1:
		addr = ctx.Args()[0]
		if !strings.Contains(addr, ":") {
			addr += ":" + strconv.Itoa(warpServerDefaultPort)
		}
	case 0:
	default:
		fatal(errInvalidArgument(), "Too many parameters")
	}
	http.HandleFunc("/ws", serveWs)
	console.Infoln("Listening on", addr)
	fatalIf(probe.NewError(http.ListenAndServe(addr, nil)), "Unable to start client")
	return nil
}

func checkClientSyntax(ctx *cli.Context) {
}
