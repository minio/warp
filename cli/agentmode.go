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
	"strings"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio/pkg/console"
)

var (
	agentFlags = []cli.Flag{}
)

// Agent command.
var agentCmd = cli.Command{
	Name:   "agent",
	Usage:  "run warp in agent mode, accepting connections to run benchmarks",
	Action: mainAgent,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, agentFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] [listen address]

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  1. Listen on port '6001' with ip 192.168.1.101:
     {{.Prompt}} {{.HelpName}} 192.168.1.101:6001
 `,
}

const warpAgentDefaultPort = "7761"

// mainPut is the entry point for cp command.
func mainAgent(ctx *cli.Context) error {
	checkAgentSyntax(ctx)
	addr := ":" + warpAgentDefaultPort
	switch ctx.NArg() {
	case 1:
		addr = ctx.Args()[0]
		if !strings.Contains(addr, ":") {
			addr += ":" + warpAgentDefaultPort
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

func checkAgentSyntax(ctx *cli.Context) {
}
