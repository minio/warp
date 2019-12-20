/*
 * Warp (C) 2019- MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cli

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
)

var (
	clientFlags = []cli.Flag{}
)

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

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
   warp client 127.0.0.1:6001
 `,
}

const warpServerDefaultPort = 7761

// mainPut is the entry point for cp command.
func mainClient(ctx *cli.Context) error {
	checkClientSyntax(ctx)
	addr := "127.0.0.1:" + strconv.Itoa(warpServerDefaultPort)
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
	return http.ListenAndServe(addr, nil)
}

func checkClientSyntax(ctx *cli.Context) {
}
