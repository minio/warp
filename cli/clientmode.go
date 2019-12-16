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
	"errors"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
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
...
 `,
}

// mainPut is the entry point for cp command.
func mainClient(ctx *cli.Context) error {
	addr := "127.0.0.1:7761"
	switch ctx.NArg() {
	case 1:
		addr = ctx.Args()[0]
		if !strings.Contains(addr, ":") {
			addr += ":7761"
		}
	case 0:
	default:
		fatal(errInvalidArgument(), "Too many parameters")
	}
	http.HandleFunc("/ws", serveWs)
	console.Infoln("Listening on", addr)
	return http.ListenAndServe(addr, nil)
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

const warpServerVersion = 1

type serverInfo struct {
	ID      string `json:"id"`
	Secret  string `json:"secret"`
	Version int    `json:"version"`
}

type clientReply struct {
	Err string `json:"err,omitempty"`
}

type serverRequest struct {
	Operation string `json:"op"`
}

func (s serverInfo) validate() error {
	if s.ID == "" {
		return errors.New("no server id sent")
	}
	if s.Version != warpServerVersion {
		return errors.New("warp server and client version mismatch")
	}
	return nil
}

var connectedMu sync.Mutex
var connected serverInfo

func serveWs(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		console.Error("upgrade:", err.Error())
		return
	}

	defer func() {
		ws.Close()
		console.Infoln("Closing connection")
	}()
	var s serverInfo
	err = ws.ReadJSON(&s)
	if err != nil {
		console.Error("Error reading server info:", err.Error())
		return
	}
	if err = s.validate(); err != nil {
		ws.WriteJSON(clientReply{Err: err.Error()})
		return
	}

	connectedMu.Lock()
	if connected.ID == "" {
		// First connection.
		connected = s
	} else if connected.ID != s.ID {
		err = errors.New("another server already connected")
	}
	connectedMu.Unlock()
	if err != nil {
		ws.WriteJSON(clientReply{Err: err.Error()})
		return
	}

	console.Infoln("Accepting connection from server:", s.ID)
	defer func() {
		// When we return, reset connection info.
		connectedMu.Lock()
		connected = serverInfo{}
		connectedMu.Unlock()
	}()
	for {
		var req serverRequest
		err := ws.ReadJSON(&req)
		if err != nil {
			console.Error("Reading server message:", err.Error())
			return
		}
		switch req.Operation {
		case "", serverReqDisconnect:
			return
		case serverReqBenchmark:
		}
	}
}

const (
	serverReqDisconnect = "disconnect"
	serverReqBenchmark  = "benchmark"
)

func checkClientSyntax(ctx *cli.Context) {

}
