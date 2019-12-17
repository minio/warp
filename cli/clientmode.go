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
	"flag"
	"fmt"
	"net/http"
	"strconv"
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

const warpServerDefaultPort = 7761

// mainPut is the entry point for cp command.
func mainClient(ctx *cli.Context) error {
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

func (s serverInfo) validate() error {
	if s.ID == "" {
		return errors.New("no server id sent")
	}
	if s.Version != warpServerVersion {
		return errors.New("warp server and client version mismatch")
	}
	return nil
}

type clientReply struct {
	Err string `json:"err,omitempty"`
}

type serverRequest struct {
	Operation string `json:"op"`
	Benchmark struct {
		Command string
		Args    cli.Args
		Flags   map[string]string
	}
}

// executeBenchmark will execute the benchmark and return any error.
func (s serverRequest) executeBenchmark() error {
	// Reconstruct
	app := registerApp("warp", benchCmds)
	cmd := app.Command(s.Benchmark.Command)
	fs, err := flagSet(cmd.Name, cmd.Flags, s.Benchmark.Args)
	if err != nil {
		return err
	}
	ctx2 := cli.NewContext(app, fs, nil)
	ctx2.Args()
	for k, v := range s.Benchmark.Flags {
		err := ctx2.Set(k, v)
		if err != nil {
			return err
		}
	}
	console.Infoln("Executing", cmd.Name, "benchmark.")
	console.Infoln("Params:", s.Benchmark.Flags, ctx2.Args())
	return runCommand(ctx2, cmd)
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
			err := req.executeBenchmark()
			var resp clientReply
			if err != nil {
				resp.Err = err.Error()
			}
			ws.WriteJSON(resp)
		}
	}
}

func flagSet(name string, flags []cli.Flag, args []string) (*flag.FlagSet, error) {
	set := flag.NewFlagSet(name, flag.ContinueOnError)
	err := set.Parse(args)
	if err != nil {
		return nil, err
	}
	for _, f := range flags {
		f.Apply(set)
	}
	return set, nil
}

// runCommand invokes the command given the context.
func runCommand(ctx *cli.Context, c *cli.Command) (err error) {
	if c.After != nil {
		defer func() {
			afterErr := c.After(ctx)
			if afterErr != nil {
				cli.HandleExitCoder(err)
				if err != nil {
					err = cli.NewMultiError(err, afterErr)
				} else {
					err = afterErr
				}
			}
		}()
	}

	if c.Before != nil {
		err = c.Before(ctx)
		if err != nil {
			fmt.Fprintln(ctx.App.Writer, err)
			fmt.Fprintln(ctx.App.Writer)
			return err
		}
	}

	if c.Action == nil {
		return errors.New("no action")
	}

	return cli.HandleAction(c.Action, ctx)
}

const (
	serverReqDisconnect = "disconnect"
	serverReqBenchmark  = "benchmark"
)

func checkClientSyntax(ctx *cli.Context) {

}
