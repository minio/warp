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
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/minio/cli"
	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/websocket"
)

// clientReplyType indicates the client reply type.
type clientReplyType string

const (
	clientRespBenchmarkStarted clientReplyType = "benchmark_started"
	clientRespStatus           clientReplyType = "benchmark_status"
	clientRespOps              clientReplyType = "ops"
)

// clientReply contains the response to a server request.
type clientReply struct {
	Type      clientReplyType  `json:"type"`
	Time      time.Time        `json:"time"`
	Err       string           `json:"err,omitempty"`
	Ops       bench.Operations `json:"ops,omitempty"`
	StageInfo struct {
		Started  bool              `json:"started"`
		Finished bool              `json:"finished"`
		Progress float64           `json:"progress"`
		Custom   map[string]string `json:"custom,omitempty"`
	} `json:"stage_info"`
}

// executeBenchmark will execute the benchmark and return any error.
func (s serverRequest) executeBenchmark(ctx context.Context) (*clientBenchmark, error) {
	// Reconstruct
	app := registerApp("warp", benchCmds)
	cmd := app.Command(s.Benchmark.Command)
	if cmd == nil {
		return nil, fmt.Errorf("command %v not found", s.Benchmark.Command)
	}
	fs, err := flagSet(cmd.Name, cmd.Flags, s.Benchmark.Args)
	if err != nil {
		return nil, err
	}
	ctx2 := cli.NewContext(app, fs, nil)
	ctx2.Command = *cmd
	for k, v := range s.Benchmark.Flags {
		err := ctx2.Set(k, v)
		if err != nil {
			err := fmt.Errorf("parsing parameters (%v:%v): %w", k, v, err)
			return nil, err
		}
	}
	var cb clientBenchmark
	cb.init(ctx)
	cb.clientIdx = s.ClientIdx
	activeBenchmarkMu.Lock()
	activeBenchmark = &cb
	activeBenchmarkMu.Unlock()

	console.Infoln("Executing", cmd.Name, "benchmark.")
	if globalDebug {
		// params have secret, so disable by default.
		console.Infoln("Params:", s.Benchmark.Flags, ctx2.Args())
	}
	go func() {
		err := runCommand(ctx2, cmd)
		cb.Lock()
		if err != nil {
			cb.err = err
		}
		cb.Unlock()
		cb.setStage(stageDone)
	}()
	return &cb, nil
}

// Information on currently or last connected server.
var (
	connectedMu sync.Mutex
	connected   serverInfo
)

// wsUpgrader performs websocket upgrades.
var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// serveWs handles incoming requests.
func serveWs(w http.ResponseWriter, r *http.Request) {
	ws, err := wsUpgrader.Upgrade(w, r, nil)
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

	s.connected = true
	connectedMu.Lock()
	if connected.ID == "" || !connected.connected {
		// First connection or server disconnected.
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
		connected.connected = false
		connectedMu.Unlock()
		ws.Close()
	}()

	// Confirm the connection
	err = ws.WriteJSON(clientReply{Time: time.Now()})
	if err != nil {
		console.Error("Writing response:", err)
		return
	}
	for {
		var req serverRequest
		err := ws.ReadJSON(&req)
		if err != nil {
			console.Error("Reading server message:", err.Error())
			return
		}
		if globalDebug {
			console.Infof("Request: %v\n", req.Operation)
		}
		var resp clientReply
		switch req.Operation {
		case serverReqDisconnect:
			console.Infoln("Received Disconnect")
			activeBenchmarkMu.Lock()
			ab := activeBenchmark
			activeBenchmarkMu.Unlock()
			if ab != nil {
				ab.cancel()
			}
			connectedMu.Lock()
			connected = serverInfo{}
			connectedMu.Unlock()
			return
		case serverReqBenchmark:
			activeBenchmarkMu.Lock()
			ab := activeBenchmark
			activeBenchmarkMu.Unlock()
			if ab != nil {
				ab.cancel()
			}
			_, err := req.executeBenchmark(context.Background())
			resp.Type = clientRespBenchmarkStarted
			if err != nil {
				console.Errorln("Starting benchmark:", err)
				resp.Err = err.Error()
			}
		case serverReqStartStage:
			activeBenchmarkMu.Lock()
			ab := activeBenchmark
			activeBenchmarkMu.Unlock()
			if ab == nil {
				resp.Err = "no benchmark running"
				break
			}
			ab.Lock()
			stageInfo := ab.info
			ab.Unlock()
			info, ok := stageInfo[req.Stage]
			if !ok {
				resp.Err = "stage not found"
				break
			}
			if info.startRequested {
				resp.Type = clientRespStatus
				break
			}
			info.startRequested = true
			ab.Lock()
			ab.info[req.Stage] = info
			ab.Unlock()

			wait := time.Until(req.StartTime)
			if wait < 0 {
				wait = 0
			}
			console.Infoln("Starting stage", req.Stage, "in", wait)
			go func() {
				time.Sleep(wait)
				close(info.start)
			}()
			resp.Type = clientRespStatus
		case serverReqStageStatus:
			activeBenchmarkMu.Lock()
			ab := activeBenchmark
			activeBenchmarkMu.Unlock()
			if ab == nil {
				resp.Err = "no benchmark running"
				break
			}
			resp.Type = clientRespStatus
			ab.Lock()
			err := ab.err
			stageInfo := ab.info
			ab.Unlock()
			if err != nil {
				resp.Err = err.Error()
				break
			}
			info, ok := stageInfo[req.Stage]
			if !ok {
				resp.Err = "stage not found"
				break
			}
			select {
			case <-info.start:
				resp.StageInfo.Started = true
			default:
			}
			select {
			case <-info.done:
				resp.StageInfo.Finished = true
				resp.StageInfo.Custom = info.custom
			default:
			}
		case serverReqSendOps:
			activeBenchmarkMu.Lock()
			ab := activeBenchmark
			activeBenchmarkMu.Unlock()
			if ab == nil {
				resp.Err = "no benchmark running"
				break
			}
			resp.Type = clientRespOps
			ab.Lock()
			resp.Ops = ab.results
			ab.Unlock()
		default:
			resp.Err = "unknown command"
		}
		resp.Time = time.Now()
		if globalDebug {
			console.Infof("Sending %v\n", resp.Type)
		}
		err = ws.WriteJSON(resp)
		if err != nil {
			console.Error("Writing response:", err)
			return
		}
	}
}

// flagSet converts args and flags to a flagset.
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
