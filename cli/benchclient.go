package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

type clientReply struct {
	Type      clientReplyType  `json:"type"`
	Time      time.Time        `json:"time"`
	Err       string           `json:"err,omitempty"`
	Ops       bench.Operations `json:"ops,omitempty"`
	StageInfo struct {
		Started  bool    `json:"started"`
		Finished bool    `json:"finished"`
		Progress float64 `json:"progress"`
	}
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
	activeBenchmark = &cb

	console.Infoln("Executing", cmd.Name, "benchmark.")
	console.Infoln("Params:", s.Benchmark.Flags, ctx2.Args())
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

var connectedMu sync.Mutex
var connected serverInfo

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

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
			if activeBenchmark != nil {
				activeBenchmark.Lock()
				activeBenchmark.cancel()
				activeBenchmark.Unlock()
			}
			connectedMu.Lock()
			connected = serverInfo{}
			connectedMu.Unlock()
			return
		case serverReqBenchmark:
			_, err := req.executeBenchmark(context.Background())
			resp.Type = clientRespBenchmarkStarted
			if err != nil {
				console.Errorln("Starting benchmark:", err)
				resp.Err = err.Error()
			}
		case serverReqStartStage:
			if activeBenchmark == nil {
				resp.Err = "no benchmark running"
				break
			}
			activeBenchmark.Lock()
			stageInfo := activeBenchmark.info
			activeBenchmark.Unlock()
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
			activeBenchmark.Lock()
			activeBenchmark.info[req.Stage] = info
			activeBenchmark.Unlock()

			wait := time.Until(req.Time)
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
			if activeBenchmark == nil {
				resp.Err = "no benchmark running"
				break
			}
			resp.Type = clientRespStatus
			activeBenchmark.Lock()
			stageInfo := activeBenchmark.info
			activeBenchmark.Unlock()
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
			default:
			}
		case serverReqSendOps:
			if activeBenchmark == nil {
				resp.Err = "no benchmark running"
				break
			}
			resp.Type = clientRespOps
			activeBenchmark.Lock()
			resp.Ops = activeBenchmark.results
			activeBenchmark.Unlock()
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

type serverRequestOp string
type clientReplyType string

const (
	serverReqDisconnect  serverRequestOp = "disconnect"
	serverReqBenchmark                   = "benchmark"
	serverReqStartStage                  = "start_stage"
	serverReqStageStatus                 = "stage_status"
	serverReqSendOps                     = "send_ops"

	clientRespBenchmarkStarted clientReplyType = "benchmark_started"
	clientRespStatus           clientReplyType = "benchmark_status"
	clientRespOps              clientReplyType = "ops"
)
