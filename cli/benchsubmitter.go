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
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

const warpSubmitterVersion = 1

type serverRequestOp string

const (
	serverReqDisconnect  serverRequestOp = "disconnect"
	serverReqBenchmark                   = "benchmark"
	serverReqStartStage                  = "start_stage"
	serverReqStageStatus                 = "stage_status"
	serverReqSendOps                     = "send_ops"
)

const serverFlagName = "serve"

type serverInfo struct {
	ID        string `json:"id"`
	Secret    string `json:"secret"`
	Version   int    `json:"version"`
	connected bool
}

// validate the serverinfo.
func (s serverInfo) validate() error {
	if s.ID == "" {
		return errors.New("no server id sent")
	}
	if s.Version != warpSubmitterVersion {
		return errors.New("warp server and agent version mismatch")
	}
	return nil
}

// serverRequest requests an operation from the agent and expects a response.
type serverRequest struct {
	Operation serverRequestOp `json:"op"`
	Benchmark struct {
		Command string            `json:"command"`
		Args    cli.Args          `json:"args"`
		Flags   map[string]string `json:"flags"`
	}
	Stage     benchmarkStage `json:"stage"`
	StartTime time.Time      `json:"start_time"`
}

// runServerBenchmark will run a benchmark server if requested.
// Returns a bool whether agents were specified.
func runServerBenchmark(ctx *cli.Context) (bool, error) {
	if ctx.Int("distributed") <= 0 {
		return false, nil
	}

	http.HandleFunc("/ws", serveWs)
	console.Infoln("Listening on", addr)
	fatalIf(probe.NewError(http.ListenAndServe(addr, nil)), "Unable to start client")

	conns := newConnections(parseEndpoints(ctx.String("agents")))
	if len(conns.endpoints) == 0 {
		return true, errors.New("no warp agents specified")
	}
	defer conns.closeAll()

	var infoLn = console.Infoln
	var errorLn = console.Errorln

	var allOps bench.Operations

	// Serialize parameters
	excludeFlags := map[string]struct{}{
		"agents":         {},
		"serverprof":     {},
		"autocompletion": {},
		"help":           {},
		"syncstart":      {},
		"inspect.out":    {},
	}
	req := serverRequest{
		Operation: serverReqBenchmark,
	}
	req.Benchmark.Command = ctx.Command.Name
	req.Benchmark.Args = ctx.Args()
	req.Benchmark.Flags = make(map[string]string)

	for _, flag := range ctx.Command.Flags {
		if _, ok := excludeFlags[flag.GetName()]; ok {
			continue
		}
		if ctx.IsSet(flag.GetName()) {
			var err error
			req.Benchmark.Flags[flag.GetName()], err = flagToJSON(ctx, flag)
			if err != nil {
				return true, err
			}
		}
	}

	// Connect to hosts, send benchmark requests.
	for i := range conns.ws {
		resp, err := conns.roundTrip(i, req)
		fatalIf(probe.NewError(err), "Unable to send benchmark info to warp agent")
		if resp.Err != "" {
			fatalIf(probe.NewError(errors.New(resp.Err)), "Error received from warp agent")
		}
		console.Infof("Agent %v connected...\n", conns.hostName(i))
		// Assume ok.
	}
	infoLn("All agents connected...")

	_ = conns.startStageAll(stagePrepare, time.Now().Add(time.Second), true)
	err := conns.waitForStage(stagePrepare, true)
	if err != nil {
		fatalIf(probe.NewError(err), "Failed to prepare")
	}
	infoLn("All agents prepared...")

	const benchmarkWait = 3 * time.Second

	prof, err := startProfiling(context.Background(), ctx)
	if err != nil {
		return true, err
	}
	err = conns.startStageAll(stageBenchmark, time.Now().Add(benchmarkWait), false)
	if err != nil {
		errorLn("Failed to start all agents", err)
	}
	err = conns.waitForStage(stageBenchmark, false)
	if err != nil {
		errorLn("Failed to keep connection to all agents", err)
	}

	fileName := ctx.String("benchdata")
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s-%s", appName, "remote", time.Now().Format("2006-01-02[150405]"), pRandASCII(4))
	}
	prof.stop(context.Background(), ctx, fileName+".profiles.zip")

	infoLn("Done. Downloading operations...")
	downloaded := conns.downloadOps()
	switch len(downloaded) {
	case 0:
	case 1:
		allOps = downloaded[0]
	default:
		threads := uint16(0)
		for _, ops := range downloaded {
			threads = ops.OffsetThreads(threads)
			allOps = append(allOps, ops...)
		}
	}

	allOps.SortByStartTime()
	f, err := os.Create(fileName + ".csv.zst")
	if err != nil {
		errorLn("Unable to write benchmark data:", err)
	} else {
		func() {
			defer f.Close()
			enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
			fatalIf(probe.NewError(err), "Unable to compress benchmark output")

			defer enc.Close()
			err = allOps.CSV(enc)
			fatalIf(probe.NewError(err), "Unable to write benchmark output")

			console.Infof("Benchmark data written to %q\n", fileName+".csv.zst")
		}()
	}
	err = conns.startStageAll(stageCleanup, time.Now(), false)
	if err != nil {
		errorLn("Failed to clean up all agents", err)
	}
	err = conns.waitForStage(stageCleanup, false)
	if err != nil {
		errorLn("Failed to keep connection to all agents", err)
	}
	printAnalysis(ctx, allOps)

	return true, nil
}

// connections keeps track of connections to agents.
type connections struct {
	endpoints []*url.URL
	ws        []*websocket.Conn
	si        serverInfo
}

// newConnections creates connections (but does not connect) to agents.
func newConnections(endpoints []*url.URL) *connections {
	var c connections
	c.si = serverInfo{
		ID:      pRandASCII(20),
		Secret:  "",
		Version: warpSubmitterVersion,
	}
	c.endpoints = endpoints
	c.ws = make([]*websocket.Conn, len(endpoints))
	return &c
}

// closeAll will close all connections.
func (c *connections) closeAll() {
	for i, conn := range c.ws {
		if conn != nil {
			conn.WriteJSON(serverRequest{Operation: serverReqDisconnect})
			conn.Close()
			c.ws[i] = nil
		}
	}
}

// hostName returns the remote host name of a connection.
func (c *connections) hostName(i int) string {
	if c.ws != nil && c.ws[i] != nil {
		return c.ws[i].RemoteAddr().String()
	}
	return c.endpoints[i].Host
}

// hostName returns the remote host name of a connection.
func (c *connections) disconnect(i int) {
	if c.ws[i] != nil {
		console.Infoln("Disconnecting agent", c.hostName(i))
		c.ws[i].WriteJSON(serverRequest{Operation: serverReqDisconnect})
		c.ws[i].Close()
		c.ws[i] = nil
	}
}

// roundTrip performs a roundtrip.
func (c *connections) roundTrip(i int, req serverRequest) (*agentReply, error) {
	conn := c.ws[i]
	if conn == nil {
		err := c.connect(i)
		if err != nil {
			return nil, err
		}
	}
	for {
		conn := c.ws[i]
		err := conn.WriteJSON(req)
		if err != nil {
			console.Error(err)
			if err := c.connect(i); err == nil {
				continue
			}
			return nil, err
		}
		var resp agentReply
		err = conn.ReadJSON(&resp)
		if err != nil {
			console.Error(err)
			if err := c.connect(i); err == nil {
				continue
			}
			return nil, err
		}
		return &resp, nil
	}
}

// connect to a agent.
func (c *connections) connect(i int) error {
	tries := 0
	for {
		err := func() error {
			endpoint := c.endpoints[i]
			if endpoint.Port() == "" {
				endpoint.Host = net.JoinHostPort(endpoint.Host, warpAgentDefaultPort)
			}
			// refer https://github.com/gorilla/websocket/blob/master/agent.go#L164
			switch endpoint.Scheme {
			case "http":
				endpoint.Scheme = "ws"
			case "https":
				endpoint.Scheme = "wss"
			}
			endpoint.Path = "/ws"
			console.Infoln("Connecting to", endpoint.Host)
			var err error
			c.ws[i], _, err = websocket.DefaultDialer.Dial(endpoint.String(), nil)
			if err != nil {
				return err
			}
			// Send server info
			err = c.ws[i].WriteJSON(c.si)
			if err != nil {
				return err
			}
			var resp agentReply
			err = c.ws[i].ReadJSON(&resp)
			if err != nil {
				return err
			}
			if resp.Err != "" {
				return errors.New(resp.Err)
			}
			delta := time.Since(resp.Time)
			if delta < 0 {
				delta = -delta
			}
			if delta > time.Second {
				return fmt.Errorf("host %v time delta too big (%v). Synchronize clock on agent and retry", endpoint.Host, delta)
			}
			return nil
		}()
		if err == nil {
			return nil
		}
		if tries == 3 {
			c.ws[i] = nil
			return err
		}
		console.Errorf("Connection failed: %v, retrying...\n", err)
		tries++
		time.Sleep(time.Second)
	}
}

// startStage will start a stage at a specific time on a agent.
func (c *connections) startStage(i int, t time.Time, stage benchmarkStage) error {
	req := serverRequest{
		Operation: serverReqStartStage,
		Stage:     stage,
		StartTime: t,
	}
	resp, err := c.roundTrip(i, req)
	if err != nil {
		return err
	}
	if resp.Err != "" {
		console.Errorf("Agent %v returned error: %v\n", c.hostName(i), resp.Err)
		return errors.New(resp.Err)
	}
	console.Infof("Agent %v: Requested stage %v start..\n", c.hostName(i), stage)
	return nil
}

// startStageAll will start a stage at a specific time on all connected agents.
func (c *connections) startStageAll(stage benchmarkStage, startAt time.Time, failOnErr bool) error {
	var wg sync.WaitGroup
	var gerr error
	var mu sync.Mutex
	console.Infof("Requesting stage %v start...\n", stage)

	for i, conn := range c.ws {
		if conn == nil {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := c.startStage(i, startAt, stage)
			if err != nil {
				if failOnErr {
					fatalIf(probe.NewError(err), "Stage start failed.")
				}
				console.Errorln("Starting stage error:", err)
				mu.Lock()
				if gerr == nil {
					gerr = err
				}
				c.ws[i] = nil
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return gerr
}

// downloadOps will download operations from all connected agents.
// If an error is encountered the result will be ignored.
func (c *connections) downloadOps() []bench.Operations {
	var wg sync.WaitGroup
	var mu sync.Mutex
	console.Infof("Downloading operations...\n")
	res := make([]bench.Operations, 0, len(c.ws))
	for i, conn := range c.ws {
		if conn == nil {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				resp, err := c.roundTrip(i, serverRequest{Operation: serverReqSendOps})
				if err != nil {
					return
				}
				if resp.Err != "" {
					console.Errorf("Agent %v returned error: %v\n", c.hostName(i), resp.Err)
					return
				}
				console.Infof("Agent %v: Operations downloaded.\n", c.hostName(i))

				mu.Lock()
				res = append(res, resp.Ops)
				mu.Unlock()
				return
			}
		}(i)
	}
	wg.Wait()
	return res
}

// waitForStage will wait for stage completion on all agents.
func (c *connections) waitForStage(stage benchmarkStage, failOnErr bool) error {
	var wg sync.WaitGroup
	for i, conn := range c.ws {
		if conn == nil {
			// log?
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				req := serverRequest{
					Operation: serverReqStageStatus,
					Stage:     stage,
				}
				resp, err := c.roundTrip(i, req)
				if err != nil {
					c.disconnect(i)
					if failOnErr {
						fatalIf(probe.NewError(err), "Stage failed.")
					}
					console.Errorln(err)
					return
				}
				if resp.Err != "" {
					c.disconnect(i)
					if failOnErr {
						fatalIf(probe.NewError(errors.New(resp.Err)), "Stage failed. Agent %v returned error.", c.hostName(i))
					}
					console.Errorf("Agent %v returned error: %v\n", c.hostName(i), resp.Err)
					return
				}
				if resp.StageInfo.Finished {
					console.Infof("Agent %v: Finished stage %v...\n", c.hostName(i), stage)
					return
				}
				time.Sleep(time.Second)
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// flagToJSON converts a flag to a representation that can be reversed into the flag.
func flagToJSON(ctx *cli.Context, flag cli.Flag) (string, error) {
	switch flag.(type) {
	case cli.StringFlag:
		if ctx.IsSet(flag.GetName()) {
			return ctx.String(flag.GetName()), nil
		}
	case cli.BoolFlag:
		if ctx.IsSet(flag.GetName()) {
			return "true", nil
		}
	case cli.Int64Flag:
		if ctx.IsSet(flag.GetName()) {
			return fmt.Sprint(ctx.Int64(flag.GetName())), nil
		}
	case cli.IntFlag:
		if ctx.IsSet(flag.GetName()) {
			return fmt.Sprint(ctx.Int(flag.GetName())), nil
		}
	case cli.DurationFlag:
		if ctx.IsSet(flag.GetName()) {
			return ctx.Duration(flag.GetName()).String(), nil
		}
	case cli.UintFlag:
		if ctx.IsSet(flag.GetName()) {
			return fmt.Sprint(ctx.Uint(flag.GetName())), nil
		}
	case cli.Uint64Flag:
		if ctx.IsSet(flag.GetName()) {
			return fmt.Sprint(ctx.Uint64(flag.GetName())), nil
		}
	case cli.Float64Flag:
		if ctx.IsSet(flag.GetName()) {
			return fmt.Sprint(ctx.Float64(flag.GetName())), nil
		}
	default:
		if ctx.IsSet(flag.GetName()) {
			return "", fmt.Errorf("unhandled flag type: %T", flag)
		}
	}
	return "", nil
}
