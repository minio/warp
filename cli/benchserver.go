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
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/warp/api"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/websocket"
)

const warpServerVersion = 1

type serverRequestOp string

const (
	serverReqDisconnect  serverRequestOp = "disconnect"
	serverReqBenchmark   serverRequestOp = "benchmark"
	serverReqStartStage  serverRequestOp = "start_stage"
	serverReqStageStatus serverRequestOp = "stage_status"
	serverReqSendOps     serverRequestOp = "send_ops"
)

const serverFlagName = "serve"

type serverInfo struct {
	ID        string `json:"id"`
	Secret    string `json:"secret"`
	Version   int    `json:"version"`
	connected bool
}

type AfterPreparer interface {
	AfterPrepare(ctx context.Context) error
}

// validate the serverinfo.
func (s serverInfo) validate() error {
	if s.ID == "" {
		return errors.New("no server id sent")
	}
	if s.Version != warpServerVersion {
		return errors.New("warp server and client version mismatch")
	}
	return nil
}

// serverRequest requests an operation from the client and expects a response.
type serverRequest struct {
	Benchmark struct {
		Flags   map[string]string `json:"flags"`
		Command string            `json:"command"`
		Args    cli.Args          `json:"args"`
	}
	StartTime time.Time       `json:"start_time"`
	Operation serverRequestOp `json:"op"`
	Stage     benchmarkStage  `json:"stage"`
	ClientIdx int             `json:"client_idx"`
}

// runServerBenchmark will run a benchmark server if requested.
// Returns a bool whether clients were specified.
func runServerBenchmark(ctx *cli.Context, b bench.Benchmark) (bool, error) {
	if ctx.String("warp-client") == "" {
		return false, nil
	}

	conns := newConnections(parseHosts(ctx.String("warp-client"), false))
	if len(conns.hosts) == 0 {
		return true, errors.New("no hosts")
	}
	conns.info = printInfo
	conns.errLn = printError
	defer conns.closeAll()
	monitor := api.NewBenchmarkMonitor(ctx.String(serverFlagName))
	defer monitor.Done()
	monitor.SetLnLoggers(printInfo, printError)
	infoLn := monitor.InfoLn
	errorLn := monitor.Errorln

	var allOps bench.Operations

	// Serialize parameters
	excludeFlags := map[string]struct{}{
		"warp-client":        {},
		"warp-client-server": {},
		"serverprof":         {},
		"autocompletion":     {},
		"help":               {},
		"syncstart":          {},
		"analyze.out":        {},
	}
	transformFlags := map[string]func(flag cli.Flag) (string, error){
		// Special handling for hosts, we read files and expand it.
		"host": func(flag cli.Flag) (string, error) {
			hostsIn := flag.String()
			if !strings.Contains(hostsIn, "file:") {
				return flagToJSON(ctx, flag)
			}
			// This is a file, we will read it locally and expand.
			hosts := parseHosts(hostsIn, false)
			// Rejoin
			return strings.Join(hosts, ","), nil
		},
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
			if t := transformFlags[flag.GetName()]; t != nil {
				req.Benchmark.Flags[flag.GetName()], err = t(flag)
			} else {
				req.Benchmark.Flags[flag.GetName()], err = flagToJSON(ctx, flag)
			}
			if err != nil {
				return true, err
			}
		}
	}
	for k, v := range b.GetCommon().ExtraFlags {
		req.Benchmark.Flags[k] = v
	}

	// Connect to hosts, send benchmark requests.
	for i := range conns.hosts {
		resp, err := conns.roundTrip(i, req)
		fatalIf(probe.NewError(err), "Unable to send benchmark info to warp client")
		if resp.Err != "" {
			fatalIf(probe.NewError(errors.New(resp.Err)), "Error received from warp client")
		}
		infoLn("Client ", conns.hostName(i), " connected...")
		// Assume ok.
	}
	infoLn("All clients connected...")

	common := b.GetCommon()
	_ = conns.startStageAll(stagePrepare, time.Now().Add(time.Second), true)
	err := conns.waitForStage(stagePrepare, true, common)
	if err != nil {
		fatalIf(probe.NewError(err), "Failed to prepare")
	}
	if ap, ok := b.(AfterPreparer); ok {
		err := ap.AfterPrepare(context.Background())
		fatalIf(probe.NewError(err), "Error preparing server")
	}

	infoLn("All clients prepared...")

	const benchmarkWait = 3 * time.Second

	prof, err := startProfiling(context.Background(), ctx)
	if err != nil {
		return true, err
	}
	err = conns.startStageAll(stageBenchmark, time.Now().Add(benchmarkWait), false)
	if err != nil {
		errorLn("Failed to start all clients", err)
	}
	infoLn("Running benchmark on all clients...")
	err = conns.waitForStage(stageBenchmark, false, common)
	if err != nil {
		errorLn("Failed to keep connection to all clients", err)
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

	if len(allOps) > 0 {
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
				err = allOps.CSV(enc, commandLine(ctx))
				fatalIf(probe.NewError(err), "Unable to write benchmark output")

				infoLn(fmt.Sprintf("Benchmark data written to %q\n", fileName+".csv.zst"))
			}()
		}
	}
	monitor.OperationsReady(allOps, fileName, commandLine(ctx))
	printAnalysis(ctx, allOps)

	err = conns.startStageAll(stageCleanup, time.Now(), false)
	if err != nil {
		errorLn("Failed to clean up all clients", err)
	}
	err = conns.waitForStage(stageCleanup, false, common)
	if err != nil {
		errorLn("Failed to keep connection to all clients", err)
	}
	infoLn("Cleanup done.\n")

	return true, nil
}

// connections keeps track of connections to clients.
type connections struct {
	info  func(data ...interface{})
	errLn func(data ...interface{})
	hosts []string
	ws    []*websocket.Conn
	si    serverInfo
}

// newConnections creates connections (but does not connect) to clients.
func newConnections(hosts []string) *connections {
	var c connections
	c.si = serverInfo{
		ID:      pRandASCII(20),
		Secret:  "",
		Version: warpServerVersion,
	}
	c.hosts = hosts
	c.ws = make([]*websocket.Conn, len(hosts))
	return &c
}

func (c *connections) errorF(format string, data ...interface{}) {
	c.errLn(fmt.Sprintf(format, data...))
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
	return c.hosts[i]
}

// hostName returns the remote host name of a connection.
func (c *connections) disconnect(i int) {
	if c.ws[i] != nil {
		c.info("Disconnecting client: ", c.hostName(i))
		c.ws[i].WriteJSON(serverRequest{Operation: serverReqDisconnect})
		c.ws[i].Close()
		c.ws[i] = nil
	}
}

// roundTrip performs a roundtrip.
func (c *connections) roundTrip(i int, req serverRequest) (*clientReply, error) {
	conn := c.ws[i]
	if conn == nil {
		err := c.connect(i)
		if err != nil {
			return nil, err
		}
	}
	for {
		req.ClientIdx = i
		conn := c.ws[i]
		err := conn.WriteJSON(req)
		if err != nil {
			c.errLn(err)
			if err := c.connect(i); err == nil {
				continue
			}
			return nil, err
		}
		var resp clientReply
		err = conn.ReadJSON(&resp)
		if err != nil {
			c.errLn(err)
			if err := c.connect(i); err == nil {
				continue
			}
			return nil, err
		}
		return &resp, nil
	}
}

// connect to a client.
func (c *connections) connect(i int) error {
	tries := 0
	for {
		err := func() error {
			host := c.hosts[i]
			if !strings.Contains(host, ":") {
				host += ":" + strconv.Itoa(warpServerDefaultPort)
			}
			u := url.URL{Scheme: "ws", Host: host, Path: "/ws"}
			c.info("Connecting to ", u.String())
			var err error
			c.ws[i], _, err = websocket.DefaultDialer.Dial(u.String(), nil)
			if err != nil {
				return err
			}
			sent := time.Now()

			// Send server info
			err = c.ws[i].WriteJSON(c.si)
			if err != nil {
				return err
			}
			var resp clientReply
			err = c.ws[i].ReadJSON(&resp)
			if err != nil {
				return err
			}
			if resp.Err != "" {
				return errors.New(resp.Err)
			}

			roundtrip := time.Since(sent)
			// Add 50% of the roundtrip.
			delta := time.Since(resp.Time.Add(roundtrip / 2))
			if delta < 0 {
				delta = -delta
			}
			if delta > time.Second {
				return fmt.Errorf("host %v time delta too big (%v). Roundtrip took %v. Synchronize clock on client and retry", host, delta.Round(time.Millisecond), roundtrip.Round(time.Millisecond))
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
		c.errorF("Connection failed:%v, retrying...\n", err)
		tries++
		time.Sleep(time.Second)
	}
}

// startStage will start a stage at a specific time on a client.
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
		c.errorF("Client %v returned error: %v\n", c.hostName(i), resp.Err)
		return errors.New(resp.Err)
	}
	c.info("Client ", c.hostName(i), ": Requested stage ", stage, " start...")
	return nil
}

// startStageAll will start a stage at a specific time on all connected clients.
func (c *connections) startStageAll(stage benchmarkStage, startAt time.Time, failOnErr bool) error {
	var wg sync.WaitGroup
	var gerr error
	var mu sync.Mutex
	c.info("Requesting stage ", stage, " start...")

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
				c.errLn("Starting stage error:", err)
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

// downloadOps will download operations from all connected clients.
// If an error is encountered the result will be ignored.
func (c *connections) downloadOps() []bench.Operations {
	var wg sync.WaitGroup
	var mu sync.Mutex
	c.info("Downloading operations...")
	res := make([]bench.Operations, 0, len(c.ws))
	for i, conn := range c.ws {
		if conn == nil {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := c.roundTrip(i, serverRequest{Operation: serverReqSendOps})
			if err != nil {
				c.errorF("Client %v download returned error: %v\n", c.hostName(i), resp.Err)
				return
			}
			if resp.Err != "" {
				c.errorF("Client %v returned error: %v\n", c.hostName(i), resp.Err)
				return
			}
			c.info("Client ", c.hostName(i), ": Operations downloaded.")

			mu.Lock()
			res = append(res, resp.Ops)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	return res
}

// waitForStage will wait for stage completion on all clients.
func (c *connections) waitForStage(stage benchmarkStage, failOnErr bool, common *bench.Common) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
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
					c.errLn(err)
					return
				}
				if resp.Err != "" {
					c.disconnect(i)
					if failOnErr {
						fatalIf(probe.NewError(errors.New(resp.Err)), "Stage failed. Client %v returned error.", c.hostName(i))
					}
					c.errorF("Client %v returned error: %v\n", c.hostName(i), resp.Err)
					return
				}
				if resp.StageInfo.Finished {
					// Merge custom
					if len(resp.StageInfo.Custom) > 0 {
						mu.Lock()
						if common.Custom == nil {
							common.Custom = make(map[string]string, len(resp.StageInfo.Custom))
						}
						for k, v := range resp.StageInfo.Custom {
							common.Custom[k] = v
						}
						mu.Unlock()
					}
					c.info("Client ", c.hostName(i), ": Finished stage ", stage, "...")
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
			return fmt.Sprint(ctx.Bool(flag.GetName())), nil
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
