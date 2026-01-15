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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/warp/api"
	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/wui"
	"github.com/minio/websocket"
)

const warpServerVersion = 1

type serverRequestOp string

const (
	serverReqDisconnect  serverRequestOp = "disconnect"
	serverReqBenchmark   serverRequestOp = "benchmark"
	serverReqStartStage  serverRequestOp = "start_stage"
	serverReqStageStatus serverRequestOp = "stage_status"
	serverReqAbortStage  serverRequestOp = "stage_abort"
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

type StatsSummary interface {
	StatsSummary() map[string]string
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
	StartTime    time.Time            `json:"start_time"`
	Operation    serverRequestOp      `json:"op"`
	Stage        benchmarkStage       `json:"stage"`
	ClientIdx    int                  `json:"client_idx"`
	TotalClients int                  `json:"total_clients"`
	Aggregate    bool                 `json:"aggregate"`
	UpdateReq    *aggregate.UpdateReq `json:"update_req,omitempty"`
}

// runServerBenchmark will run a benchmark server if requested.
// Returns a bool whether clients were specified.
func runServerBenchmark(ctx *cli.Context, b bench.Benchmark) (bool, error) {
	if ctx.String("warp-client") == "" {
		return false, nil
	}

	if ctx.Bool("autoterm") && ctx.Bool("full") {
		return true, errors.New("use of -autoterm cannot be used with --full on remote benchmarks")
	}

	var ui ui
	if !globalQuiet && !globalJSON {
		go ui.Run()
	}

	conns := newConnections(parseHosts(ctx.String("warp-client"), false))
	if len(conns.hosts) == 0 {
		return true, errors.New("no hosts")
	}
	infoLn := func(data ...any) {
		ui.SetSubText(strings.TrimRight(fmt.Sprintln(data...), "\r\n."))
	}

	conns.info = infoLn
	conns.errLn = printError
	defer conns.closeAll()
	monitor := api.NewBenchmarkMonitor(ctx.String(serverFlagName), nil)
	monitor.SetLnLoggers(infoLn, printError)
	defer monitor.Done()
	errorLn := printError

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
				return flagToJSON(ctx, flag, "host")
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
		name := strings.Split(flag.GetName(), ",")[0]
		if _, ok := excludeFlags[name]; ok {
			continue
		}
		if ctx.IsSet(name) {
			var err error
			if t := transformFlags[name]; t != nil {
				req.Benchmark.Flags[name], err = t(flag)
			} else {
				req.Benchmark.Flags[name], err = flagToJSON(ctx, flag, name)
			}
			if err != nil {
				return true, err
			}
		}
	}
	maps.Copy(req.Benchmark.Flags, b.GetCommon().ExtraFlags)

	// Connect to hosts, send benchmark requests.
	ui.StartPrepare("Preparing", nil, nil)
	for i := range conns.hosts {
		resp, err := conns.roundTrip(i, req)
		fatalIf(probe.NewError(err), "Unable to send benchmark info to warp client")
		if resp.Err != "" {
			fatalIf(probe.NewError(errors.New(resp.Err)), "Error received from warp client")
		}
		infoLn("Client ", conns.hostName(i), " connected...")
		// Assume ok.
	}
	infoLn("All clients connected. Preparing benchmark...")

	common := b.GetCommon()
	benchCtx, cancel := context.WithCancel(context.Background())
	ui.cancelFn.Store(&cancel)
	defer cancel()

	_ = conns.startStageAll(stagePrepare, time.Now().Add(time.Second), true)
	err := conns.waitForStage(benchCtx, stagePrepare, true, common, nil)
	if benchCtx.Err() != nil {
		return true, benchCtx.Err()
	}
	if err != nil {
		fatalIf(probe.NewError(err), "Failed to prepare")
	}
	if ap, ok := b.(AfterPreparer); ok {
		err := ap.AfterPrepare(context.Background())
		fatalIf(probe.NewError(err), "Error preparing server")
	}

	infoLn("All clients prepared...")

	const benchmarkWait = 3 * time.Second
	var updates chan aggregate.UpdateReq
	srv := wui.New(nil)
	showAddress := ""
	if !ctx.Bool("full") {
		updates = make(chan aggregate.UpdateReq, 10)
		monitor.SetUpdate(updates)
		if ctx.Bool("web") {
			addr, err := srv.Start()
			srv.WithPoll(updates)
			fatalIf(probe.NewError(err), "Failed to start web server")
			showAddress = "Web UI: " + addr
			monitor.InfoLn("Web UI available at:", addr)
			if err := srv.OpenBrowser(); err != nil {
				monitor.Errorln("Could not open browser automatically. Please visit:", addr)
			}
		}
	}
	prof, err := startProfiling(context.Background(), ctx)
	if err != nil {
		return true, err
	}
	tStart := time.Now().Add(benchmarkWait)
	benchDur := ctx.Duration("duration")
	err = conns.startStageAll(stageBenchmark, time.Now().Add(benchmarkWait), false)
	if err != nil {
		errorLn("Failed to start all clients", err)
	}
	ui.StartBenchmark("Benchmarking", tStart, tStart.Add(benchDur), updates)
	ui.SetSubText("Press 'q' to stop benchmark. " + showAddress)

	if ctx.Bool("autoterm") {
		if ctx.Bool("full") {
			return true, errors.New("use of -autoterm cannot be combined with -full on remote benchmarks")
		}
		common.AutoTermDur = ctx.Duration("autoterm.dur")
		common.AutoTermScale = ctx.Float64("autoterm.pct") / 100
		if common.AutoTermDur > 0 {
			benchCtx = aggregate.AutoTerm(benchCtx, "", common.AutoTermScale, int(common.AutoTermDur.Seconds()+0.999), common.AutoTermDur, updates)
		}
	}

	err = conns.waitForStage(benchCtx, stageBenchmark, false, common, updates)
	if err != nil {
		errorLn("Failed to keep connection to all clients", err)
	}

	fileName := ctx.String("benchdata")
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s-%s", appName, "remote", time.Now().Format("2006-01-02[150405]"), pRandASCII(4))
	}
	prof.stop(context.Background(), ctx, fileName+".profiles.zip")

	ui.SetPhase("Downloading Operations")
	if updates == nil {
		downloaded := conns.downloadOps()
		switch len(downloaded) {
		case 0:
		case 1:
			allOps = downloaded[0]
		default:
			threads := uint32(0)
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
		ui.Update(tea.Quit())
		ui.Wait()
		printAnalysis(ctx, os.Stdout, allOps)
	} else {
		final := conns.downloadAggr()
		if final.Total.TotalRequests == 0 {
			return true, errors.New("no operations received")
		}
		final.Commandline = commandLine(ctx)
		final.WarpVersion = GlobalVersion
		final.WarpDate = GlobalDate
		final.WarpCommit = GlobalCommit
		f, err := os.Create(fileName + ".json.zst")
		if err != nil {
			monitor.Errorln("Unable to write benchmark data:", err)
		} else {
			func() {
				defer f.Close()
				enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
				fatalIf(probe.NewError(err), "Unable to compress benchmark output")
				defer enc.Close()
				js := json.NewEncoder(enc)
				js.SetIndent("", "  ")
				err = js.Encode(final)
				fatalIf(probe.NewError(err), "Unable to write benchmark output")

				monitor.InfoLn(fmt.Sprintf("Benchmark data written to %q\n", fileName+".json.zst"))
			}()
		}
		monitor.UpdateAggregate(&final, fileName)
		var rep *bytes.Buffer
		if globalJSON {
			rep = &bytes.Buffer{}
			enc := json.NewEncoder(rep)
			enc.SetIndent("", "  ")
			_ = enc.Encode(final)
		} else {
			rep = final.Report(aggregate.ReportOptions{
				Details: ctx.Bool("analyze.v"),
				Color:   !globalNoColor,
				OnlyOps: getAnalyzeOPS(ctx),
			})
		}
		ui.Update(tea.Quit())
		ui.Wait()
		fmt.Println("")
		fmt.Println(rep)
	}

	if !ctx.Bool("keep-data") && !ctx.Bool("noclear") {
		ui.SetPhase("Cleanup")
		monitor.InfoLn("Starting cleanup...")
		b.Cleanup(context.Background())

		err = conns.startStageAll(stageCleanup, time.Now(), false)
		if err != nil {
			errorLn("Failed to clean up all clients", err)
		}
		err = conns.waitForStage(context.Background(), stageCleanup, false, common, nil)
		if err != nil {
			errorLn("Failed to keep connection to all clients", err)
		}
		infoLn("Cleanup done.\n")
	}

	if ctx.Bool("web") && updates != nil {
		monitor.InfoLn("Press Enter to exit..." + showAddress)
		srv.WaitForKeypress()
		srv.Shutdown()
	}

	return true, nil
}

// connections keeps track of connections to clients.
type connections struct {
	info  func(data ...any)
	errLn func(data ...any)
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

func (c *connections) errorF(format string, data ...any) {
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
		req.TotalClients = len(c.hosts)
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
	c.info("Client ", c.hostName(i), ": Requested stage", stage, "start...")
	return nil
}

// startStageAll will start a stage at a specific time on all connected clients.
func (c *connections) startStageAll(stage benchmarkStage, startAt time.Time, failOnErr bool) error {
	var wg sync.WaitGroup
	var gerr error
	var mu sync.Mutex
	c.info("Requesting stage", stage, "start...")

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

// downloadOps will download operations from all connected clients.
// If an error is encountered the result will be ignored.
func (c *connections) downloadAggr() aggregate.Realtime {
	var wg sync.WaitGroup
	var mu sync.Mutex
	c.info("Downloading operations...")
	var res aggregate.Realtime
	for i, conn := range c.ws {
		if conn == nil {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := c.roundTrip(i, serverRequest{Operation: serverReqSendOps, Aggregate: true, UpdateReq: &aggregate.UpdateReq{Final: true}})
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
			res.Merge(resp.Update)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	return res
}

// waitForStage will wait for stage completion on all clients.
func (c *connections) waitForStage(ctx context.Context, stage benchmarkStage, failOnErr bool, common *bench.Common, updates <-chan aggregate.UpdateReq) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var lastUpdate []atomic.Pointer[aggregate.Realtime]
	var done chan struct{}
	if updates != nil {
		lastUpdate = make([]atomic.Pointer[aggregate.Realtime], len(c.ws))
		done = make(chan struct{})
		defer close(done)
		go func() {
			for {
				select {
				case <-done:
					return
				case r := <-updates:
					var m aggregate.Realtime
					for i := range lastUpdate {
						if u := lastUpdate[i].Load(); u != nil {
							m.Merge(u)
						}
					}
					r.C <- &m
				}
			}
		}()
	}
	for i, conn := range c.ws {
		if conn == nil {
			// log?
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			didCancel := false
			for {
				if !didCancel && ctx != nil && ctx.Err() != nil {
					c.info("Client ", c.hostName(i), ": Sending cancellation to stage", stage, "...")
					req := serverRequest{
						Operation: serverReqAbortStage,
						Stage:     stage,
					}
					_, err := c.roundTrip(i, req)
					if err != nil {
						c.disconnect(i)
						if failOnErr {
							fatalIf(probe.NewError(err), "Stage failed.")
						}
						c.errLn(err)
						return
					}
					didCancel = true
					continue
				}
				req := serverRequest{
					Operation: serverReqStageStatus,
					Stage:     stage,
					Aggregate: updates != nil,
				}
				if updates != nil {
					req.UpdateReq = &aggregate.UpdateReq{}
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
				if updates != nil {
					lastUpdate[i].Store(resp.Update)
				}
				if resp.StageInfo.Finished {
					// Merge custom
					if len(resp.StageInfo.Custom) > 0 {
						mu.Lock()
						if common.Custom == nil {
							common.Custom = make(map[string]string, len(resp.StageInfo.Custom))
						}
						maps.Copy(common.Custom, resp.StageInfo.Custom)
						mu.Unlock()
					}
					c.info("Client", c.hostName(i), ": Finished stage", stage, "...")
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
func flagToJSON(ctx *cli.Context, flag cli.Flag, name string) (string, error) {
	switch flag.(type) {
	case cli.StringFlag:
		if ctx.IsSet(name) {
			return ctx.String(name), nil
		}
	case cli.BoolFlag:
		if ctx.IsSet(name) {
			return fmt.Sprint(ctx.Bool(name)), nil
		}
	case cli.Int64Flag:
		if ctx.IsSet(name) {
			return fmt.Sprint(ctx.Int64(name)), nil
		}
	case cli.IntFlag:
		if ctx.IsSet(name) {
			return fmt.Sprint(ctx.Int(name)), nil
		}
	case cli.DurationFlag:
		if ctx.IsSet(name) {
			return ctx.Duration(name).String(), nil
		}
	case cli.UintFlag:
		if ctx.IsSet(name) {
			return fmt.Sprint(ctx.Uint(name)), nil
		}
	case cli.Uint64Flag:
		if ctx.IsSet(name) {
			return fmt.Sprint(ctx.Uint64(name)), nil
		}
	case cli.Float64Flag:
		if ctx.IsSet(name) {
			return fmt.Sprint(ctx.Float64(name)), nil
		}
	default:
		if ctx.IsSet(name) {
			return "", fmt.Errorf("unhandled flag type: %T", flag)
		}
	}
	return "", nil
}
