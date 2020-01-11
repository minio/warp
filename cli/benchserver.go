package cli

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/warp/pkg/bench"
)

const warpServerVersion = 1

type serverRequestOp string

const (
	serverReqDisconnect  serverRequestOp = "disconnect"
	serverReqBenchmark                   = "benchmark"
	serverReqStartStage                  = "start_stage"
	serverReqStageStatus                 = "stage_status"
	serverReqSendOps                     = "send_ops"
)

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
	if s.Version != warpServerVersion {
		return errors.New("warp server and client version mismatch")
	}
	return nil
}

// serverRequest requests an operation from the client and expects a response.
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
// Returns a bool whether clients were specified.
func runServerBenchmark(ctx *cli.Context) (bool, error) {
	if ctx.String("warp-client") == "" {
		return false, nil
	}

	conns := newConnections(parseHosts(ctx.String("warp-client")))
	if len(conns.hosts) == 0 {
		return true, errors.New("no hosts")
	}
	defer conns.closeAll()

	// Serialize parameters
	excludeFlags := map[string]struct{}{
		"warp-client":    {},
		"serverprof":     {},
		"autocompletion": {},
		"help":           {},
		"syncstart":      {},
		"analyze.out":    {},
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
			req.Benchmark.Flags[flag.GetName()], err = flagToJson(ctx, flag)
			if err != nil {
				return true, err
			}
		}
	}

	// Connect to hosts, send benchmark requests.
	for i := range conns.hosts {
		resp, err := conns.roundTrip(i, req)
		fatalIf(probe.NewError(err), "Unable to send benchmark info to warp client")
		if resp.Err != "" {
			fatalIf(probe.NewError(errors.New(resp.Err)), "Error received from warp client")
		}
		console.Infof("Client %v connected...\n", conns.hostName(i))
		// Assume ok.
	}
	console.Infoln("All clients connected...")

	_ = conns.startStageAll(stagePrepare, time.Now().Add(time.Second), true)
	err := conns.waitForStage(stagePrepare, true)
	if err != nil {
		fatalIf(probe.NewError(err), "Failed to prepare")
	}
	console.Infoln("All clients prepared...")

	const benchmarkWait = 3 * time.Second

	prof, err := startProfiling(ctx)
	if err != nil {
		return true, err
	}
	err = conns.startStageAll(stageBenchmark, time.Now().Add(benchmarkWait), false)
	if err != nil {
		console.Errorln("Failed to start all clients", err)
	}
	err = conns.waitForStage(stageBenchmark, false)
	if err != nil {
		console.Errorln("Failed to keep connection to all clients", err)
	}

	fileName := ctx.String("benchdata")
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s-%s", appName, "remote", time.Now().Format("2006-01-02[150405]"), pRandAscii(4))
	}
	prof.stop(ctx, fileName+".profiles.zip")

	console.Infoln("Done. Downloading operations...")
	downloaded := conns.downloadOps()
	var allOps bench.Operations
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
		console.Error("Unable to write benchmark data:", err)
	} else {
		func() {
			defer f.Close()
			enc, err := zstd.NewWriter(f)
			fatalIf(probe.NewError(err), "Unable to compress benchmark output")

			defer enc.Close()
			err = allOps.CSV(enc)
			fatalIf(probe.NewError(err), "Unable to write benchmark output")

			console.Infof("Benchmark data written to %q\n", fileName+".csv.zst")
		}()
	}

	err = conns.startStageAll(stageCleanup, time.Now(), false)
	if err != nil {
		console.Errorln("Failed to clean up all clients", err)
	}
	err = conns.waitForStage(stageCleanup, false)
	if err != nil {
		console.Errorln("Failed to keep connection to all clients", err)
	}

	printAnalysis(ctx, allOps)

	return true, nil
}

// connections keeps track of connections to clients.
type connections struct {
	hosts []string
	ws    []*websocket.Conn
	si    serverInfo
}

// newConnections creates connections (but does not connect) to clients.
func newConnections(hosts []string) *connections {
	var c connections
	c.si = serverInfo{
		ID:      pRandAscii(20),
		Secret:  "",
		Version: warpServerVersion,
	}
	c.hosts = hosts
	c.ws = make([]*websocket.Conn, len(hosts))
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
	return c.hosts[i]
}

// hostName returns the remote host name of a connection.
func (c *connections) disconnect(i int) {
	if c.ws[i] != nil {
		console.Infoln("Disconnecting client", c.hostName(i))
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
		conn := c.ws[i]
		err := conn.WriteJSON(req)
		if err != nil {
			console.Error(err)
			if err := c.connect(i); err == nil {
				continue
			}
			return nil, err
		}
		var resp clientReply
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
			console.Infoln("Connecting to", u.String())
			var err error
			c.ws[i], _, err = websocket.DefaultDialer.Dial(u.String(), nil)
			if err != nil {
				return err
			}
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
			delta := time.Now().Sub(resp.Time)
			if delta < 0 {
				delta = -delta
			}
			if delta > time.Second {
				return fmt.Errorf("host %v time delta too big (%v). Synchronize clock on client and retry", host, delta)
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
		console.Errorf("Connection failed:%v, retrying...\n", err)
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
		console.Errorf("Client %v returned error: %v\n", c.hostName(i), resp.Err)
		return errors.New(resp.Err)
	}
	console.Infof("Client %v: Requested stage %v start..\n", c.hostName(i), stage)
	return nil
}

// startStageAll will start a stage at a specific time on all connected clients.
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

// downloadOps will download operations from all connected clients.
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
					console.Errorf("Client %v returned error: %v\n", c.hostName(i), resp.Err)
					return
				}
				console.Infof("Client %v: Operations downloaded.\n", c.hostName(i))

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

// waitForStage will wait for stage completion on all clients.
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
						fatalIf(probe.NewError(errors.New(resp.Err)), "Stage failed. Client %v returned error.", c.hostName(i))
					}
					console.Errorf("Client %v returned error: %v\n", c.hostName(i), resp.Err)
					return
				}
				if resp.StageInfo.Finished {
					console.Infof("Client %v: Finished stage %v...\n", c.hostName(i), stage)
					return
				}
				time.Sleep(time.Second)
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// flagToJson converts a flag to a representation that can be reversed into the flag.
func flagToJson(ctx *cli.Context, flag cli.Flag) (string, error) {
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
	default:
		if ctx.IsSet(flag.GetName()) {
			return "", fmt.Errorf("unhandled flag type: %T", flag)
		}
	}
	return "", nil
}
