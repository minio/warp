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
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/mc/pkg/probe"
)

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

type serverRequest struct {
	Operation serverRequestOp `json:"op"`
	Benchmark struct {
		Command string            `json:"command"`
		Args    cli.Args          `json:"args"`
		Flags   map[string]string `json:"flags"`
	}
	Stage benchmarkStage `json:"stage"`
	Time  time.Time      `json:"time"`
}

func runServerBenchmark(ctx *cli.Context) error {
	if ctx.String("warp-client") == "" {
		return nil
	}

	si := serverInfo{
		ID:      pRandAscii(20),
		Secret:  "",
		Version: warpServerVersion,
	}

	hosts := parseHosts(ctx.String("warp-client"))
	ws := make([]*websocket.Conn, len(hosts))
	defer func() {
		for _, c := range ws {
			if c != nil {
				c.Close()
			}
		}
	}()

	// Serialize parameters
	excludeFlags := map[string]struct{}{"warp-client": {}, "serverprof": {}, "autocompletion": {}, "help": {}}
	bench := serverRequest{
		Operation: serverReqBenchmark,
	}
	bench.Benchmark.Command = ctx.Command.Name
	bench.Benchmark.Args = ctx.Args()
	bench.Benchmark.Flags = make(map[string]string)

	for _, flag := range ctx.Command.Flags {
		if _, ok := excludeFlags[flag.GetName()]; ok {
			continue
		}
		if ctx.IsSet(flag.GetName()) {
			var err error
			bench.Benchmark.Flags[flag.GetName()], err = flagToJson(ctx, flag)
			if err != nil {
				return err
			}
		}
	}

	connect := func(i int) error {
		tries := 0
		for {
			err := func() error {
				host := hosts[i]
				if !strings.Contains(host, ":") {
					host += ":" + strconv.Itoa(warpServerDefaultPort)
				}
				u := url.URL{Scheme: "ws", Host: host, Path: "/ws"}
				console.Infoln("Connecting to", u)
				var err error
				ws[i], _, err = websocket.DefaultDialer.Dial(u.String(), nil)
				// Send server info
				err = ws[i].WriteJSON(si)
				if err != nil {
					return err
				}
				var resp clientReply
				err = ws[i].ReadJSON(&resp)
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
			if err != nil && tries == 3 {
				ws[i] = nil
				return err
			}
			console.Println("Connection failed, retrying")
			tries++
			time.Sleep(time.Second)
		}
	}

	for i := range hosts {
		err := connect(i)
		fatalIf(probe.NewError(err), "Unable to connect to warp client")
		err = ws[i].WriteJSON(bench)
		fatalIf(probe.NewError(err), "Unable to send benchmark info to warp client")
		resp := clientReply{}
		err = ws[i].ReadJSON(&resp)
		fatalIf(probe.NewError(err), "Did not receive response from warp client")
		if resp.Err != "" {
			fatalIf(probe.NewError(errors.New(resp.Err)), "Error received from warp client")
		}
		console.Infof("Client %v connected...", ws[i].RemoteAddr())
		// Assume ok.
	}
	console.Println("All clients connected...")

	// All clients connected.
	startStage := func(t time.Time, i int, stage benchmarkStage) error {
		req := serverRequest{
			Operation: serverReqStartStage,
			Stage:     stage,
			Time:      t,
		}
	retry:
		conn := ws[i]
		err := conn.WriteJSON(req)
		if err != nil {
			console.Error(err)
			if err := connect(i); err == nil {
				goto retry
			}
			return err
		}
		var resp clientReply
		err = conn.ReadJSON(&resp)
		if err != nil {
			console.Error(err)
			if err := connect(i); err == nil {
				goto retry
			}
			return err
		}
		if resp.Err != "" {
			console.Errorf("Client %v return error: %v\n", conn.RemoteAddr(), resp.Err)
			return errors.New(resp.Err)
		}
		console.Infof("Client %v: Requested stage start..\n", conn.RemoteAddr())
		return nil
	}
	console.Println("Starting Preparation...")
	startPrep := time.Now().Add(3 * time.Second)
	var wg sync.WaitGroup
	for i, conn := range ws {
		if conn == nil {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := startStage(startPrep, i, stagePrepare)
			if err != nil {
				// TODO: Probably shouldn't hard fail.
				fatalIf(probe.NewError(err), "Prepare failed")
			}
		}(i)
	}
	wg.Wait()
	waitForStage := func(stage benchmarkStage) error {
		var wg sync.WaitGroup
		for i, conn := range ws {
			if conn == nil {
				// log?
				continue
			}
			wg.Add(1)
			go func(i int) {
				conn := ws[i]
				defer wg.Done()
				for {
					req := serverRequest{
						Operation: serverReqStageStatus,
						Stage:     stage,
						Time:      time.Now(),
					}
					err := conn.WriteJSON(req)
					if err != nil {
						console.Error(err)
						if err := connect(i); err == nil {
							continue
						}
						ws[i] = nil
						return
					}
					var resp clientReply
					err = conn.ReadJSON(&resp)
					if err != nil {
						console.Error(err)
						if err := connect(i); err == nil {
							continue
						}
						ws[i] = nil
						return
					}
					if resp.Err != "" {
						console.Errorf("Client %v returned error: %v", resp.Err)
						return
					}
					if resp.StageInfo.Finished {
						console.Infof("Client %v finished stage...", conn.RemoteAddr())
						return
					}
					time.Sleep(time.Second)
				}
			}(i)
		}
		wg.Wait()
		return nil
	}
	err := waitForStage(stagePrepare)
	if err != nil {
		fatalIf(probe.NewError(err), "Failed to prepare")
	}
	console.Println("All clients prepared...")

	// TODO: send and wait
	os.Exit(0)

	return nil
}

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
			return fmt.Sprintf(`"%v"`, ctx.Int64(flag.GetName())), nil
		}
	case cli.IntFlag:
		if ctx.IsSet(flag.GetName()) {
			return fmt.Sprintf(`"%v"`, ctx.Int(flag.GetName())), nil
		}
	case cli.DurationFlag:
		if ctx.IsSet(flag.GetName()) {
			return ctx.Duration(flag.GetName()).String(), nil
		}
	case cli.UintFlag:
		if ctx.IsSet(flag.GetName()) {
			return fmt.Sprintf(`"%v"`, ctx.Uint(flag.GetName())), nil
		}
	case cli.Uint64Flag:
		if ctx.IsSet(flag.GetName()) {
			return fmt.Sprintf(`"%v"`, ctx.Uint64(flag.GetName())), nil
		}
	default:
		if ctx.IsSet(flag.GetName()) {
			return "", fmt.Errorf("unhandled flag type: %T", flag)
		}
	}
	return "", nil
}
