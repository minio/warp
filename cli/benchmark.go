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
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/madmin-go/v2"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/console"
	"github.com/minio/warp/api"
	"github.com/minio/warp/pkg/bench"
)

var benchFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "benchdata",
		Value: "",
		Usage: "Output benchmark+profile data to this file. By default unique filename is generated.",
	},
	cli.StringFlag{
		Name:  "serverprof",
		Usage: "Run MinIO server profiling during benchmark; possible values are 'cpu', 'mem', 'block', 'mutex' and 'trace'.",
		Value: "",
	},
	cli.DurationFlag{
		Name:  "duration",
		Usage: "Duration to run the benchmark. Use 's' and 'm' to specify seconds and minutes.",
		Value: 5 * time.Minute,
	},
	cli.BoolFlag{
		Name:  "autoterm",
		Usage: "Auto terminate when benchmark is considered stable.",
	},
	cli.DurationFlag{
		Name:  "autoterm.dur",
		Usage: "Minimum duration where output must have been stable to allow automatic termination.",
		Value: 10 * time.Second,
	},
	cli.Float64Flag{
		Name:  "autoterm.pct",
		Usage: "The percentage the last 6/25 time blocks must be within current speed to auto terminate.",
		Value: 7.5,
	},
	cli.BoolFlag{
		Name:  "noclear",
		Usage: "Do not clear bucket before or after running benchmarks. Use when running multiple clients.",
	},
	cli.BoolFlag{
		Name:   "keep-data",
		Usage:  "Leave benchmark data. Do not run cleanup after benchmark. Bucket will still be cleaned prior to benchmark",
		Hidden: true,
	},
	cli.StringFlag{
		Name:  "syncstart",
		Usage: "Specify a benchmark start time. Time format is 'hh:mm' where hours are specified in 24h format, server TZ.",
		Value: "",
	},
	cli.StringFlag{
		Name:   "warp-client",
		Usage:  "Connect to warp clients and run benchmarks there.",
		EnvVar: "",
		Value:  "",
	},
}

// runBench will run the supplied benchmark and save/print the analysis.
func runBench(ctx *cli.Context, b bench.Benchmark) error {
	activeBenchmarkMu.Lock()
	ab := activeBenchmark
	activeBenchmarkMu.Unlock()
	b.GetCommon().Error = printError
	if ab != nil {
		b.GetCommon().ClientIdx = ab.clientIdx
		return runClientBenchmark(ctx, b, ab)
	}
	if done, err := runServerBenchmark(ctx, b); done || err != nil {
		fatalIf(probe.NewError(err), "Error running remote benchmark")
		return nil
	}

	monitor := api.NewBenchmarkMonitor(ctx.String(serverFlagName))
	monitor.SetLnLoggers(printInfo, printError)
	defer monitor.Done()

	monitor.InfoLn("Preparing server.")
	pgDone := make(chan struct{})
	c := b.GetCommon()
	c.Clear = !ctx.Bool("noclear")
	if ctx.Bool("autoterm") {
		// TODO: autoterm cannot be used when in client/server mode
		c.AutoTermDur = ctx.Duration("autoterm.dur")
		c.AutoTermScale = ctx.Float64("autoterm.pct") / 100
	}
	if !globalQuiet && !globalJSON {
		c.PrepareProgress = make(chan float64, 1)
		const pgScale = 10000
		pg := newProgressBar(pgScale, pb.U_NO)
		pg.ShowCounters = false
		pg.ShowElapsedTime = false
		pg.ShowSpeed = false
		pg.ShowTimeLeft = false
		pg.ShowFinalTime = true
		go func() {
			defer close(pgDone)
			defer pg.Finish()
			tick := time.NewTicker(time.Millisecond * 125)
			defer tick.Stop()
			pg.Set(-1)
			pg.SetCaption("Preparing: ")
			newVal := int64(-1)
			for {
				select {
				case <-tick.C:
					current := pg.Get()
					if current != newVal {
						pg.Set64(newVal)
						pg.Update()
					}
					monitor.InfoQuietln(fmt.Sprintf("Preparation: %0.0f%% done...", float64(newVal)/float64(100)))
				case pct, ok := <-c.PrepareProgress:
					if !ok {
						pg.Set64(pgScale)
						if newVal > 0 {
							pg.Update()
						}
						return
					}
					newVal = int64(pct * pgScale)
				}
			}
		}()
	} else {
		close(pgDone)
	}

	err := b.Prepare(context.Background())
	fatalIf(probe.NewError(err), "Error preparing server")
	if c.PrepareProgress != nil {
		close(c.PrepareProgress)
		<-pgDone
	}

	if ap, ok := b.(AfterPreparer); ok {
		err := ap.AfterPrepare(context.Background())
		fatalIf(probe.NewError(err), "Error preparing server")
	}

	// Start after waiting a second or until we reached the start time.
	tStart := time.Now().Add(time.Second * 3)
	if st := ctx.String("syncstart"); st != "" {
		startTime := parseLocalTime(st)
		now := time.Now()
		if startTime.Before(now) {
			monitor.Errorln("Did not manage to prepare before syncstart")
			tStart = time.Now()
		} else {
			tStart = startTime
		}
	}

	benchDur := ctx.Duration("duration")
	ctx2, cancel := context.WithDeadline(context.Background(), tStart.Add(benchDur))
	defer cancel()
	start := make(chan struct{})
	go func() {
		<-time.After(time.Until(tStart))
		monitor.InfoLn("Benchmark starting...")
		close(start)
	}()

	fileName := ctx.String("benchdata")
	cID := pRandASCII(4)
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s-%s", appName, ctx.Command.Name, time.Now().Format("2006-01-02[150405]"), cID)
	}

	prof, err := startProfiling(ctx2, ctx)
	fatalIf(probe.NewError(err), "Unable to start profile.")
	monitor.InfoLn("Starting benchmark in ", time.Until(tStart).Round(time.Second), "...")
	pgDone = make(chan struct{})
	if !globalQuiet && !globalJSON {
		pg := newProgressBar(int64(benchDur), pb.U_DURATION)
		go func() {
			defer close(pgDone)
			defer pg.Finish()
			pg.SetCaption("Benchmarking:")
			tick := time.NewTicker(time.Millisecond * 125)
			defer tick.Stop()
			done := ctx2.Done()
			for {
				select {
				case t := <-tick.C:
					elapsed := t.Sub(tStart)
					if elapsed < 0 {
						continue
					}
					pg.Set64(int64(elapsed))
					pg.Update()
					monitor.InfoQuietln(fmt.Sprintf("Running benchmark: %0.0f%%...", 100*float64(elapsed)/float64(benchDur)))
				case <-done:
					pg.Set64(int64(benchDur))
					pg.Update()
					return
				}
			}
		}()
	} else {
		close(pgDone)
	}
	ops, _ := b.Start(ctx2, start)
	cancel()
	<-pgDone

	// Previous context is canceled, create a new...
	monitor.InfoLn("Saving benchmark data...")
	ctx2 = context.Background()
	ops.SortByStartTime()
	ops.SetClientID(cID)
	prof.stop(ctx2, ctx, fileName+".profiles.zip")

	f, err := os.Create(fileName + ".csv.zst")
	if err != nil {
		monitor.Errorln("Unable to write benchmark data:", err)
	} else {
		func() {
			defer f.Close()
			enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
			fatalIf(probe.NewError(err), "Unable to compress benchmark output")

			defer enc.Close()
			err = ops.CSV(enc, commandLine(ctx))
			fatalIf(probe.NewError(err), "Unable to write benchmark output")

			monitor.InfoLn(fmt.Sprintf("Benchmark data written to %q\n", fileName+".csv.zst"))
		}()
	}
	monitor.OperationsReady(ops, fileName, commandLine(ctx))
	printAnalysis(ctx, ops)
	if !ctx.Bool("keep-data") && !ctx.Bool("noclear") {
		monitor.InfoLn("Starting cleanup...")
		b.Cleanup(context.Background())
	}
	monitor.InfoLn("Cleanup Done.")
	return nil
}

var (
	activeBenchmarkMu sync.Mutex
	activeBenchmark   *clientBenchmark
)

type clientBenchmark struct {
	sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	results   bench.Operations
	err       error
	stage     benchmarkStage
	info      map[benchmarkStage]stageInfo
	clientIdx int
}

type stageInfo struct {
	startRequested bool
	start          chan struct{}
	done           chan struct{}
	custom         map[string]string
}

func (c *clientBenchmark) init(ctx context.Context) {
	c.results = nil
	c.err = nil
	c.stage = stageNotStarted
	c.info = make(map[benchmarkStage]stageInfo, len(benchmarkStages))
	c.ctx, c.cancel = context.WithCancel(ctx)
	for _, stage := range benchmarkStages {
		c.info[stage] = stageInfo{
			start: make(chan struct{}),
			done:  make(chan struct{}),
		}
	}
}

// waitForStage waits for the stage to be ready and updates the stage when it is
func (c *clientBenchmark) waitForStage(s benchmarkStage) error {
	c.Lock()
	info, ok := c.info[s]
	ctx := c.ctx
	c.Unlock()
	if !ok {
		return errors.New("waitForStage: unknown stage")
	}
	select {
	case <-info.start:
		c.setStage(s)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// waitForStage waits for the stage to be ready and updates the stage when it is
func (c *clientBenchmark) stageDone(s benchmarkStage, err error, custom map[string]string) {
	console.Infoln(s, "done...")
	if err != nil {
		console.Errorln(err.Error())
	}
	c.Lock()
	info := c.info[s]
	info.custom = custom
	if err != nil && c.err == nil {
		c.err = err
	}
	if info.done != nil {
		close(info.done)
	}
	c.info[s] = info
	c.Unlock()
}

func (c *clientBenchmark) setStage(s benchmarkStage) {
	c.Lock()
	c.stage = s
	c.Unlock()
}

type benchmarkStage string

const (
	stagePrepare    benchmarkStage = "prepare"
	stageBenchmark  benchmarkStage = "benchmark"
	stageCleanup    benchmarkStage = "cleanup"
	stageDone       benchmarkStage = "done"
	stageNotStarted benchmarkStage = ""
)

var benchmarkStages = []benchmarkStage{
	stagePrepare, stageBenchmark, stageCleanup,
}

func runClientBenchmark(ctx *cli.Context, b bench.Benchmark, cb *clientBenchmark) error {
	err := cb.waitForStage(stagePrepare)
	if err != nil {
		return err
	}
	common := b.GetCommon()
	cb.Lock()
	start := cb.info[stageBenchmark].start
	ctx2, cancel := context.WithCancel(cb.ctx)
	defer cancel()
	cb.Unlock()
	err = b.Prepare(ctx2)

	cb.stageDone(stagePrepare, err, common.Custom)
	if err != nil {
		return err
	}

	// Start after waiting a second or until we reached the start time.
	benchDur := ctx.Duration("duration")
	go func() {
		console.Infoln("Waiting")
		// Wait for start signal
		select {
		case <-ctx2.Done():
			console.Infoln("Aborted")
			return
		case <-start:
		}
		console.Infoln("Starting")
		// Finish after duration
		select {
		case <-ctx2.Done():
			console.Infoln("Aborted")
			return
		case <-time.After(benchDur):
		}
		console.Infoln("Stopping")
		// Stop the benchmark
		cancel()
	}()

	fileName := ctx.String("benchdata")
	cID := pRandASCII(6)
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s-%s", appName, ctx.Command.Name, time.Now().Format("2006-01-02[150405]"), cID)
	}

	ops, err := b.Start(ctx2, start)
	cb.Lock()
	cb.results = ops
	cb.Unlock()
	cb.stageDone(stageBenchmark, err, common.Custom)
	if err != nil {
		return err
	}
	ops.SetClientID(cID)
	ops.SortByStartTime()

	f, err := os.Create(fileName + ".csv.zst")
	if err != nil {
		console.Error("Unable to write benchmark data:", err)
	} else {
		func() {
			defer f.Close()
			enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
			fatalIf(probe.NewError(err), "Unable to compress benchmark output")

			defer enc.Close()
			err = ops.CSV(enc, commandLine(ctx))
			fatalIf(probe.NewError(err), "Unable to write benchmark output")

			console.Infof("Benchmark data written to %q\n", fileName+".csv.zst")
		}()
	}

	err = cb.waitForStage(stageCleanup)
	if err != nil {
		return err
	}
	if !ctx.Bool("keep-data") && !ctx.Bool("noclear") {
		console.Infoln("Starting cleanup...")
		b.Cleanup(context.Background())
	}
	cb.stageDone(stageCleanup, nil, common.Custom)

	return nil
}

type runningProfiles struct {
	client *madmin.AdminClient
}

func startProfiling(ctx2 context.Context, ctx *cli.Context) (*runningProfiles, error) {
	prof := ctx.String("serverprof")
	if len(prof) == 0 {
		return nil, nil
	}
	var r runningProfiles
	r.client = newAdminClient(ctx)

	// Start profile
	_, cmdErr := r.client.StartProfiling(ctx2, madmin.ProfilerType(prof))
	if cmdErr != nil {
		return nil, cmdErr
	}
	console.Infoln("Server profiling successfully started.")
	return &r, nil
}

func (rp *runningProfiles) stop(ctx2 context.Context, ctx *cli.Context, fileName string) {
	if rp == nil || rp.client == nil {
		return
	}

	// Ask for profile data, which will come compressed with zip format
	zippedData, adminErr := rp.client.DownloadProfilingData(ctx2)
	fatalIf(probe.NewError(adminErr), "Unable to download profile data.")
	defer zippedData.Close()

	f, err := os.Create(fileName)
	if err != nil {
		console.Error("Unable to write profile data:", err)
		return
	}
	defer f.Close()

	// Copy zip content to target download file
	_, err = io.Copy(f, zippedData)
	if err != nil {
		console.Error("Unable to download profile data:", err)
		return
	}

	console.Infof("Profile data successfully downloaded as %s\n", fileName)
}

func checkBenchmark(ctx *cli.Context) {
	profilerTypes := []madmin.ProfilerType{
		madmin.ProfilerCPU,
		madmin.ProfilerMEM,
		madmin.ProfilerBlock,
		madmin.ProfilerMutex,
		madmin.ProfilerTrace,
	}

	profs := strings.Split(ctx.String("serverprof"), ",")
	for _, profilerType := range profs {
		if len(profilerType) == 0 {
			continue
		}
		// Check if the provided profiler type is known and supported
		supportedProfiler := false
		for _, profiler := range profilerTypes {
			if profilerType == string(profiler) {
				supportedProfiler = true
				break
			}
		}
		if !supportedProfiler {
			fatalIf(errDummy(), "Profiler type %s unrecognized. Possible values are: %v.", profilerType, profilerTypes)
		}
	}
	if st := ctx.String("syncstart"); st != "" {
		t := parseLocalTime(st)
		if t.Before(time.Now()) {
			fatalIf(errDummy(), "syncstart is in the past: %v", t)
		}
	}
	if ctx.Bool("autoterm") {
		// TODO: autoterm cannot be used when in client/server mode
		if ctx.Duration("autoterm.dur") <= 0 {
			fatalIf(errDummy(), "autoterm.dur cannot be zero or negative")
		}
		if ctx.Float64("autoterm.pct") <= 0 {
			fatalIf(errDummy(), "autoterm.pct cannot be zero or negative")
		}
	}
}

// time format for start time.
const timeLayout = "15:04"

func parseLocalTime(s string) time.Time {
	t, err := time.ParseInLocation(timeLayout, s, time.Local)
	fatalIf(probe.NewError(err), "Unable to parse time: %s", s)
	now := time.Now()
	y, m, d := now.Date()
	t = t.AddDate(y, int(m)-1, d-1)
	return t
}

// pRandASCII return pseudorandom ASCII string with length n.
// Should never be considered for true random data generation.
func pRandASCII(n int) string {
	const asciiLetters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	// Use a single seed.
	dst := make([]byte, n)
	var seed [8]byte

	// Get something random
	_, _ = rand.Read(seed[:])
	rnd := binary.LittleEndian.Uint32(seed[0:4])
	rnd2 := binary.LittleEndian.Uint32(seed[4:8])
	for i := range dst {
		dst[i] = asciiLetters[int(rnd>>16)%len(asciiLetters)]
		rnd ^= rnd2
		rnd *= 2654435761
	}
	return string(dst)
}
