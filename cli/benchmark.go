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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/madmin-go/v4"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/api"
	"github.com/minio/warp/pkg/aggregate"
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
		Value: 15 * time.Second,
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
	cli.StringSliceFlag{
		Name:   "add-metadata",
		Usage:  "Add user metadata to all objects using the format <key>=<value>. Random value can be set with 'rand:%length'. Can be used multiple times. Example: --add-metadata foo=bar --add-metadata randomValue=rand:1024.",
		Hidden: true,
	},
	cli.StringSliceFlag{
		Name:   "tag",
		Usage:  "Add user tag to all objects using the format <key>=<value>. Random value can be set with 'rand:%length'. Can be used multiple times. Example: --tag foo=bar --tag randomValue=rand:1024.",
		Hidden: true,
	},
}

// runBench will run the supplied benchmark and save/print the analysis.
func runBench(ctx *cli.Context, b bench.Benchmark) error {
	defer globalWG.Wait()
	activeBenchmarkMu.Lock()
	ab := activeBenchmark
	activeBenchmarkMu.Unlock()
	c := b.GetCommon()
	c.Error = printError
	if ab != nil {
		c.ClientIdx = ab.clientIdx
		return runClientBenchmark(ctx, b, ab)
	}
	if done, err := runServerBenchmark(ctx, b); done || err != nil {
		// Close all extra output channels so the benchmark will terminate
		for _, out := range c.ExtraOut {
			close(out)
		}
		fatalIf(probe.NewError(err), "Error running remote benchmark")
		return nil
	}
	var ui ui
	if !globalQuiet && !globalJSON {
		go ui.Run()
	}

	retrieveOps, updates := addCollector(ctx, b)
	c.UpdateStatus = ui.SetSubText

	monitor := api.NewBenchmarkMonitor(ctx.String(serverFlagName), updates)
	monitor.SetLnLoggers(func(data ...interface{}) {
		ui.SetSubText(strings.TrimRight(fmt.Sprintln(data...), "\r\n."))
	}, printError)
	defer monitor.Done()

	monitor.InfoLn("Preparing server")
	c.Clear = !ctx.Bool("noclear")
	if ctx.Bool("autoterm") {
		c.AutoTermDur = ctx.Duration("autoterm.dur")
		c.AutoTermScale = ctx.Float64("autoterm.pct") / 100
	}
	c.PrepareProgress = make(chan float64, 1)
	ui.StartPrepare("Preparing", c.PrepareProgress, updates)

	err := b.Prepare(context.Background())
	fatalIf(probe.NewError(err), "Error preparing server")
	if c.PrepareProgress != nil {
		close(c.PrepareProgress)
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
	if u := ui.updates.Load(); u != nil {
		*u <- aggregate.UpdateReq{Reset: true}
	}
	benchDur := ctx.Duration("duration")
	ui.StartBenchmark("Benchmarking", tStart, tStart.Add(benchDur), updates)
	ctx2, cancel := context.WithDeadline(context.Background(), tStart.Add(benchDur))
	defer cancel()
	ui.cancelFn.Store(&cancel)
	start := make(chan struct{})
	go func() {
		monitor.InfoLn("Pausing before benchmark")
		<-time.After(time.Until(tStart))
		monitor.InfoLn("Press 'q' to abort benchmark and print partial results")
		close(start)
	}()

	fileName := ctx.String("benchdata")
	cID := pRandASCII(4)
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s-%s", appName, ctx.Command.Name, time.Now().Format("2006-01-02[150405]"), cID)
	}

	prof, err := startProfiling(ctx2, ctx)
	fatalIf(probe.NewError(err), "Unable to start profile.")
	monitor.InfoLn("Starting benchmark in", time.Until(tStart).Round(time.Second))
	b.Start(ctx2, start)
	c.Collector.Close()
	cancel()

	ctx2 = context.Background()
	prof.stop(ctx2, ctx, fileName+".profiles.zip")

	// Previous context is canceled, create a new...
	monitor.InfoLn("Saving benchmark data")
	if ops := retrieveOps(); len(ops) > 0 {
		ops.SortByStartTime()
		ops.SetClientID(cID)

		if len(ops) > 0 {
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

					monitor.InfoLn(fmt.Sprintf("\nBenchmark data written to %q\n", fileName+".csv.zst"))
				}()
			}
		}
		monitor.OperationsReady(ops, fileName, commandLine(ctx))
		var buf bytes.Buffer
		printAnalysis(ctx, &buf, ops)
		ui.Update(tea.Quit())
		ui.Wait()
		fmt.Println(buf.String())
	} else if updates != nil {
		finalCh := make(chan *aggregate.Realtime, 1)
		updates <- aggregate.UpdateReq{Final: true, C: finalCh}
		final := <-finalCh
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
				if err != nil {
					monitor.Errorln("Unable to compress benchmark data:", err)
				}

				defer enc.Close()
				js := json.NewEncoder(enc)
				js.SetIndent("", "  ")
				err = js.Encode(final)
				if err != nil {
					monitor.Errorln("Unable to write benchmark data:", err)
				}

				monitor.InfoLn(fmt.Sprintf("\nBenchmark data written to %q\n\n", fileName+".json.zst"))
			}()
		}
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

		monitor.UpdateAggregate(final, fileName)
		ui.Update(tea.Quit())
		ui.Wait()
		fmt.Println("")
		fmt.Println(rep)
	}
	if !ctx.Bool("keep-data") && !ctx.Bool("noclear") {
		ui.SetPhase("Cleanup")
		monitor.InfoLn("Starting cleanup...")
		b.Cleanup(context.Background())
	}
	monitor.InfoLn("Cleanup Done.")
	ui.Wait()
	return nil
}

var (
	activeBenchmarkMu sync.Mutex
	activeBenchmark   *clientBenchmark
)

type clientBenchmark struct {
	ctx       context.Context
	err       error
	cancel    context.CancelFunc
	info      map[benchmarkStage]stageInfo
	stage     benchmarkStage
	results   bench.Operations
	updates   chan<- aggregate.UpdateReq
	clientIdx int
	sync.Mutex
}

type stageInfo struct {
	start          chan struct{}
	done           chan struct{}
	custom         map[string]string
	stageCtx       context.Context
	cancelFn       context.CancelFunc
	startRequested bool
}

func (c *clientBenchmark) init(ctx context.Context) {
	c.results = nil
	c.err = nil
	c.stage = stageNotStarted
	c.info = make(map[benchmarkStage]stageInfo, len(benchmarkStages))
	c.ctx, c.cancel = context.WithCancel(ctx)
	for _, stage := range benchmarkStages {
		sCtx, sCancel := context.WithCancel(ctx)
		c.info[stage] = stageInfo{
			start:    make(chan struct{}),
			done:     make(chan struct{}),
			stageCtx: sCtx,
			cancelFn: sCancel,
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
	console.Infoln("Stage", s, "done...")
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
	if info.cancelFn != nil {
		info.cancelFn()
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

	retrieveOps, updates := addCollector(ctx, b)
	common := b.GetCommon()
	common.UpdateStatus = func(s string) {
		console.Infoln(s)
	}
	defer common.Collector.Close()

	cb.Lock()
	benchStage := cb.info[stageBenchmark]
	start := benchStage.start
	cb.updates = updates
	cb.Unlock()

	err = b.Prepare(cb.info[stagePrepare].stageCtx)
	cb.stageDone(stagePrepare, err, common.Custom)
	if err != nil {
		return err
	}

	ctx2, cancel := benchStage.stageCtx, benchStage.cancelFn
	defer cancel()

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

	err = b.Start(ctx2, start)
	ops := retrieveOps()
	cb.Lock()
	cb.results = ops
	cb.Unlock()
	cb.stageDone(stageBenchmark, err, common.Custom)
	if err != nil {
		return err
	}
	ops.SetClientID(cID)
	ops.SortByStartTime()
	common.Collector.Close()

	if len(ops) > 0 {
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
	} else if updates != nil {
		finalCh := make(chan *aggregate.Realtime, 1)
		updates <- aggregate.UpdateReq{Final: true, C: finalCh}
		final := <-finalCh
		final.Commandline = commandLine(ctx)
		final.WarpVersion = GlobalVersion
		final.WarpDate = GlobalDate
		final.WarpCommit = GlobalCommit
		f, err := os.Create(fileName + ".json.zst")
		if err != nil {
			console.Errorln("Unable to write benchmark data:", err)
		} else {
			func() {
				defer f.Close()
				enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
				if err != nil {
					console.Errorln("Unable to compress benchmark data:", err)
				}

				defer enc.Close()
				js := json.NewEncoder(enc)
				js.SetIndent("", "  ")
				err = js.Encode(final)
				if err != nil {
					console.Errorln("Unable to write benchmark data:", err)
				}
				console.Infoln(fmt.Sprintf("\nBenchmark data written to %q\n\n", fileName+".json.zst"))
			}()
		}
	}

	err = cb.waitForStage(stageCleanup)
	if err != nil {
		return err
	}
	if !ctx.Bool("keep-data") && !ctx.Bool("noclear") {
		console.Infoln("Starting cleanup...")
		b.Cleanup(cb.info[stageCleanup].stageCtx)
	}
	cb.stageDone(stageCleanup, nil, common.Custom)

	return nil
}

func addCollector(ctx *cli.Context, b bench.Benchmark) (bench.OpsCollector, chan<- aggregate.UpdateReq) {
	// Add collectors
	common := b.GetCommon()

	if !ctx.Bool("full") {
		updates := make(chan aggregate.UpdateReq, 1000)
		c := aggregate.LiveCollector(context.Background(), updates, pRandASCII(4))
		c.AddOutput(common.ExtraOut...)
		common.Collector = c
		return bench.EmptyOpsCollector, updates
	}
	if common.DiscardOutput {
		common.Collector = bench.NewNullCollector()
		common.Collector.AddOutput(common.ExtraOut...)
		return bench.EmptyOpsCollector, nil
	}
	var retrieveOps bench.OpsCollector
	common.Collector, retrieveOps = bench.NewOpsCollector()
	common.Collector.AddOutput(common.ExtraOut...)
	return retrieveOps, nil
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
	//nolint:staticcheck
	_, cmdErr := r.client.StartProfiling(ctx2, madmin.ProfilerType(prof))
	if cmdErr != nil {
		return nil, cmdErr
	}
	console.Infoln("Server profiling successfully started.")
	return &r, nil
}

func (rp *runningProfiles) stop(ctx2 context.Context, _ *cli.Context, fileName string) {
	if rp == nil || rp.client == nil {
		return
	}

	// Ask for profile data, which will come compressed with zip format
	//nolint:staticcheck
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
		madmin.ProfilerCPUIO,
		madmin.ProfilerThreads,
	}

	_, err := parseInfluxURL(ctx)
	fatalIf(probe.NewError(err), "invalid influx config")

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
	if t.Before(time.Now()) {
		t = t.Add(24 * time.Hour)
	}
	return t
}

// pRandASCII return pseudorandom ASCII string with length n.
// Should never be considered for true random data generation.
func pRandASCII(n int) string {
	const asciiLetters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	// Use a single seed.
	dst := make([]byte, n)
	seed := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}

	// Use a fixed seed for deterministic output
	rnd := binary.LittleEndian.Uint32(seed[0:4])
	rnd2 := binary.LittleEndian.Uint32(seed[4:8])
	for i := range dst {
		dst[i] = asciiLetters[int(rnd>>16)%len(asciiLetters)]
		rnd ^= rnd2
		rnd *= 2654435761
	}
	return string(dst)
}
