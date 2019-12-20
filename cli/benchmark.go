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
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/warp/pkg/bench"
)

var emptyStringSlice = cli.StringSlice([]string{})

var benchFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "benchdata",
		Value: "",
		Usage: "Output benchmark+profile data to this file. By default unique filename is generated.",
	},
	cli.StringSliceFlag{
		Name:  "serverprof",
		Usage: "Run MinIO server profiling during benchmark; possible values are 'cpu', 'mem', 'block', 'mutex' and 'trace'.",
		Value: &emptyStringSlice,
	},
	cli.DurationFlag{
		Name:  "duration",
		Usage: "Duration to run the benchmark. Use 's' and 'm' to specify seconds and minutes.",
		Value: 5 * time.Minute,
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
	if ab != nil {
		return runClientBenchmark(ctx, b, ab)
	}
	if done, err := runServerBenchmark(ctx); done || err != nil {
		fatalIf(probe.NewError(err), "Error running remote benchmark")
		return nil
	}

	console.Infoln("Preparing server.")
	pgDone := make(chan struct{})
	c := b.GetCommon()
	c.Clear = !ctx.Bool("noclear")
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
			defer pg.FinishPrint("\n")
			tick := time.Tick(time.Millisecond * 125)
			pg.Set(-1)
			newVal := int64(-1)
			for {
				select {
				case <-tick:
					current := pg.Get()
					if current != newVal {
						pg.Set64(newVal)
						pg.Update()
					}
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

	// Start after waiting a second or until we reached the start time.
	tStart := time.Now().Add(time.Second)
	if st := ctx.String("syncstart"); st != "" {
		startTime := parseLocalTime(st)
		now := time.Now()
		if startTime.Before(now) {
			console.Errorln("Did not manage to prepare before syncstart")
			tStart = time.Now()
		} else {
			tStart = startTime
		}
	}

	bechDur := ctx.Duration("duration")
	ctx2, cancel := context.WithDeadline(context.Background(), tStart.Add(bechDur))
	defer cancel()
	start := make(chan struct{})
	go func() {
		<-time.After(time.Until(tStart))
		close(start)
	}()

	fileName := ctx.String("benchdata")
	cID := pRandAscii(4)
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s-%s", appName, ctx.Command.Name, time.Now().Format("2006-01-02[150405]"), cID)
	}

	prof, err := startProfiling(ctx)
	fatalIf(probe.NewError(err), "Unable to start profile.")
	console.Infoln("Starting benchmark in", tStart.Sub(time.Now()).Round(time.Second), "...")
	pgDone = make(chan struct{})
	if !globalQuiet && !globalJSON {
		pg := newProgressBar(int64(bechDur), pb.U_DURATION)
		go func() {
			defer close(pgDone)
			defer pg.FinishPrint("\n")
			tick := time.Tick(time.Millisecond * 125)
			done := ctx2.Done()
			for {
				select {
				case t := <-tick:
					elapsed := t.Sub(tStart)
					if elapsed < 0 {
						continue
					}
					pg.Set64(int64(elapsed))
					pg.Update()
				case <-done:
					pg.Set64(int64(bechDur))
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
	ops.SortByStartTime()
	ops.SetClientID(cID)
	prof.stop(ctx, fileName+".profiles.zip")

	f, err := os.Create(fileName + ".csv.zst")
	if err != nil {
		console.Error("Unable to write benchmark data:", err)
	} else {
		func() {
			defer f.Close()
			enc, err := zstd.NewWriter(f)
			fatalIf(probe.NewError(err), "Unable to compress benchmark output")

			defer enc.Close()
			err = ops.CSV(enc)
			fatalIf(probe.NewError(err), "Unable to write benchmark output")

			console.Infof("Benchmark data written to %q\n", fileName+".csv.zst")
		}()
	}
	printAnalysis(ctx, ops)
	if !ctx.Bool("keep-data") && !ctx.Bool("noclear") {
		console.Infoln("Starting cleanup...")
		b.Cleanup(context.Background())
	}
	return nil
}

var activeBenchmarkMu sync.Mutex
var activeBenchmark *clientBenchmark

type clientBenchmark struct {
	sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	results bench.Operations
	err     error
	stage   benchmarkStage
	info    map[benchmarkStage]stageInfo
}

type stageInfo struct {
	startRequested bool
	start          chan struct{}
	done           chan struct{}
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
func (c *clientBenchmark) stageDone(s benchmarkStage, err error) {
	console.Infoln(s, "done...")
	if err != nil {
		console.Errorln(err.Error())
	}
	c.Lock()
	info := c.info[s]
	if err != nil && c.err == nil {
		c.err = err
	}
	if info.done != nil {
		close(info.done)
	}
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
	stageBenchmark                 = "benchmark"
	stageCleanup                   = "cleanup"
	stageDone                      = "done"
	stageNotStarted                = ""
)

var benchmarkStages = []benchmarkStage{
	stagePrepare, stageBenchmark, stageCleanup,
}

func runClientBenchmark(ctx *cli.Context, b bench.Benchmark, cb *clientBenchmark) error {
	err := cb.waitForStage(stagePrepare)
	if err != nil {
		return err
	}
	err = b.Prepare(context.Background())
	cb.stageDone(stagePrepare, err)
	if err != nil {
		return err
	}

	// Start after waiting a second or until we reached the start time.
	benchDur := ctx.Duration("duration")
	cb.Lock()
	ctx2, cancel := context.WithCancel(cb.ctx)
	start := cb.info[stageBenchmark].start
	cb.Unlock()
	defer cancel()
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
	cID := pRandAscii(6)
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s-%s", appName, ctx.Command.Name, time.Now().Format("2006-01-02[150405]"), cID)
	}

	ops, err := b.Start(ctx2, start)
	cb.Lock()
	cb.results = ops
	cb.Unlock()
	cb.stageDone(stageBenchmark, err)
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
			enc, err := zstd.NewWriter(f)
			fatalIf(probe.NewError(err), "Unable to compress benchmark output")

			defer enc.Close()
			err = ops.CSV(enc)
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
	cb.stageDone(stageCleanup, nil)

	return nil
}

type runningProfiles struct {
	client *madmin.AdminClient
}

func startProfiling(ctx *cli.Context) (*runningProfiles, error) {
	prof := ctx.StringSlice("serverprof")
	if len(prof) == 0 {
		return nil, nil
	}
	var r runningProfiles
	r.client = newAdminClient(ctx)

	// Start profile
	for _, profilerType := range ctx.StringSlice("serverprof") {
		_, cmdErr := r.client.StartProfiling(madmin.ProfilerType(profilerType))
		if cmdErr != nil {
			return nil, cmdErr
		}
	}
	console.Infoln("Server profiling successfully started.")
	return &r, nil
}

func (rp *runningProfiles) stop(ctx *cli.Context, fileName string) {
	if rp == nil || rp.client == nil {
		return
	}

	// Ask for profile data, which will come compressed with zip format
	zippedData, adminErr := rp.client.DownloadProfilingData()
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

	for _, profilerType := range ctx.StringSlice("serverprof") {
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

// pRandAscii return pseudorandom ASCII string with length n.
// Should never be considered for true random data generation.
func pRandAscii(n int) string {
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
