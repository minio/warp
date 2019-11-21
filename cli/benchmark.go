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
	"fmt"
	"io"
	"os"
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
		Name:   "keep-data",
		Usage:  "Leave benchmark data. Do not run cleanup after benchmark. Bucket will still be cleaned prior to benchmark",
		Hidden: true,
	},
}

// runBench will run the supplied benchmark and save/print the analysis.
func runBench(ctx *cli.Context, b bench.Benchmark) error {
	console.Infoln("Preparing server.")

	pgDone := make(chan struct{})
	c := b.GetCommon()
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

	b.Prepare(context.Background())
	if c.PrepareProgress != nil {
		close(c.PrepareProgress)
		<-pgDone
	}
	// Start after waiting a second.
	tStart := time.Now().Add(time.Second)
	bechDur := ctx.Duration("duration")
	ctx2, cancel := context.WithDeadline(context.Background(), tStart.Add(bechDur))
	defer cancel()
	start := make(chan struct{})
	go func() {
		<-time.After(time.Until(tStart))
		close(start)
	}()

	fileName := ctx.String("benchdata")
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s", appName, ctx.Command.Name, time.Now().Format("2006-01-02[150405]"))
	}

	prof := startProfiling(ctx)
	console.Infoln("Starting benchmark...")
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
	ops := b.Start(ctx2, start)
	<-pgDone
	ops.SortByStartTime()
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
	if !ctx.Bool("keep-data") {
		console.Infoln("Starting cleanup...")
		b.Cleanup(context.Background())
	}
	printAnalysis(ctx, ops)
	return nil
}

type runningProfiles struct {
	client *madmin.AdminClient
}

func startProfiling(ctx *cli.Context) *runningProfiles {
	prof := ctx.StringSlice("serverprof")
	if len(prof) == 0 {
		return nil
	}
	var r runningProfiles
	r.client = newAdminClient(ctx)

	// Start profile
	for _, profilerType := range ctx.StringSlice("serverprof") {
		_, cmdErr := r.client.StartProfiling(madmin.ProfilerType(profilerType))
		fatalIf(probe.NewError(cmdErr), "Unable to start profile.")
	}
	console.Infoln("Server profiling successfully started.")
	return &r
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
}
