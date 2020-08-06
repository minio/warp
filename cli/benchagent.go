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
	"flag"
	"fmt"
	"time"

	"github.com/minio/cli"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

// agentReplyType indicates the agent reply type.
type agentReplyType string

const (
	agentRespBenchmarkStarted agentReplyType = "benchmark_started"
	agentRespStatus           agentReplyType = "benchmark_status"
	agentRespOps              agentReplyType = "ops"
)

// agentReply contains the response to a server request.
type agentReply struct {
	Type      agentReplyType   `json:"type"`
	Time      time.Time        `json:"time"`
	Err       string           `json:"err,omitempty"`
	Ops       bench.Operations `json:"ops,omitempty"`
	StageInfo struct {
		Started  bool    `json:"started"`
		Finished bool    `json:"finished"`
		Progress float64 `json:"progress"`
	} `json:"stage_info"`
}

// executeBenchmark will execute the benchmark and return any error.
func (s serverRequest) executeBenchmark(ctx context.Context) (*benchmarkOpts, error) {
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
	var cb benchmarkOpts
	cb.init(ctx)
	activeBenchmarkMu.Lock()
	activeBenchmark = &cb
	activeBenchmarkMu.Unlock()

	console.Infoln("Executing", cmd.Name, "benchmark.")
	if globalDebug {
		// params have secret, so disable by default.
		console.Infoln("Params:", s.Benchmark.Flags, ctx2.Args())
	}
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

// flagSet converts args and flags to a flagset.
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
