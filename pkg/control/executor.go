/*
 * Warp (C) 2019-2026 MinIO, Inc.
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

package control

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Executor runs a benchmark for a single run. It is an interface so the
// subprocess-based DistributedExecutor used today can be replaced with an
// in-process coordinator (with live stats, degraded grace windows, etc.) later
// without changing the service.
type Executor interface {
	// Run executes the benchmark described by run. It blocks until the run
	// finishes or ctx is cancelled, returning the path to the result data file.
	Run(ctx context.Context, run *Run, target *Target, runDir string) (resultFile string, err error)
}

// DistributedExecutor drives a distributed warp run by invoking the warp binary
// in server mode (the presence of --warp-client switches warp into coordinator
// mode). This reuses warp's existing stage protocol, synchronized start and
// result merging without re-implementing them.
//
// TODO(milestone-2): replace with an in-process coordinator that streams live
// per-client stats, applies a reconnect grace window before declaring a client
// dropped, marks runs DEGRADED with the surviving client set, and reconciles
// orphaned objects left by dropped clients.
type DistributedExecutor struct {
	// WarpBinary is the path to the warp executable. Defaults to the running
	// binary when empty.
	WarpBinary string
}

// Run implements Executor.
func (e *DistributedExecutor) Run(ctx context.Context, run *Run, target *Target, runDir string) (string, error) {
	bin := e.WarpBinary
	if bin == "" {
		self, err := os.Executable()
		if err != nil {
			return "", fmt.Errorf("locating warp binary: %w", err)
		}
		bin = self
	}

	benchBase := filepath.Join(runDir, "result")
	args, env, err := buildArgs(run.Spec, target, run.ClientAddrs, benchBase)
	if err != nil {
		return "", err
	}

	logPath := filepath.Join(runDir, "run.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return "", fmt.Errorf("creating log file: %w", err)
	}
	defer logFile.Close()
	run.LogFile = logPath

	// Log the command (with secrets masked) so runs are debuggable.
	fmt.Fprintf(logFile, "# warp %s\n\n", strings.Join(maskSecrets(args), " "))

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("warp run failed: %w (see %s)", err, logPath)
	}

	// warp appends an extension to the --benchdata base; locate whichever it wrote.
	for _, ext := range []string{".csv.zst", ".json.zst"} {
		if p := benchBase + ext; fileExists(p) {
			return p, nil
		}
	}
	return "", fmt.Errorf("benchmark finished but no result file found at %s.*", benchBase)
}

func fileExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

// maskSecrets returns a copy of args with the value following --access-key or
// --secret-key replaced, so command lines can be logged safely.
func maskSecrets(args []string) []string {
	out := make([]string, len(args))
	copy(out, args)
	for i := 0; i < len(out)-1; i++ {
		if out[i] == "--access-key" || out[i] == "--secret-key" {
			if out[i+1] == "" {
				out[i+1] = "<EMPTY>"
			} else {
				out[i+1] = "****"
			}
		}
	}
	return out
}
