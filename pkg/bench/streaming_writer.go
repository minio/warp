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

package bench

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// StreamingOpsWriter writes Operations to a zstd-compressed TSV file as they
// arrive, without buffering them in memory. It is the streaming equivalent of
// Operations.CSV(), suitable for use during a live benchmark run.
//
// Usage with a Collector (production):
//  1. Create with NewStreamingOpsWriter.
//  2. Pass Receiver() as an extra channel to bench.NewNullCollector.
//  3. After Collector.Close() (which closes the receiver channel), call Wait().
//
// Standalone usage (tests):
//  1. Create with NewStreamingOpsWriter.
//  2. Send ops to Receiver().
//  3. Call Close() to flush and finalize.
type StreamingOpsWriter struct {
	ch       chan Operation
	done     chan struct{}
	clientID string
	cmdLine  string
	err      error
}

// NewStreamingOpsWriter creates the output file immediately, writes the TSV
// header, and starts a background goroutine to consume and write operations.
//
//   - path     – full path to the output .csv.zst file (created immediately)
//   - clientID – stamped on every row's client_id column; leave empty to
//     preserve any ClientID already set on arriving Operations
//   - cmdLine  – written as a trailing "# …" comment after all ops are flushed
func NewStreamingOpsWriter(path, clientID, cmdLine string) (*StreamingOpsWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		f.Close()
		return nil, err
	}

	bw := bufio.NewWriterSize(enc, 256*1024)

	// Write the header immediately so the file is valid even if the benchmark
	// is interrupted before any ops complete.
	const header = "idx\tthread\top\tclient_id\tn_objects\tbytes\tendpoint\tfile\terror\tstart\tfirst_byte\tend\tduration_ns\tcat\n"
	if _, err := bw.WriteString(header); err != nil {
		enc.Close()
		f.Close()
		return nil, err
	}

	w := &StreamingOpsWriter{
		ch:       make(chan Operation, 1000),
		done:     make(chan struct{}),
		clientID: clientID,
		cmdLine:  cmdLine,
	}

	go func() {
		defer close(w.done)

		var idx int
		for op := range w.ch {
			if w.clientID != "" {
				op.ClientID = w.clientID
			}
			if err := op.WriteCSV(bw, idx); err != nil {
				w.err = err
				// Drain remaining ops so senders are not blocked indefinitely.
				for range w.ch { //nolint:revive // intentionally empty drain loop
				}
				break
			}
			idx++
		}

		// Write trailing command-line comment (mirrors Operations.CSV behavior).
		if w.err == nil && len(w.cmdLine) > 0 {
			for txt := range strings.SplitSeq(w.cmdLine, "\n") {
				if _, err := fmt.Fprintf(bw, "# %s\n", txt); err != nil {
					w.err = err
					break
				}
			}
		}

		if w.err == nil {
			w.err = bw.Flush()
		}
		if err := enc.Close(); err != nil && w.err == nil {
			w.err = err
		}
		if err := f.Close(); err != nil && w.err == nil {
			w.err = err
		}
	}()

	return w, nil
}

// Receiver returns the send-only channel for incoming Operations.
// Pass this to bench.NewNullCollector as an extra channel for collector-integrated
// use; the collector's Close() will close the channel when the benchmark ends.
func (w *StreamingOpsWriter) Receiver() chan<- Operation {
	return w.ch
}

// Close stops accepting operations and waits for all buffered operations to be
// flushed to disk.  For standalone / test use only.  When the writer is wired
// through a Collector, use Wait() instead — Collector.Close() already closes
// the channel; calling Close() afterwards would panic on double-close.
func (w *StreamingOpsWriter) Close() error {
	close(w.ch)
	return w.Wait()
}

// Wait blocks until the background goroutine has fully flushed all operations
// and closed the output file.  Call this after the Collector's Close() method
// has been called: Collector.Close() closes all extra channels it holds
// (including the one returned by Receiver()), which signals the goroutine to
// finish.
func (w *StreamingOpsWriter) Wait() error {
	<-w.done
	return w.err
}
