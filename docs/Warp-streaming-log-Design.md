# Warp Streaming Op-Log Design

**Status**: Proposed  
**Branch context**: `fix/default-tsv-output` (PR #475 on `minio/warp`)  
**Author**: Russ Fellows  
**Date**: April 2026

---

## 1. Problem Statement

When `--full` is used, warp accumulates every individual `Operation` struct in memory
inside `collector.ops []Operation` for the entire duration of the benchmark. Only after
`b.Close()` returns does `ops.CSV(enc, ...)` iterate through the slice and flush it to
a zstd-compressed file.

This batch-write design causes memory to scale with operation count — and worse, with
concurrency and duration combined:

| Duration | Concurrency | Obj size | Ops | Extra RAM (`--full` vs no-full) | Bytes/op |
|----------|-------------|----------|-----|---------------------------------|----------|
| 30 s     | 8           | 64 KiB   | ~30K  | +9 MB    | ~310 B/op |
| 120 s    | 8           | 64 KiB   | ~119K | +84 MB   | ~740 B/op |
| 60 s     | 32          | 4 KiB    | ~152K | +124 MB  | ~850 B/op |
| 1 hr     | 32          | 4 KiB    | ~9M   | ~8 GB (projected) | — |

The per-op overhead grows super-linearly because Go's GC arena does not release
allocations between operations — the entire slice stays live until the process exits.

**Goal**: Replace the post-benchmark batch write with a background goroutine that
streams operations to the zstd encoder as they arrive, making memory usage constant
(~2–3 MB) regardless of benchmark duration or concurrency.

---

## 2. Background: the Rust Reference Implementation

The design mirrors two existing Rust implementations in the sibling projects that already
use an identical TSV format (modeled directly on warp's `.csv.zst` format).

### 2.1 `s3dlio/src/s3_logger.rs`

Implements `Logger` — a `SyncSender<LogEntry>` + background `thread::spawn` writer:

```rust
// Background writer thread — runs for the whole process lifetime
thread::spawn(move || {
    for (idx, mut entry) in (0_u64..).zip(receiver.into_iter()) {
        if entry.operation == SHUTDOWN_OP { break; }
        entry.idx = idx;
        let line = entry.to_log_line(clock_offset);
        encoder.write_all(line.as_bytes())?;
    }
    drop(encoder);       // finalises the zstd frame on drop
    let _ = done_tx.send(());
});
```

Key design decisions:
- **Bounded channel** (default 256, tunable via `S3DLIO_OPLOG_BUF` env var)
- **`try_send` by default** (drops on overflow to avoid stalling I/O); lossless mode
  available via `S3DLIO_OPLOG_LOSSLESS=1`
- **`BufWriter::with_capacity(256 KiB)`** wrapping the zstd encoder
- **zstd level 1** (bias to speed)
- File opened at logger creation time; header written immediately

### 2.2 `sai3-bench/src/perf_log.rs`

Implements `PerfLogWriter` — a synchronous writer called from a periodic ticker:

```rust
pub fn write_entry(&mut self, entry: &PerfLogEntry) -> Result<()> {
    writeln!(self.writer, "{}", entry.to_tsv())?;
    Ok(())
}
```

Key design decisions:
- `BufWriter::with_capacity(64 KiB)` wrapping optional zstd encoder
- zstd level 3
- `auto_finish()` encoder — frame finalised automatically on drop
- File and header created at `PerfLogWriter::new()`

### 2.3 What warp needs

warp's `--full` path is more like s3dlio's `Logger` (high-frequency, concurrent ops
arriving from worker goroutines) than sai3-bench's `PerfLogWriter` (periodic 1-second
intervals). The channel-based goroutine model from s3dlio is the right approach.

**Critical difference from s3dlio**: warp must be **lossless** — dropping ops would
produce an incomplete benchmark record. `try_send` is not acceptable; all sends must
be blocking.

---

## 3. Current Code Architecture

### 3.1 Operation struct (`pkg/bench/ops.go`)

```go
type Operation struct {
    OpType    string
    Thread    uint32
    File      string
    Endpoint  string
    Size      int64
    ObjPerOp  int
    ClientID  string      // ← always empty at construction time; set post-hoc
    Start     time.Time
    End       time.Time
    FirstByte *time.Time
    Err       string
    Categories bench.Categories
}
```

At construction in every benchmark worker (e.g. `put.go`, `multipart.go`, etc.),
`ClientID` is **always left empty**. It is stamped retroactively via
`ops.SetClientID(cID)` after the benchmark completes.

### 3.2 CSV serialisation (`pkg/bench/ops.go`)

```go
// Header
"idx\tthread\top\tclient_id\tn_objects\tbytes\tendpoint\tfile\terror\tstart\tfirst_byte\tend\tduration_ns\tcat\n"

// Per-row (WriteCSV)
fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\n",
    i, o.Thread, o.OpType, o.ClientID, o.ObjPerOp, o.Size,
    csvEscapeString(o.Endpoint), o.File, csvEscapeString(o.Err),
    o.Start.Format(time.RFC3339Nano), ttfb,
    o.End.Format(time.RFC3339Nano), o.End.Sub(o.Start)/time.Nanosecond,
    o.Categories)
```

### 3.3 Current `--full` write path (local benchmark, `cli/benchmark.go`)

```
b.Start(ctx2, start)          // benchmark runs; ops accumulate in memory
c.Collector.Close()           // drains channel; all ops now in []Operation slice
ops := retrieveOps()          // returns the slice (~84–124 MB at moderate load)
ops.SortByStartTime()         // in-place sort of entire slice
ops.SetClientID(cID)          // stamps client_id on every element
ops.CSV(enc, cmdLine)         // writes entire slice to zstd encoder at once
```

`cID` is generated via `pRandASCII(4)` **before** `b.Start()` is called. It is also
embedded in the output filename suffix. This is important: `cID` is available at
the start of the benchmark, not just at the end.

### 3.4 Current `--full` write path (distributed, `cli/benchserver.go`)

In distributed mode:
1. Each agent runs its own local benchmark and holds ops in memory
2. After the benchmark, the controller calls `conns.downloadOps()` — each agent
   serialises its ops and sends them over gRPC; the controller receives them all
   into a combined `allOps` slice in memory
3. Controller calls `allOps.SortByStartTime()` then `allOps.CSV(enc, cmdLine)`

The distributed path has an additional constraint: ops from multiple agents arrive
in bulk transfers, not as a real-time stream. Streaming at the controller is still
beneficial (avoids holding all agents' ops simultaneously), but the architecture
is different from the local path.

### 3.5 Collector fan-out (`cli/benchmark.go` `addCollector()`)

```go
// --full path:
liveC := aggregate.LiveCollector(ctx, updates, pRandASCII(4), nil)  // separate ID
common.Collector, retrieveOps = bench.NewOpsCollector(
    append(common.ExtraOut, liveC.Receiver())...,
)
```

`NewOpsCollector` fans out each incoming op to: (a) the live display collector, and
(b) appends to `collector.ops` in memory. With streaming, the memory accumulation
in (b) is eliminated — a `StreamingOpsWriter` replaces the in-memory buffer.

Note: `pRandASCII(4)` passed to `LiveCollector` is a **separate, independent** random
string used only for per-client breakdown in the real-time display. It is not written
to the CSV and has no relation to `cID`.

---

## 4. Proposed Design

### 4.1 New type: `StreamingOpsWriter` (`pkg/bench/streaming_writer.go`)

A background-goroutine writer that consumes `Operation` values from a channel and
writes them directly to a zstd-compressed TSV file as they arrive.

```go
package bench

import (
    "bufio"
    "fmt"
    "os"
    "sync/atomic"
    "time"

    "github.com/klauspost/compress/zstd"
)

// StreamingOpsWriter writes Operations to a zstd-compressed TSV file as they
// arrive, without buffering them in memory. It is the streaming equivalent of
// Operations.CSV() but suitable for use during a live benchmark run.
type StreamingOpsWriter struct {
    ch       chan Operation  // ops arrive here from benchmark workers (via fan-out)
    done     chan struct{}   // closed when background goroutine has fully flushed
    clientID string         // stamped on every row (known before benchmark starts)
    cmdLine  string         // written as trailing # comment
    err      error          // first error from background goroutine; read after Close()
}

// NewStreamingOpsWriter creates the output file immediately, writes the TSV header,
// and starts a background goroutine to consume and write operations.
//
// path     - full path to the .csv.zst file (created immediately)
// clientID - value for the client_id TSV column (known before benchmark start)
// cmdLine  - written as a trailing comment after all ops are flushed
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

    // Write header immediately so the file is valid even if the benchmark is
    // interrupted before any ops complete.
    if _, err := bw.WriteString("idx\tthread\top\tclient_id\tn_objects\tbytes\tendpoint\tfile\terror\tstart\tfirst_byte\tend\tduration_ns\tcat\n"); err != nil {
        enc.Close()
        f.Close()
        return nil, err
    }

    w := &StreamingOpsWriter{
        ch:       make(chan Operation, 1000), // matches collector.rcv buffer size
        done:     make(chan struct{}),
        clientID: clientID,
        cmdLine:  cmdLine,
    }

    go func() {
        defer close(w.done)
        var idx int
        for op := range w.ch {
            op.ClientID = w.clientID  // stamp here, same as SetClientID did before
            if err := op.WriteCSV(bw, idx); err != nil {
                w.err = err
                // Drain channel so senders are not blocked
                for range w.ch {
                }
                break
            }
            idx++
        }
        // Write trailing command-line comment (same as Operations.CSV)
        if w.err == nil && len(w.cmdLine) > 0 {
            for _, line := range strings.SplitSeq(w.cmdLine, "\n") {
                if _, err := fmt.Fprintf(bw, "# %s\n", line); err != nil {
                    w.err = err
                    break
                }
            }
        }
        if w.err == nil {
            w.err = bw.Flush()
        }
        enc.Close() // finalises the zstd frame
        f.Close()
    }()

    return w, nil
}

// Receiver returns the send-only channel for incoming Operations.
// This channel should be passed as an `extra` channel to NewNullCollector or
// NewOpsCollector so that each completed operation is forwarded here.
func (w *StreamingOpsWriter) Receiver() chan<- Operation {
    return w.ch
}

// Close signals that no more operations will arrive and blocks until the
// background goroutine has fully flushed and closed the file.
// Returns the first write error encountered during streaming, or nil.
func (w *StreamingOpsWriter) Close() error {
    close(w.ch)
    <-w.done
    return w.err
}
```

### 4.2 Changes to `cli/benchmark.go` — local benchmark path

**Before** (current):
```go
fileName := ctx.String("benchdata")
cID := pRandASCII(4)
if fileName == "" {
    fileName = fmt.Sprintf(...)
}
// ... b.Start() runs, ops accumulate in memory ...
ops := retrieveOps()
ops.SortByStartTime()
ops.SetClientID(cID)
if len(ops) > 0 {
    f, _ := os.Create(fileName + ".csv.zst")
    enc, _ := zstd.NewWriter(f, ...)
    ops.CSV(enc, commandLine(ctx))
}
```

**After** (streaming):
```go
fileName := ctx.String("benchdata")
cID := pRandASCII(4)
if fileName == "" {
    fileName = fmt.Sprintf(...)
}

// Create streaming writer BEFORE b.Start() — file exists from t=0,
// even if benchmark is interrupted.
var csvWriter *bench.StreamingOpsWriter
if ctx.Bool("full") {
    var err error
    csvWriter, err = bench.NewStreamingOpsWriter(
        fileName+".csv.zst", cID, commandLine(ctx))
    if err != nil {
        monitor.Errorln("Unable to create benchmark data file:", err)
    }
}

// ... b.Start() runs — ops flow through channel to csvWriter in real time ...

// After benchmark ends:
if csvWriter != nil {
    if err := csvWriter.Close(); err != nil {
        monitor.Errorln("Error finalising benchmark data:", err)
    } else {
        monitor.InfoLn(fmt.Sprintf("\nBenchmark data written to %q\n", fileName+".csv.zst"))
    }
}
// json.zst write path unchanged
```

### 4.3 Changes to `cli/benchmark.go` `addCollector()`

**Before** (current):
```go
liveC := aggregate.LiveCollector(ctx, updates, pRandASCII(4), nil)
common.Collector, retrieveOps = bench.NewOpsCollector(
    append(common.ExtraOut, liveC.Receiver())...,
)
return retrieveOps, updates
```

**After** (streaming):
```go
liveC := aggregate.LiveCollector(ctx, updates, pRandASCII(4), nil)
// NewNullCollector discards ops after forwarding — no in-memory accumulation.
// csvWriter.Receiver() and liveC.Receiver() both receive every op.
common.Collector = bench.NewNullCollector(
    append(common.ExtraOut, liveC.Receiver(), csvWriter.Receiver())...,
)
return bench.EmptyOpsCollector, updates
```

`retrieveOps` is no longer needed; `EmptyOpsCollector` is returned as a sentinel.
`csvWriter` is passed in as a parameter or via closure.

### 4.4 Changes to `cli/benchserver.go` — distributed path

In distributed mode, the streaming opportunity is different: ops arrive as bulk
downloads from agents (not as a real-time stream). The streaming writer can still be
used to write each agent's batch as it arrives, avoiding holding all agents' ops
simultaneously in RAM:

```go
if ctx.Bool("full") {
    csvWriter, err := bench.NewStreamingOpsWriter(
        fileName+".csv.zst", "", commandLine(ctx))
    // ...
    for _, agentOps := range conns.downloadOpsStreaming() {
        agentOps.SortByStartTime()
        for _, op := range agentOps {
            csvWriter.Receiver() <- op
        }
    }
    csvWriter.Close()
}
```

Note: distributed mode does not use a `clientID` in the same way — agents set
their own `ClientID` before sending ops. The `StreamingOpsWriter` should accept an
empty `clientID` string and skip the stamp when empty.

**Simpler alternative for distributed**: keep the existing batch write for
`benchserver.go` in the initial implementation and only stream the local path.
The memory savings are largest for local benchmarks. This avoids changes to the
gRPC download path and reduces implementation risk.

### 4.5 `SortByStartTime` — no longer possible, and not needed

Streaming writes in arrival order, not start-time order. This is acceptable:

- `warp analyze` uses `StreamOperationsFromCSV` which processes ops via
  `bench.OperationsFromCSV` — this function does not require sorted input
- `warp replay` uses `StreamOperationsFromCSV` similarly
- Downstream tools (polarWarp, s3dlio replay) handle unsorted input

If sorted output is required for some reason, it could be done as a post-processing
step in `warp analyze` (already partially supported). Do not add an in-memory sort
back into the streaming path.

---

## 5. Memory Impact Summary

With streaming, the entire `[]Operation` slice is eliminated. The only memory overhead is:

- Channel buffer: 1000 ops × ~200 B (struct size, not GC-arena size) ≈ **200 KB**
- zstd write buffer: `bufio.NewWriterSize(..., 256*1024)` ≈ **256 KB**
- zstd encoder internal state: **~1 MB** (fixed, level-dependent)

Total constant overhead: **~1.5 MB**, regardless of benchmark duration or concurrency.

Comparison:

| Scenario | Current batch | Streaming | Savings |
|----------|--------------|-----------|---------|
| 30s / 8c / 64KiB   | +9 MB    | ~1.5 MB | ~7.5 MB |
| 120s / 8c / 64KiB  | +84 MB   | ~1.5 MB | ~82.5 MB |
| 60s / 32c / 4KiB   | +124 MB  | ~1.5 MB | ~122.5 MB |
| 1hr / 32c (proj.)  | ~8 GB    | ~1.5 MB | ~8 GB |

---

## 6. Analysis Time Impact

Measured on a 120s / 8c / 64KiB run (119K ops):

| Input | Analysis time | vs json.zst |
|-------|--------------|-------------|
| `.json.zst` (pre-aggregated)          | 0.133 s | 1× baseline |
| `.csv.zst` with `warp analyze --full` | 0.651 s | 4.9× slower |
| `.csv.zst` without `--full` (re-agg)  | 1.739 s | 13× slower  |

Streaming does not change these numbers — the on-disk format is identical. The
analysis time difference is inherent to the data volume in `.csv.zst` vs the
pre-aggregated `.json.zst`.

---

## 7. Files to Create / Modify

| File | Action | Notes |
|------|--------|-------|
| `pkg/bench/streaming_writer.go` | **Create** | New `StreamingOpsWriter` type |
| `cli/benchmark.go` | **Modify** | Two locations: `addCollector()` and local write block |
| `cli/benchserver.go` | **Modify (optional)** | Distributed write block; can defer to v2 |
| `cli/addcollector_test.go` | **Modify** | Add tests for streaming path |
| `README.md` | **Modify** | Update memory note to reflect new behaviour |

No changes required to:
- `pkg/bench/ops.go` — `WriteCSV` and `StreamOperationsFromCSV` used as-is
- `pkg/bench/collector.go` — `NewNullCollector` used as-is; no new collector type needed
- `pkg/bench/csv.go` — TSV escape helpers used as-is
- `pkg/aggregate/` — `LiveCollector` unchanged

---

## 8. Implementation Notes for Next Agent

1. **Read `pkg/bench/ops.go` lines 1048–1100** before writing the streaming writer.
   `WriteCSV` already exists and takes `(w io.Writer, i int)` — use it directly.
   Do not reimplement the serialisation.

2. **`cID` is generated before `b.Start()`** in both benchmark code paths (lines 220
   and 520 of `cli/benchmark.go`). Pass it to `NewStreamingOpsWriter` immediately after
   generation. The writer must stamp `op.ClientID = w.clientID` in the goroutine loop
   because all `Operation` structs arrive with `ClientID == ""`.

3. **The channel buffer size of 1000** matches `collector.rcv`. If benchmark workers
   produce ops faster than the zstd encoder can flush them, the channel will block
   worker goroutines. This is the correct backpressure behaviour — it prevents silent
   data loss. Do not use `try_send` / non-blocking sends.

4. **`strings.SplitSeq`** is used in the existing `ops.CSV()` for the trailing comment
   block (Go 1.24+). Use the same pattern in the streaming writer.

5. **Error handling in the goroutine**: use a sticky `w.err` field. If a write fails,
   drain the channel so worker goroutines are not blocked indefinitely, then surface
   the error via `Close()`. Use `fatalIf` in the caller (consistent with existing
   write-error handling in `benchmark.go`).

6. **`addCollector()` refactor**: the function currently returns `(OpsCollector, chan UpdateReq)`.
   With streaming, `OpsCollector` is no longer meaningful for `--full`. Two options:
   - Return `bench.EmptyOpsCollector` (already exists as a sentinel)
   - Add a `*StreamingOpsWriter` return value to `addCollector()`
   
   The second option is cleaner but changes the function signature. The first option
   requires passing `csvWriter` as a parameter to `addCollector()`. Either works;
   be consistent with whichever you choose.

7. **Test additions** (`cli/addcollector_test.go`): add at minimum:
   - `TestStreamingWriter_OpCount` — sends N ops, verifies N rows in output
   - `TestStreamingWriter_ClientID` — verifies `clientID` is stamped correctly
   - `TestStreamingWriter_FileCreatedEarly` — verifies file exists before `Close()`

8. **Do not modify** `benchserver.go` in the first implementation. Keep the scope
   minimal for the PR. The local benchmark path has 100% of the memory impact for
   single-node usage.

9. **Verify** with `go vet ./...` and `go test ./cli/ -run TestAddCollector -v`
   after implementation. Check for data races with `go test -race ./cli/`.

10. **The existing README memory note** (added in this PR) should be updated after
    implementation to reflect that streaming eliminates the memory scaling concern
    for local benchmarks.

---

## 9. Relationship to PR #475

This streaming writer is a follow-on improvement to PR #475 ("Restore per-transaction
.csv.zst output via --full flag"). It is **not part of PR #475** — that PR establishes
the `--full` flag semantics and restores the feature. The streaming implementation
should be a separate PR after #475 is merged, titled something like:

> `bench: stream --full ops directly to csv.zst, eliminating in-memory buffering`

The memory measurements and README note in PR #475 accurately describe the current
(batch) behaviour. Once streaming is implemented, the README note should be updated
to reflect the improvement.
