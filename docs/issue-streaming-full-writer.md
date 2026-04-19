# Feature: Stream `--full` ops to disk rather than buffering in memory

## Background

PR #475 restores the per-transaction `.csv.zst` output file behind a `--full` flag.
With `--full`, every individual operation is buffered in a `[]Operation` slice in RAM
for the entire benchmark duration, then written to disk in a single batch when the
benchmark ends.

## Problem

Memory overhead scales with both operation count and concurrency.
Measured on a local MinIO server (`put`, 64 KiB objects):

| Duration | Concurrency | Extra RAM (`--full` vs default) |
|----------|-------------|---------------------------------|
| 30 s     | 8           | +9 MB   (~310 B/op)             |
| 120 s    | 8           | +84 MB  (~740 B/op)             |
| 60 s     | 32          | +124 MB (~850 B/op)             |
| 1 hr     | 32          | ~8 GB (projected)               |

The per-op cost grows super-linearly with concurrency because Go's GC arena retains
the entire slice live until the process exits. At high concurrency or long durations
`--full` becomes impractical on machines with limited RAM, precisely the scenarios
where per-transaction data is most useful for diagnosing behaviour.

## Proposed Fix

Replace the post-benchmark batch write with a **background goroutine** that writes
each operation to a zstd encoder as it completes — a streaming writer.

The approach is straightforward:

1. Open the `.csv.zst` file and write the TSV header **before** `b.Start()`.
2. Spin up a goroutine that reads from a channel and calls `op.WriteCSV()` for each
   arriving operation.
3. After `b.Close()`, signal the goroutine and wait for the zstd frame to be finalised.

No changes to the on-disk format or the `warp analyze` read paths.
The `--full` flag semantics are unchanged.

## Memory Impact After Fix

The entire `[]Operation` slice is eliminated. Remaining overhead is constant:

- Channel buffer (1 000 ops): ~200 KB
- zstd write buffer (256 KiB): ~256 KB
- zstd encoder state: ~1 MB

**Total: ~1.5 MB constant, regardless of duration or concurrency.**

## Analysis Time (unchanged)

Streaming does not affect read-side performance. For reference:

| Input | Analysis time |
|-------|--------------|
| `.json.zst` (default, pre-aggregated) | 0.13 s |
| `.csv.zst` with `warp analyze --full` | 0.65 s |
| `.csv.zst` without `--full` (re-agg)  | 1.74 s |

## Scope

- New file `pkg/bench/streaming_writer.go` (~60 lines)
- Minor changes to `cli/benchmark.go` and `addCollector()`
- No changes to `pkg/bench/ops.go`, `pkg/bench/collector.go`, or any read path
- `benchserver.go` (distributed mode) can follow in a separate PR
