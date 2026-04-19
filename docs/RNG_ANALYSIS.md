# RNG Data Generation Analysis

## Overview

This fork of [minio/warp](https://github.com/minio/warp) includes a **bug fix** to the
pseudo-random data generator and a local copy of the fixed library in
`pkg/generator/rngfix/`. This document records the analysis of the generator's behavior,
the bug that was found, and why the fixed output is genuinely non-deduplicatable.

---

## Bug: `ResetSize()` Always Produced Identical Output

### Root cause

`minio/pkg/v3/rng` v3.6.1 contained a typo in `init()`. A local variable `tmp` (zero-filled
on the stack) was used instead of the struct field `r.tmp` (populated from the PRNG) when
deriving the four 64-bit XOR sub-keys (`r.subxor[0..3]`):

```go
// Buggy upstream code (init() in reader.go):
var tmp [32]byte                            // ← local, always zero
_, err := io.ReadFull(r.o.rng, r.tmp[:])   // ← fills r.tmp (struct field) — ignored!
r.subxor[0] = binary.LittleEndian.Uint64(tmp[:8])    // ← reads local zero
r.subxor[1] = binary.LittleEndian.Uint64(tmp[8:16])  // ← reads local zero
r.subxor[2] = binary.LittleEndian.Uint64(tmp[16:24]) // ← reads local zero
r.subxor[3] = binary.LittleEndian.Uint64(tmp[24:32]) // ← reads local zero
```

Because `subxor` was always `[0, 0, 0, 0]`, every call to `ResetSize()` produced an
**identical byte stream**, regardless of the PRNG seed.

### Impact

Measured on a 10 GB file split into 160 × 64 MiB chunks:

| Metric | Buggy upstream | Fixed (`rngfix`) |
|---|---|---|
| Unique 64 MiB chunks | **1 / 160** | **160 / 160** |
| Dedup savings (64 MiB blocks) | 99.375% | 0.000% |
| Unique 4 KB blocks | **16,384 / 2,621,440** | **2,621,440 / 2,621,440** |
| Dedup savings (4 KB blocks) | 99.375% | 0.000% |

GitHub issue filed: [minio/warp#471](https://github.com/minio/warp/issues/471)

### Fix (in `pkg/generator/rngfix/reader.go`)

Remove the dead local variable and read from `r.tmp` (the struct field):

```go
// Fixed:
_, err := io.ReadFull(r.o.rng, r.tmp[:])
r.subxor[0] = binary.LittleEndian.Uint64(r.tmp[:8])
r.subxor[1] = binary.LittleEndian.Uint64(r.tmp[8:16])
r.subxor[2] = binary.LittleEndian.Uint64(r.tmp[16:24])
r.subxor[3] = binary.LittleEndian.Uint64(r.tmp[24:32])
```

---

## How the Generator Works (Fixed)

### Data structure

```
Reader {
    buf    [16384]byte   // 16 KB seed — filled once from PRNG, never changes
    subxor [4]uint64     // 4 × 64-bit XOR keys — re-randomized on every ResetSize()
    offset int64         // current read position
}
```

### Output generation (`Read()`)

For every 32-byte aligned slice of output:

```
blockN       = offset / 16384             // changes every 16 KB of output
scrambleBase = scrambleU64(blockN)        // xxh3-style mixing of blockN
keys[i]      = scrambleBase ^ subxor[i] ^ (blockN × prime)

output[32 bytes] = buf[offset % 16384 : +32] XOR keys   (via SSE2 PXOR)
```

The `xorSlice` inner loop is SSE2 assembly (`MOVOU` + `PXOR`), processing 32 bytes per
iteration at ~46 GB/s single-core.

### Why every 4 KB block is unique

`bufferLog = 14` means `blockN` (and therefore `keys`) rotate once per **16 KB** of output.
This does **not** mean only 32 bytes per 16 KB window are unique. The full 16 KB of `r.buf`
seed data is indexed positionally with `offset & bufferMask`, so every 32-byte slot within
a 16 KB window reads from a **different position** in the 16 KB seed. The XOR keys are
identical within a window, but the seed inputs are different at every position.

Concretely:

- **Within a 16 KB window**: 512 × 32-byte output blocks differ because `buf` offsets differ.
- **Across 16 KB windows**: `blockN` changes, rotating `keys`, so even the same `buf`
  position produces different output.
- **Across `ResetSize()` calls**: `subxor` is fully re-randomized from the PRNG, so the
  entire stream is uncorrelated with any previous call.

This gives **three independent sources of variation**, making deduplication impossible at
any granularity — confirmed empirically down to 4 KB blocks across a 10 GB test file.

### Why the 4:1 dedup hypothesis was incorrect

One might expect that a 16 KB buffer would produce 4:1 redundancy at 4 KB granularity —
i.e., four identical 4 KB blocks per 16 KB window. This would be true only if `r.buf` were
a 32-byte repeating pattern. It is not: `r.buf` is a **16 KB block of independent random
seed data**, so every 4 KB sub-block within a 16 KB window is already distinct.

---

## Performance

Measured on this machine with the fixed generator:

**Generator speed (in-memory, CPU-bound):**

| Metric | Value |
|---|---|
| Single-core throughput | ~46 GB/s |
| 8-goroutine throughput | ~373 GB/s |

**10 GB test file write (I/O-bound):**

| Metric | Value |
|---|---|
| File write throughput | ~1.36 GB/s |
| Bottleneck | NVMe write speed |
| Dedup savings at 4 KB | 0.000% |
| Dedup savings at 64 MB | 0.000% |

The generator itself (SSE2 PXOR loop in `xorSlice`) runs at ~46 GB/s single-core — roughly
35× faster than the NVMe write speed. When writing to disk, the generator is completely
invisible in profiling; the workload is entirely I/O bound. In warp's normal use case
(writing objects to S3/GCS/Azure), the network is the bottleneck, not the generator.

---

## Files Changed

| File | Change |
|---|---|
| `pkg/generator/rngfix/` | New package — fixed copy of `minio/pkg/v3/rng` |
| `pkg/generator/random.go` | Uses `rngfix` instead of upstream `rng` |
| `cmd/datagen/main.go` | Standalone 10 GB test generator, uses `rngfix` |
| `pkg/generator/generator_bench_test.go` | Microbenchmarks for the fixed generator |
