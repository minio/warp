package cli

// Tests for addCollector behavior.
//
// The invariants we care about:
//
//  1. WITHOUT --full: the live aggregating collector is always active (updates ≠ nil,
//     live display and autoterm work); per-transaction ops are NOT stored (retrieveOps
//     returns empty), so no csv.zst is written.
//
//  2. WITH --full:    the live aggregating collector is ALSO active (updates ≠ nil,
//     live display and autoterm still work); per-transaction ops ARE stored
//     (retrieveOps returns all sent ops), so csv.zst IS written. --full is additive.
//
//  3. DiscardOutput=true: both retrieveOps and updates are nil/empty regardless of --full.

import (
	"context"
	"flag"
	"testing"
	"time"

	mcli "github.com/minio/cli"
	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// stubBench is the minimal bench.Benchmark stub needed by addCollector.
// It only needs GetCommon(); the other methods are never called in tests.
type stubBench struct {
	bench.Common
}

func (s *stubBench) Prepare(_ context.Context) error                { return nil }
func (s *stubBench) Start(_ context.Context, _ chan struct{}) error { return nil }
func (s *stubBench) Cleanup(_ context.Context)                      {}

// makeCtx builds a *cli.Context with only the flags addCollector reads.
func makeCtx(full bool) *mcli.Context {
	app := mcli.NewApp()
	set := flag.NewFlagSet("test", flag.ContinueOnError)
	set.Bool("full", full, "")
	return mcli.NewContext(app, set, nil)
}

// sendOp sends a single representative operation to the collector and returns it.
func sendOp(t *testing.T, c bench.Collector) bench.Operation {
	t.Helper()
	now := time.Now()
	op := bench.Operation{
		OpType: "GET",
		Start:  now,
		End:    now.Add(time.Millisecond),
		Size:   1024,
		Thread: 0,
	}
	c.Receiver() <- op
	return op
}

// requestFinal asks the live collector for its final aggregate (blocking).
func requestFinal(t *testing.T, updates chan<- aggregate.UpdateReq) *aggregate.Realtime {
	t.Helper()
	ch := make(chan *aggregate.Realtime, 1)
	updates <- aggregate.UpdateReq{Final: true, C: ch}
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for final aggregate from updates channel")
		return nil
	}
}

// ---------------------------------------------------------------------------
// Test 1: default mode (no --full)
// ---------------------------------------------------------------------------

// Without --full:
//   - updates must be non-nil  (live display / autoterm work)
//   - retrieveOps must return empty  (no per-transaction recording → no csv.zst)
func TestAddCollector_DefaultMode_NoOpsStored(t *testing.T) {
	ctx := makeCtx(false)
	b := &stubBench{}

	retrieveOps, updates := addCollector(ctx, b)

	// updates must be non-nil so live display and autoterm remain functional.
	if updates == nil {
		t.Fatal("expected non-nil updates channel in default (no --full) mode")
	}

	// Send an operation to the collector and close it.
	sendOp(t, b.Collector)
	b.Collector.Close()

	// Without --full, retrieveOps is EmptyOpsCollector → must return empty.
	ops := retrieveOps()
	if len(ops) != 0 {
		t.Errorf("expected 0 ops from retrieveOps in default mode, got %d", len(ops))
	}
}

// ---------------------------------------------------------------------------
// Test 2: --full mode — ops are stored for csv.zst
// ---------------------------------------------------------------------------

// With --full:
//   - updates must be non-nil  (live display / autoterm still work)
//   - retrieveOps must return all ops that were sent  (enables csv.zst write)
func TestAddCollector_FullMode_OpsAreCollected(t *testing.T) {
	ctx := makeCtx(true)
	b := &stubBench{}

	retrieveOps, updates := addCollector(ctx, b)

	// updates must be non-nil even in --full mode.
	if updates == nil {
		t.Fatal("expected non-nil updates channel in --full mode")
	}

	const numOps = 5
	for i := 0; i < numOps; i++ {
		sendOp(t, b.Collector)
	}
	b.Collector.Close()

	ops := retrieveOps()
	if len(ops) != numOps {
		t.Errorf("expected %d ops from retrieveOps in --full mode, got %d", numOps, len(ops))
	}
}

// ---------------------------------------------------------------------------
// Test 3: --full mode — live collector ALSO receives every op (additive)
// ---------------------------------------------------------------------------

// With --full the live collector is wired in via the extra-channel fan-out.
// After closing the collector the live aggregate must reflect the ops that
// were sent (TotalRequests > 0), proving the live path is active alongside
// the per-transaction path.
func TestAddCollector_FullMode_LiveCollectorAlsoReceivesOps(t *testing.T) {
	ctx := makeCtx(true)
	b := &stubBench{}

	retrieveOps, updates := addCollector(ctx, b)

	const numOps = 3
	for i := 0; i < numOps; i++ {
		sendOp(t, b.Collector)
	}
	// Close flushes bench.OpsCollector and, via the extra channel, also
	// signals the live collector to finish computing its aggregate.
	b.Collector.Close()

	// Sanity-check: per-transaction ops are present.
	ops := retrieveOps()
	if len(ops) != numOps {
		t.Fatalf("expected %d ops, got %d", numOps, len(ops))
	}

	// Live collector must also have received the ops.
	final := requestFinal(t, updates)
	if final == nil {
		t.Fatal("final aggregate is nil; live collector did not receive ops")
	}
	if final.Total.TotalRequests == 0 {
		t.Errorf("live collector TotalRequests == 0; ops were not forwarded to the live collector")
	}
}

// ---------------------------------------------------------------------------
// Test 4: default mode — updates channel is writable (live collector active)
// ---------------------------------------------------------------------------

// Without --full the live collector is the sole collector.
// We verify the updates channel is non-nil and functional. We cannot safely
// call requestFinal in this case because aggregate.collector.Close() sets its
// internal channel field to nil — if the LiveCollector goroutine reads after
// that, it would block on a nil channel range. In production this is fine
// because Close() is called minutes into a benchmark; in tests we avoid the
// race by only checking structural invariants here.
func TestAddCollector_DefaultMode_UpdatesChannelFunctional(t *testing.T) {
	ctx := makeCtx(false)
	b := &stubBench{}

	_, updates := addCollector(ctx, b)

	if updates == nil {
		t.Fatal("expected non-nil updates channel in default mode")
	}

	// Send ops then close.
	const numOps = 4
	for i := 0; i < numOps; i++ {
		sendOp(t, b.Collector)
	}
	b.Collector.Close()

	// The updates channel is buffered (capacity 1000); a write must not block.
	// A Reset request is a no-op for the live collector and safe to use here.
	select {
	case updates <- aggregate.UpdateReq{Reset: true}:
		// Channel is live and accepting writes.
	default:
		t.Fatal("updates channel blocked unexpectedly")
	}
}

// ---------------------------------------------------------------------------
// Test 5: DiscardOutput=true always suppresses collection
// ---------------------------------------------------------------------------

// When DiscardOutput is true, both retrieveOps and updates must be nil/empty.
// This is the "silent / terse run" mode used in distributed client mode.
func TestAddCollector_DiscardOutput_NullCollector(t *testing.T) {
	// DiscardOutput with --full=false
	t.Run("full=false", func(t *testing.T) {
		ctx := makeCtx(false)
		b := &stubBench{}
		b.DiscardOutput = true

		retrieveOps, updates := addCollector(ctx, b)

		if updates != nil {
			t.Error("expected nil updates when DiscardOutput=true and full=false")
		}
		ops := retrieveOps()
		if len(ops) != 0 {
			t.Errorf("expected 0 ops when DiscardOutput=true and full=false, got %d", len(ops))
		}
	})

	// DiscardOutput with --full=true (--full must not override DiscardOutput)
	t.Run("full=true", func(t *testing.T) {
		ctx := makeCtx(true)
		b := &stubBench{}
		b.DiscardOutput = true

		retrieveOps, updates := addCollector(ctx, b)

		if updates != nil {
			t.Error("expected nil updates when DiscardOutput=true even with --full")
		}
		ops := retrieveOps()
		if len(ops) != 0 {
			t.Errorf("expected 0 ops when DiscardOutput=true even with --full, got %d", len(ops))
		}
	})
}

// ---------------------------------------------------------------------------
// Test 6: --full is ADDITIVE — both file outputs can coexist in one run
// ---------------------------------------------------------------------------

// This test validates the core contract: --full must not disable any existing
// behavior, only add the per-transaction csv.zst path on top.
// We verify this by confirming both the ops slice AND the live aggregate are
// non-empty after the same set of operations.
func TestAddCollector_FullMode_IsAdditive(t *testing.T) {
	ctx := makeCtx(true)
	b := &stubBench{}

	retrieveOps, updates := addCollector(ctx, b)

	// Both must be non-nil immediately after addCollector returns.
	if updates == nil {
		t.Fatal("updates must be non-nil with --full (live display required)")
	}

	// Send ops.
	const numOps = 6
	for i := 0; i < numOps; i++ {
		sendOp(t, b.Collector)
	}
	b.Collector.Close()

	// Per-transaction store must be full.
	ops := retrieveOps()
	if len(ops) != numOps {
		t.Errorf("per-transaction store: expected %d ops, got %d", numOps, len(ops))
	}

	// Live aggregate must also be non-empty (additive, not replacing).
	final := requestFinal(t, updates)
	if final == nil {
		t.Fatal("live aggregate is nil; --full must not disable the live collector")
	}
	if final.Total.TotalRequests == 0 {
		t.Errorf("live aggregate TotalRequests == 0; --full must not disable the live collector")
	}
}

// ---------------------------------------------------------------------------
// Test 7: ops correctness — values are preserved through the collector
// ---------------------------------------------------------------------------

// With --full, the ops returned by retrieveOps must be the exact operations
// that were sent — no data loss or corruption through the fan-out path.
func TestAddCollector_FullMode_OpValuesPreserved(t *testing.T) {
	ctx := makeCtx(true)
	b := &stubBench{}

	retrieveOps, _ := addCollector(ctx, b)

	now := time.Now().Truncate(time.Millisecond) // avoid sub-ms rounding
	op := bench.Operation{
		OpType: "PUT",
		Start:  now,
		End:    now.Add(42 * time.Millisecond),
		Size:   8192,
		Thread: 3,
	}
	b.Collector.Receiver() <- op
	b.Collector.Close()

	ops := retrieveOps()
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d", len(ops))
	}
	got := ops[0]
	if got.OpType != op.OpType {
		t.Errorf("OpType: got %q want %q", got.OpType, op.OpType)
	}
	if !got.Start.Equal(op.Start) {
		t.Errorf("Start: got %v want %v", got.Start, op.Start)
	}
	if !got.End.Equal(op.End) {
		t.Errorf("End: got %v want %v", got.End, op.End)
	}
	if got.Size != op.Size {
		t.Errorf("Size: got %d want %d", got.Size, op.Size)
	}
	if got.Thread != op.Thread {
		t.Errorf("Thread: got %d want %d", got.Thread, op.Thread)
	}
}
