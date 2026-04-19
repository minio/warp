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
	"os"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// openCSVZst opens a .csv.zst file and returns its decoded Operations.
// Registered as a test helper so failures show the caller's file/line.
func openCSVZst(t *testing.T, path string) Operations {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	dec, err := zstd.NewReader(f)
	if err != nil {
		t.Fatalf("zstd.NewReader: %v", err)
	}
	defer dec.Close()

	ops, err := OperationsFromCSV(dec, false, 0, 0, func(string, ...any) {})
	if err != nil {
		t.Fatalf("OperationsFromCSV(%s): %v", path, err)
	}
	return ops
}

// makeOp returns a simple PUT operation with predictable field values.
func makeOp(thread uint32, size int64) Operation {
	now := time.Now().Truncate(time.Millisecond)
	return Operation{
		OpType:   "PUT",
		Thread:   thread,
		Start:    now,
		End:      now.Add(42 * time.Millisecond),
		Size:     size,
		Endpoint: "localhost:9000",
		File:     "test/obj",
	}
}

// ---------------------------------------------------------------------------
// Test 1: file is created at construction time, before any ops arrive
// ---------------------------------------------------------------------------

func TestStreamingWriter_FileCreatedOnNew(t *testing.T) {
	path := t.TempDir() + "/test.csv.zst"

	w, err := NewStreamingOpsWriter(path, "c1", "warp test")
	if err != nil {
		t.Fatalf("NewStreamingOpsWriter: %v", err)
	}
	defer w.Close() //nolint:errcheck

	// File must exist before any ops are sent and before Close() is called.
	if _, err := os.Stat(path); err != nil {
		t.Errorf("file not created at construction time: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test 2: correct number of ops written
// ---------------------------------------------------------------------------

func TestStreamingWriter_OpCount(t *testing.T) {
	path := t.TempDir() + "/ops.csv.zst"

	w, err := NewStreamingOpsWriter(path, "c1", "")
	if err != nil {
		t.Fatalf("NewStreamingOpsWriter: %v", err)
	}

	const numOps = 50
	for i := 0; i < numOps; i++ {
		w.Receiver() <- makeOp(uint32(i%4), int64(i*1024))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := openCSVZst(t, path)
	if len(got) != numOps {
		t.Errorf("expected %d ops, got %d", numOps, len(got))
	}
}

// ---------------------------------------------------------------------------
// Test 3: clientID is stamped on every row
// ---------------------------------------------------------------------------

func TestStreamingWriter_ClientIDStamped(t *testing.T) {
	path := t.TempDir() + "/cid.csv.zst"
	const wantID = "XYZW"

	w, err := NewStreamingOpsWriter(path, wantID, "")
	if err != nil {
		t.Fatalf("NewStreamingOpsWriter: %v", err)
	}

	w.Receiver() <- makeOp(0, 512)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ops := openCSVZst(t, path)
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d", len(ops))
	}
	if ops[0].ClientID != wantID {
		t.Errorf("ClientID: got %q, want %q", ops[0].ClientID, wantID)
	}
}

// ---------------------------------------------------------------------------
// Test 4: empty clientID preserves any ClientID set on the arriving Operation
// ---------------------------------------------------------------------------

func TestStreamingWriter_EmptyClientID_PreservesField(t *testing.T) {
	path := t.TempDir() + "/nocid.csv.zst"

	// Pass empty clientID — writer must not overwrite op.ClientID.
	w, err := NewStreamingOpsWriter(path, "", "")
	if err != nil {
		t.Fatalf("NewStreamingOpsWriter: %v", err)
	}

	op := makeOp(0, 1024)
	op.ClientID = "already-set"
	w.Receiver() <- op

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ops := openCSVZst(t, path)
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d", len(ops))
	}
	if ops[0].ClientID != "already-set" {
		t.Errorf("ClientID: got %q, want %q", ops[0].ClientID, "already-set")
	}
}

// ---------------------------------------------------------------------------
// Test 5: zero ops — empty writer produces a valid (header-only) file
// ---------------------------------------------------------------------------

func TestStreamingWriter_EmptyRun(t *testing.T) {
	path := t.TempDir() + "/empty.csv.zst"

	w, err := NewStreamingOpsWriter(path, "c1", "mycmd")
	if err != nil {
		t.Fatalf("NewStreamingOpsWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close on empty writer: %v", err)
	}

	ops := openCSVZst(t, path)
	if len(ops) != 0 {
		t.Errorf("expected 0 ops from empty writer, got %d", len(ops))
	}
}

// ---------------------------------------------------------------------------
// Test 6: operation field values are preserved end-to-end
// ---------------------------------------------------------------------------

func TestStreamingWriter_ValuesPreserved(t *testing.T) {
	path := t.TempDir() + "/vals.csv.zst"

	w, err := NewStreamingOpsWriter(path, "ID42", "")
	if err != nil {
		t.Fatalf("NewStreamingOpsWriter: %v", err)
	}

	now := time.Now().Truncate(time.Millisecond)
	fb := now.Add(10 * time.Millisecond)
	sent := Operation{
		OpType:    "STAT",
		Thread:    7,
		Start:     now,
		End:       now.Add(123 * time.Millisecond),
		FirstByte: &fb,
		Size:      65536,
		ObjPerOp:  2,
		Endpoint:  "s3.example.com:443",
		File:      "bucket/prefix/obj",
		Err:       "",
	}
	w.Receiver() <- sent

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ops := openCSVZst(t, path)
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d", len(ops))
	}
	got := ops[0]

	if got.ClientID != "ID42" {
		t.Errorf("ClientID: got %q, want %q", got.ClientID, "ID42")
	}
	if got.OpType != sent.OpType {
		t.Errorf("OpType: got %q, want %q", got.OpType, sent.OpType)
	}
	if got.Thread != sent.Thread {
		t.Errorf("Thread: got %d, want %d", got.Thread, sent.Thread)
	}
	if !got.Start.Equal(sent.Start) {
		t.Errorf("Start: got %v, want %v", got.Start, sent.Start)
	}
	if !got.End.Equal(sent.End) {
		t.Errorf("End: got %v, want %v", got.End, sent.End)
	}
	if got.FirstByte == nil || !got.FirstByte.Equal(fb) {
		t.Errorf("FirstByte: got %v, want %v", got.FirstByte, fb)
	}
	if got.Size != sent.Size {
		t.Errorf("Size: got %d, want %d", got.Size, sent.Size)
	}
}

// ---------------------------------------------------------------------------
// Test 7: multiple ops, all with correct clientID
// ---------------------------------------------------------------------------

func TestStreamingWriter_MultipleOps_AllClientIDSet(t *testing.T) {
	path := t.TempDir() + "/multi.csv.zst"
	const wantID = "AAAA"

	w, err := NewStreamingOpsWriter(path, wantID, "")
	if err != nil {
		t.Fatalf("NewStreamingOpsWriter: %v", err)
	}

	const n = 20
	for i := 0; i < n; i++ {
		op := makeOp(uint32(i), int64(i*512))
		op.OpType = "GET"
		w.Receiver() <- op
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ops := openCSVZst(t, path)
	if len(ops) != n {
		t.Fatalf("expected %d ops, got %d", n, len(ops))
	}
	for i, op := range ops {
		if op.ClientID != wantID {
			t.Errorf("op[%d] ClientID: got %q, want %q", i, op.ClientID, wantID)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 8: Wait() can be used as an alternative to Close() (collector pattern)
// ---------------------------------------------------------------------------

// Simulate the collector-integrated pattern:
//  1. Pass Receiver() to a NullCollector as an extra channel.
//  2. Close the collector (which closes the extra channel = Receiver()).
//  3. Call Wait() to drain.

func TestStreamingWriter_Wait_CollectorPattern(t *testing.T) {
	path := t.TempDir() + "/wait.csv.zst"

	w, err := NewStreamingOpsWriter(path, "cWait", "")
	if err != nil {
		t.Fatalf("NewStreamingOpsWriter: %v", err)
	}

	// Use NewNullCollector to simulate what addCollector does in streaming mode.
	col := NewNullCollector(w.Receiver())

	const numOps = 15
	for i := 0; i < numOps; i++ {
		col.Receiver() <- makeOp(0, 1024)
	}
	// Collector.Close() closes its extra channels, including w.Receiver().
	col.Close()

	// Wait() (not Close()) must be called here — Close() would double-close.
	if err := w.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	ops := openCSVZst(t, path)
	if len(ops) != numOps {
		t.Errorf("expected %d ops after Wait, got %d", numOps, len(ops))
	}
	for _, op := range ops {
		if op.ClientID != "cWait" {
			t.Errorf("ClientID mismatch: %q", op.ClientID)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 9: data-race check — concurrent sends are safe
// ---------------------------------------------------------------------------

func TestStreamingWriter_ConcurrentSends(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent test in short mode")
	}
	path := t.TempDir() + "/race.csv.zst"

	w, err := NewStreamingOpsWriter(path, "raceID", "")
	if err != nil {
		t.Fatalf("NewStreamingOpsWriter: %v", err)
	}

	const goroutines = 8
	const opsPerGoroutine = 50
	done := make(chan struct{})

	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < opsPerGoroutine; i++ {
				w.Receiver() <- makeOp(uint32(g), int64(g*i+1))
			}
		}(g)
	}
	for g := 0; g < goroutines; g++ {
		<-done
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ops := openCSVZst(t, path)
	if want := goroutines * opsPerGoroutine; len(ops) != want {
		t.Errorf("expected %d ops, got %d", want, len(ops))
	}
}
