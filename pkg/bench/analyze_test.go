package bench

import (
	"bytes"
	"io/ioutil"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

var zstdDec, _ = zstd.NewReader(nil)

func TestOperations_Segment(t *testing.T) {
	b, err := ioutil.ReadFile("testdata/warp-benchdata-put.csv.zst")
	if err != nil {
		t.Fatal(err)
	}
	b, err = zstdDec.DecodeAll(b, nil)
	if err != nil {
		t.Fatal(err)
	}
	ops, err := OperationsFromCSV(bytes.NewBuffer(b))
	if err != nil {
		t.Fatal(err)
	}
	for typ, ops := range ops.ByOp() {
		segs := ops.Segment(SegmentOptions{
			From:           time.Time{},
			PerSegDuration: time.Second,
		})
		t.Log("Operation type:", typ)

		var buf bytes.Buffer
		err := segs.Print(&buf)
		if err != nil {
			t.Fatal(err)
		}

		segs.SortByThroughput()
		t.Log("Errors:", len(ops.Errors()))
		t.Log("Fastest:", segs.Median(1))
		t.Log("Average:", ops.Total())
		t.Log("50% Median:", segs.Median(0.5))
		t.Log("Slowest:", segs.Median(0.0))
		t.Log(buf.String())
	}
}
