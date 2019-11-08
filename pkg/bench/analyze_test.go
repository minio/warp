package bench

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

var zstdDec, _ = zstd.NewReader(nil)

func TestOperations_Segment(t *testing.T) {
	b, err := ioutil.ReadFile("testdata/put-sample.csv.zst")
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
	segs := ops.Segment(SegmentOptions{
		Op:             "PUT",
		From:           time.Time{},
		PerSegDuration: 5 * time.Second,
	})
	segs.Print(os.Stdout)
	segs.SortByThroughput()
	fmt.Println("Errors:", len(ops.Errors()))
	fmt.Println("Fastest:", segs.Median(1))
	fmt.Println("Average:", ops.Total())
	fmt.Println("50% Median:", segs.Median(0.5))
	fmt.Println("Slowest:", segs.Median(0.0))
}
