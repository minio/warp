/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
	b, err := ioutil.ReadFile("testdata/warp-benchdata-get.csv.zst")
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
			AllThreads:     true,
		})

		var buf bytes.Buffer
		err := segs.Print(&buf)
		if err != nil {
			t.Fatal(err)
		}

		segs.SortByThroughput()
		totals := ops.Total(true)
		ttfb := ops.TTFB(ops.ActiveTimeRange(true))

		t.Log("Operation type:", typ)
		t.Log("OpErrors:", len(ops.Errors()))
		t.Log("Fastest:", segs.Median(1))
		t.Log("Average:", totals)
		t.Log("50% Median:", segs.Median(0.5))
		t.Log("Slowest:", segs.Median(0.0))
		if ttfb.Average > 0 {
			t.Log("Time To First Byte:", ttfb)
		}
		t.Log(buf.String())
	}
}
