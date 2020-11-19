/*
 * Warp (C) 2019-2020 MinIO, Inc.
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
	ops, err := OperationsFromCSV(bytes.NewBuffer(b), false, 0, 0, t.Logf)
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
