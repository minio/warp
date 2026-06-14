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

package wui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"
)

// CompareData is the payload served at /api/compare. It holds a fully
// display-ready before/after comparison so the frontend only has to render it.
type CompareData struct {
	BeforeFile string      `json:"before_file"`
	AfterFile  string      `json:"after_file"`
	Ops        []CompareOp `json:"ops"`
}

// CompareOp is the comparison for a single operation type (GET, PUT, ...).
type CompareOp struct {
	Op         string       `json:"op"`
	Error      string       `json:"error,omitempty"`
	Info       []CompareRow `json:"info"`
	Throughput []CompareRow `json:"throughput"`
	Requests   []CompareRow `json:"requests"`
	TTFB       []CompareRow `json:"ttfb,omitempty"`
}

// CompareRow is a single before/after metric, already formatted for display.
// Improved is nil for neutral rows, true if "after" is better than "before".
// BeforeNum/AfterNum hold the raw numeric values (in Unit) for charting.
type CompareRow struct {
	Name      string  `json:"name"`
	Before    string  `json:"before"`
	After     string  `json:"after"`
	Change    string  `json:"change,omitempty"`
	Improved  *bool   `json:"improved,omitempty"`
	BeforeNum float64 `json:"before_num"`
	AfterNum  float64 `json:"after_num"`
	Unit      string  `json:"unit,omitempty"`
}

// BuildCompareData compares two benchmark results and returns a display-ready
// structure. wantOp, if set, limits the output to a single operation type.
func BuildCompareData(before, after *aggregate.Realtime, beforeFile, afterFile, wantOp string) *CompareData {
	out := &CompareData{BeforeFile: beforeFile, AfterFile: afterFile}

	ops := make([]string, 0, len(before.ByOpType))
	for typ := range before.ByOpType {
		ops = append(ops, typ)
	}
	sort.Strings(ops)

	for _, typ := range ops {
		if wantOp != "" && strings.ToUpper(wantOp) != typ {
			continue
		}
		b := before.ByOpType[typ]
		a := after.ByOpType[typ]
		op := CompareOp{Op: typ}
		if a == nil {
			op.Error = "operation not present in 'after' data"
			out.Ops = append(out.Ops, op)
			continue
		}

		cmp, err := aggregate.Compare(b, a, typ)
		if err != nil {
			op.Error = err.Error()
			out.Ops = append(out.Ops, op)
			continue
		}

		op.Info = buildInfoRows(b, a)
		op.Throughput = buildThroughputRows(cmp)
		op.Requests = buildRequestRows(cmp)
		op.TTFB = buildTTFBRows(cmp)
		out.Ops = append(out.Ops, op)
	}
	return out
}

func buildInfoRows(b, a *aggregate.LiveAggregate) []CompareRow {
	rows := []CompareRow{
		{Name: "Operations", Before: fmt.Sprintf("%d", b.TotalRequests), After: fmt.Sprintf("%d", a.TotalRequests)},
		{Name: "Concurrency", Before: fmt.Sprintf("%d", b.Concurrency), After: fmt.Sprintf("%d", a.Concurrency)},
		{Name: "Endpoints", Before: fmt.Sprintf("%d", len(b.ThroughputByHost)), After: fmt.Sprintf("%d", len(a.ThroughputByHost))},
		{Name: "Duration", Before: durStr(b), After: durStr(a)},
	}
	if b.TotalRequests > 0 && a.TotalRequests > 0 {
		rows = append(rows, CompareRow{
			Name:   "Objects / op",
			Before: fmt.Sprintf("%d", b.TotalObjects/b.TotalRequests),
			After:  fmt.Sprintf("%d", a.TotalObjects/a.TotalRequests),
		})
	}
	if b.TotalErrors+a.TotalErrors > 0 {
		rows = append(rows, CompareRow{
			Name:   "Errors",
			Before: fmt.Sprintf("%d", b.TotalErrors),
			After:  fmt.Sprintf("%d", a.TotalErrors),
		})
	}
	return rows
}

func durStr(l *aggregate.LiveAggregate) string {
	return l.EndTime.Sub(l.StartTime).Round(time.Second).String()
}

func buildThroughputRows(cmp *bench.Comparison) []CompareRow {
	type seg struct {
		name string
		c    bench.CmpSegment
	}
	segs := []seg{
		{"Average", cmp.Average},
		{"Fastest", cmp.Fastest},
		{"Median", cmp.Median},
		{"Slowest", cmp.Slowest},
	}
	var rows []CompareRow
	for _, s := range segs {
		if s.c.Before == nil || s.c.After == nil {
			continue
		}
		mibB, _, objsB := s.c.Before.SpeedPerSec()
		mibA, _, objsA := s.c.After.SpeedPerSec()
		rows = append(rows,
			pctRow(s.name+" throughput",
				fmt.Sprintf("%.1f MiB/s", mibB),
				fmt.Sprintf("%.1f MiB/s", mibA),
				mibB, mibA, true, "MiB/s"),
			pctRow(s.name+" objects",
				fmt.Sprintf("%.1f obj/s", objsB),
				fmt.Sprintf("%.1f obj/s", objsA),
				objsB, objsA, true, "obj/s"),
		)
	}
	return rows
}

func buildRequestRows(cmp *bench.Comparison) []CompareRow {
	b, a := cmp.Reqs.Before, cmp.Reqs.After
	if b.Requests == 0 && a.Requests == 0 {
		return nil
	}
	rows := []CompareRow{
		latRow("Average", b.Average, a.Average),
		latRow("Best", b.Best, a.Best),
		latRow("Median (P50)", b.Median, a.Median),
		latRow("P90", b.P90, a.P90),
		latRow("P99", b.P99, a.P99),
		latRow("Worst", b.Worst, a.Worst),
		latRow("Std Dev", b.StdDev, a.StdDev),
	}
	if b.AvgObjSize != a.AvgObjSize {
		rows = append([]CompareRow{{
			Name:   "Avg object size",
			Before: humanize.IBytes(uint64(b.AvgObjSize)),
			After:  humanize.IBytes(uint64(a.AvgObjSize)),
		}}, rows...)
	}
	return rows
}

func buildTTFBRows(cmp *bench.Comparison) []CompareRow {
	if cmp.TTFB == nil {
		return nil
	}
	b, a := cmp.TTFB.Before, cmp.TTFB.After
	return []CompareRow{
		latRow("Average", b.Average, a.Average),
		latRow("Best", b.Best, a.Best),
		latRow("Median (P50)", b.Median, a.Median),
		latRow("P90", b.P90, a.P90),
		latRow("P99", b.P99, a.P99),
		latRow("Worst", b.Worst, a.Worst),
		latRow("Std Dev", b.StdDev, a.StdDev),
	}
}

// latRow builds a latency row where lower is better. Numeric values are
// expressed in milliseconds for charting.
func latRow(name string, before, after time.Duration) CompareRow {
	ms := float64(time.Millisecond)
	return pctRow(name,
		before.Round(10*time.Microsecond).String(),
		after.Round(10*time.Microsecond).String(),
		float64(before)/ms, float64(after)/ms, false, "ms")
}

// pctRow formats a row with a percentage change. higherIsBetter selects the
// direction that counts as an improvement. before/after are the raw numeric
// values (in unit) and are stored for charting.
func pctRow(name, beforeStr, afterStr string, before, after float64, higherIsBetter bool, unit string) CompareRow {
	row := CompareRow{
		Name:      name,
		Before:    beforeStr,
		After:     afterStr,
		BeforeNum: before,
		AfterNum:  after,
		Unit:      unit,
	}
	if before != 0 {
		pct := 100 * (after - before) / before
		sign := ""
		if pct > 0 {
			sign = "+"
		}
		row.Change = fmt.Sprintf("%s%.1f%%", sign, pct)
		improved := after > before
		if !higherIsBetter {
			improved = after < before
		}
		if after != before {
			row.Improved = &improved
		}
	}
	return row
}
