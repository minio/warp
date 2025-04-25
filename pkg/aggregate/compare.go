/*
 * Warp (C) 2019-2025 MinIO, Inc.
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

package aggregate

import (
	"fmt"
	"time"

	"github.com/minio/warp/pkg/bench"
)

func Compare(before, after *LiveAggregate, op string) (*bench.Comparison, error) {
	var res bench.Comparison

	if before.TotalErrors > 0 || after.TotalErrors > 0 {
		return nil, fmt.Errorf("errors recorded in benchmark run. before: %v, after %d", before.TotalErrors, after.TotalErrors)
	}
	if after.Throughput.Segmented.Segments == nil || before.Throughput.Segmented.Segments == nil {
		return nil, fmt.Errorf("no segments found in benchmark run. before: %v, after %v", before.Throughput.Segmented.Segments, after.Throughput.Segmented.Segments)
	}
	res.Op = op
	as := after.Throughput.Segmented.Segments
	aDur := time.Duration(after.Throughput.Segmented.SegmentDurationMillis) * time.Millisecond
	bDur := time.Duration(after.Throughput.Segmented.SegmentDurationMillis) * time.Millisecond
	bs := before.Throughput.Segmented.Segments
	as.SortByObjsPerSec()
	bs.SortByObjsPerSec()
	res.Median.Compare(bs.Median(0.5).LongSeg(bDur), as.Median(0.5).LongSeg(aDur))
	res.Slowest.Compare(bs.Median(0.0).LongSeg(bDur), as.Median(0.0).LongSeg(aDur))
	res.Fastest.Compare(bs.Median(1).LongSeg(bDur), as.Median(1).LongSeg(aDur))

	beforeTotals := bench.Segment{
		EndsBefore: before.EndTime,
		Start:      before.StartTime,
		OpType:     op,
		Host:       "",
		OpsStarted: before.TotalRequests,
		PartialOps: 0,
		FullOps:    before.TotalObjects,
		OpsEnded:   before.TotalRequests,
		Objects:    float64(before.TotalObjects),
		Errors:     before.TotalErrors,
		ReqAvg:     float64(before.TotalRequests),
		TotalBytes: before.TotalBytes,
		ObjsPerOp:  before.TotalObjects / before.TotalRequests,
	}

	afterTotals := bench.Segment{
		EndsBefore: after.EndTime,
		Start:      after.StartTime,
		OpType:     op,
		Host:       "",
		OpsStarted: after.TotalRequests,
		PartialOps: 0,
		FullOps:    after.TotalObjects,
		OpsEnded:   after.TotalRequests,
		Objects:    float64(after.TotalObjects),
		Errors:     after.TotalErrors,
		ReqAvg:     float64(after.TotalRequests),
		TotalBytes: after.TotalBytes,
		ObjsPerOp:  after.TotalObjects / after.TotalRequests,
	}
	res.Average.Compare(beforeTotals, afterTotals)

	if after.Requests != nil && before.Requests != nil {
		a, _ := mergeRequests(after.Requests)
		b, _ := mergeRequests(before.Requests)
		// TODO: Do multisized?
		if a.Requests > 0 || b.Requests > 0 {
			ms := float64(time.Millisecond)
			const round = 100 * time.Microsecond
			aInv := 1.0 / max(1, float64(a.MergedEntries))
			bInv := 1.0 / max(1, float64(b.MergedEntries))
			res.Reqs.CmpRequests = bench.CmpRequests{
				AvgObjSize: a.ObjSize/int64(a.MergedEntries) - b.ObjSize/int64(b.MergedEntries),
				Requests:   a.Requests - b.Requests,
				Average:    time.Duration((a.DurAvgMillis*aInv - b.DurMedianMillis*bInv) * ms).Round(round),
				Worst:      time.Duration((a.SlowestMillis - b.SlowestMillis) * ms).Round(round),
				Best:       time.Duration((a.FastestMillis - b.FastestMillis) * ms).Round(round),
				Median:     time.Duration((a.DurMedianMillis*aInv - b.DurMedianMillis*bInv) * ms).Round(round),
				P90:        time.Duration((a.Dur90Millis*aInv - b.Dur90Millis*bInv) * ms).Round(round),
				P99:        time.Duration((a.Dur99Millis*aInv - b.Dur99Millis*bInv) * ms).Round(round),
				StdDev:     time.Duration((a.StdDev*aInv - b.StdDev*bInv) * ms).Round(round),
			}
			res.Reqs.Before = bench.CmpRequests{
				AvgObjSize: b.ObjSize / int64(b.MergedEntries),
				Requests:   b.Requests,
				Average:    time.Duration(b.DurAvgMillis * bInv * ms).Round(round),
				Worst:      time.Duration(b.SlowestMillis * ms).Round(round),
				Best:       time.Duration(b.FastestMillis * ms).Round(round),
				Median:     time.Duration(b.DurMedianMillis * bInv * ms).Round(round),
				P90:        time.Duration(b.Dur90Millis * bInv * ms).Round(round),
				P99:        time.Duration(b.Dur99Millis * bInv * ms).Round(round),
				StdDev:     time.Duration(b.StdDev * bInv * ms).Round(round),
			}
			res.Reqs.After = bench.CmpRequests{
				AvgObjSize: a.ObjSize / int64(a.MergedEntries),
				Requests:   a.Requests,
				Average:    time.Duration(a.DurAvgMillis * aInv * ms).Round(round),
				Worst:      time.Duration(a.SlowestMillis * ms).Round(round),
				Best:       time.Duration(a.FastestMillis * ms).Round(round),
				Median:     time.Duration(a.DurMedianMillis * aInv * ms).Round(round),
				P90:        time.Duration(a.Dur90Millis * aInv * ms).Round(round),
				P99:        time.Duration(a.Dur99Millis * aInv * ms).Round(round),
				StdDev:     time.Duration(a.StdDev * aInv * ms).Round(round),
			}
			if a.FirstByte != nil && b.FirstByte != nil {
				res.TTFB = b.FirstByte.AsBench(b.MergedEntries).Compare(a.FirstByte.AsBench(a.MergedEntries))
			}
		}
	}

	return &res, nil
}
