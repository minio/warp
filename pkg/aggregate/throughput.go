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

package aggregate

import (
	"fmt"
	"math"
	"time"

	"github.com/minio/warp/pkg/bench"
)

// Throughput contains throughput.
type Throughput struct {
	// Errors recorded.
	Errors int `json:"errors"`
	// Time period of the throughput measurement.
	MeasureDurationMillis int `json:"measure_duration_millis"`
	// Start time of the measurement.
	StartTime time.Time `json:"start_time"`
	// End time of the measurement.
	EndTime time.Time `json:"end_time"`
	// Average bytes per second. Can be 0.
	AverageBPS float64 `json:"average_bps"`
	// Average operations per second.
	AverageOPS float64 `json:"average_ops"`
	// Number of full operations
	Operations int `json:"operations"`
	// Time segmented throughput summary.
	Segmented *ThroughputSegmented `json:"segmented,omitempty"`
}

// String returns a string representation of the segment
func (t Throughput) String() string {
	return t.StringDetails(true) + " " + t.StringDuration()
}

// StringDuration returns a string representation of the segment duration
func (t Throughput) StringDuration() string {
	return fmt.Sprintf("Duration: %v, starting %v", time.Duration(t.MeasureDurationMillis)*time.Millisecond, t.StartTime.Format("15:04:05 MST"))
}

// String returns a string representation of the segment
func (t Throughput) StringDetails(details bool) string {
	speed := ""
	if t.AverageBPS > 0 {
		speed = fmt.Sprintf("%.02f MiB/s, ", t.AverageBPS/(1<<20))
	}
	errs := ""
	if t.Errors > 0 {
		errs = fmt.Sprintf(", %d errors", t.Errors)
	}
	return fmt.Sprintf("%s%.02f obj/s%s",
		speed, t.AverageOPS, errs)
}

func (t *Throughput) fill(total bench.Segment) {
	mib, _, objs := total.SpeedPerSec()
	*t = Throughput{
		Operations:            total.FullOps,
		MeasureDurationMillis: durToMillis(total.EndsBefore.Sub(total.Start)),
		StartTime:             total.Start,
		EndTime:               total.EndsBefore,
		AverageBPS:            math.Round(mib*(1<<20)*10) / 10,
		AverageOPS:            math.Round(objs*100) / 100,
		Errors:                total.Errors,
	}
}

// ThroughputSegmented contains time segmented throughput statics.
type ThroughputSegmented struct {
	// Time of each segment.
	SegmentDurationMillis int `json:"segment_duration_millis"`
	// Will contain how segments are sorted.
	// Will be 'bps' (bytes per second) or 'ops' (objects per second).
	SortedBy string `json:"sorted_by"`

	// All segments, sorted
	Segments []SegmentSmall `json:"segments"`

	// Start time of fastest time segment.
	FastestStart time.Time `json:"fastest_start"`
	// Fastest segment bytes per second. Can be 0. In that case segments are sorted by operations per second.
	FastestBPS float64 `json:"fastest_bps"`
	// Fastest segment in terms of operations per second.
	FastestOPS float64 `json:"fastest_ops"`
	// 50% Median....
	MedianStart time.Time `json:"median_start"`
	MedianBPS   float64   `json:"median_bps"`
	MedianOPS   float64   `json:"median_ops"`
	// Slowest ...
	SlowestStart time.Time `json:"slowest_start"`
	SlowestBPS   float64   `json:"slowest_bps"`
	SlowestOPS   float64   `json:"slowest_ops"`
}

// BPSorOPS returns bytes per second if non zero otherwise operations per second as human readable string.
func BPSorOPS(bps, ops float64) string {
	if bps > 0 {
		return bench.Throughput(bps).String()
	}
	return fmt.Sprintf("%0.2f obj/s", ops)
}

// SegmentSmall represents a time segment of the run.
// Length of the segment is defined elsewhere.
type SegmentSmall struct {
	// Bytes per second during the time segment.
	BPS float64 `json:"bytes_per_sec"`

	// Operations per second during the time segment.
	OPS float64 `json:"obj_per_sec"`

	// Errors logged during the time segment.
	Errors int `json:"errors,omitempty"`

	// Start time of the segment.
	Start time.Time `json:"start"`
}

// cloneBenchSegments clones benchmark segments to the simpler representation.
func cloneBenchSegments(s bench.Segments) []SegmentSmall {
	res := make([]SegmentSmall, len(s))
	for i, seg := range s {
		mbps, _, ops := seg.SpeedPerSec()
		res[i] = SegmentSmall{
			BPS:    math.Round(mbps * (1 << 20)),
			OPS:    math.Round(ops*100) / 100,
			Errors: seg.Errors,
			Start:  seg.Start,
		}
	}
	return res
}

// String returns a string representation of the segment.
func (s SegmentSmall) StringLong(d time.Duration, details bool) string {
	speed := ""
	if s.BPS > 0 {
		speed = bench.Throughput(s.BPS).String() + ", "
	}
	detail := ""
	if details {
		detail = fmt.Sprintf(" (%v, starting %v)", d, s.Start.Format("15:04:05 MST"))
	}
	return fmt.Sprintf("%s%.02f obj/s%s",
		speed, s.OPS, detail)
}

func (a *ThroughputSegmented) fill(segs bench.Segments, total bench.Segment) {
	// Copy by time.
	segs.SortByTime()
	smallSegs := cloneBenchSegments(segs)

	// Sort to get correct medians.
	if total.TotalBytes > 0 {
		segs.SortByThroughput()
		a.SortedBy = "bps"
	} else {
		segs.SortByObjsPerSec()
		a.SortedBy = "ops"
	}

	fast := segs.Median(1)
	med := segs.Median(0.5)
	slow := segs.Median(0)

	bps := func(s bench.Segment) float64 {
		mib, _, _ := s.SpeedPerSec()
		return math.Round(mib * (1 << 20))
	}
	ops := func(s bench.Segment) float64 {
		_, _, objs := s.SpeedPerSec()
		return math.Round(objs*100) / 100
	}

	*a = ThroughputSegmented{
		Segments:              smallSegs,
		SortedBy:              a.SortedBy,
		SegmentDurationMillis: a.SegmentDurationMillis,
		FastestStart:          fast.Start,
		FastestBPS:            bps(fast),
		FastestOPS:            ops(fast),
		MedianStart:           med.Start,
		MedianBPS:             bps(med),
		MedianOPS:             ops(med),
		SlowestStart:          slow.Start,
		SlowestBPS:            bps(slow),
		SlowestOPS:            ops(slow),
	}
}
