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
	"sort"
	"time"

	"github.com/minio/warp/pkg/bench"
)

// Throughput contains throughput.
type Throughput struct {
	// Start time of the measurement.
	StartTime time.Time `json:"start_time"`
	// End time of the measurement.
	EndTime time.Time `json:"end_time"`
	// Time segmented throughput summary.
	Segmented *ThroughputSegmented `json:"segmented,omitempty"`
	// Errors recorded.
	Errors int `json:"errors"`
	// Time period of the throughput measurement.
	MeasureDurationMillis int `json:"measure_duration_millis"`
	// Total bytes.
	Bytes float64 `json:"bytes"`
	// Total objects
	Objects float64 `json:"objects"`
	// Number of full operations
	Operations int `json:"ops"`
}

func (t Throughput) Add(o bench.Operation) Throughput {
	if t.StartTime.IsZero() || t.StartTime.After(o.Start) {
		t.StartTime = o.Start
	}
	if t.EndTime.IsZero() || t.EndTime.Before(o.End) {
		t.EndTime = o.End
	}
	t.MeasureDurationMillis = int(t.EndTime.Sub(t.StartTime).Milliseconds())
	t.Operations++
	if o.Err != "" {
		t.Errors++
	}
	t.Bytes += float64(o.Size)
	t.Objects += float64(o.ObjPerOp)
	return t
}

// BytesPS returns the bytes per second throughput for the time segment.
func (t Throughput) BytesPS() bench.Throughput {
	return bench.Throughput(1000 * t.Bytes / float64(t.MeasureDurationMillis))
}

// ObjectsPS returns the objects per second for the segment.
func (t Throughput) ObjectsPS() float64 {
	return 1000 * float64(t.Objects) / float64(t.MeasureDurationMillis)
}

// Merge currently running measurements.
func (t *Throughput) Merge(other Throughput) {
	if other.Operations == 0 {
		return
	}
	t.Errors += other.Errors
	t.Bytes += other.Bytes
	t.Objects += other.Objects
	t.Operations += other.Operations
	t.MeasureDurationMillis = max(t.MeasureDurationMillis, other.MeasureDurationMillis)
	if other.StartTime.Before(t.StartTime) {
		t.StartTime = other.StartTime
	}
	if other.EndTime.After(t.EndTime) {
		t.EndTime = other.EndTime
	}
	t.MeasureDurationMillis = int(t.EndTime.Sub(t.StartTime).Milliseconds())
	if t.Segmented == nil {
		t.Segmented = other.Segmented
	} else if t.Segmented != nil && other.Segmented != nil {
		t.Segmented.Merge(*other.Segmented)
	}
}

// String returns a string representation of the segment
func (t Throughput) String() string {
	return t.StringDetails(true) + " " + t.StringDuration()
}

// StringDuration returns a string representation of the segment duration
func (t Throughput) StringDuration() string {
	return fmt.Sprintf("Duration: %v, starting %v", time.Duration(t.MeasureDurationMillis)*time.Millisecond, t.StartTime.Format("15:04:05 MST"))
}

// StringDetails returns a detailed string representation of the segment
func (t Throughput) StringDetails(_ bool) string {
	if t.Bytes == 0 && t.Objects == 0 {
		return ""
	}
	speed := ""
	if t.Bytes > 0 {
		speed = fmt.Sprintf("%.02f MiB/s, ", t.BytesPS()/(1<<20))
	}
	errs := ""
	if false && t.Errors > 0 {
		errs = fmt.Sprintf(", %d errors", t.Errors)
	}
	//speed = fmt.Sprintf("O: %.0f, B:%.0f, D: %v - %s", t.Objects, t.Bytes, time.Duration(t.MeasureDurationMillis)*time.Millisecond, speed)
	return fmt.Sprintf("%s%.02f obj/s%s (%vs)",
		speed, t.ObjectsPS(), errs, (t.MeasureDurationMillis+500)/1000)
}

func (t *Throughput) fill(total bench.Segment) {
	*t = Throughput{
		Operations:            total.FullOps,
		MeasureDurationMillis: durToMillis(total.EndsBefore.Sub(total.Start)),
		StartTime:             total.Start,
		EndTime:               total.EndsBefore,
		Bytes:                 float64(total.TotalBytes),
		Objects:               total.Objects,
		Errors:                total.Errors,
	}
}

// ThroughputSegmented contains time segmented throughput statics.
type ThroughputSegmented struct {
	// Start time of fastest time segment.
	FastestStart time.Time `json:"fastest_start"`
	// 50% Median....
	MedianStart time.Time `json:"median_start"`
	// Slowest ...
	SlowestStart time.Time `json:"slowest_start"`
	// Will contain how segments are sorted.
	// Will be 'bps' (bytes per second) or 'ops' (objects per second).
	SortedBy string `json:"sorted_by"`

	// All segments, sorted
	Segments SegmentsSmall `json:"segments"`

	// Time of each segment.
	SegmentDurationMillis int `json:"segment_duration_millis"`

	// Fastest segment bytes per second. Can be 0. In that case segments are sorted by operations per second.
	FastestBPS float64 `json:"fastest_bps"`
	// Fastest segment in terms of operations per second.
	FastestOPS float64 `json:"fastest_ops"`
	MedianBPS  float64 `json:"median_bps"`
	MedianOPS  float64 `json:"median_ops"`
	SlowestBPS float64 `json:"slowest_bps"`
	SlowestOPS float64 `json:"slowest_ops"`
}

type SegmentsSmall []SegmentSmall

// SortByThroughput sorts the segments by throughput.
// Slowest first.
func (s SegmentsSmall) SortByThroughput() {
	sort.Slice(s, func(i, j int) bool {
		return s[i].BPS < s[j].BPS
	})
}

// SortByObjsPerSec sorts the segments by the number of objects processed in the segment.
// Lowest first.
func (s SegmentsSmall) SortByObjsPerSec() {
	sort.Slice(s, func(i, j int) bool {
		return s[i].OPS < s[j].OPS
	})
}

// SortByStartTime sorts the segments by the start time.
// Earliest first.
func (s SegmentsSmall) SortByStartTime() {
	sort.Slice(s, func(i, j int) bool {
		return s[i].Start.Before(s[j].Start)
	})
}

// Median returns the m part median.
// m is clamped to the range 0 -> 1.
func (s SegmentsSmall) Median(m float64) SegmentSmall {
	if len(s) == 0 {
		return SegmentSmall{}
	}
	m = math.Round(float64(len(s)) * m)
	m = math.Max(m, 0)
	m = math.Min(m, float64(len(s)-1))
	return s[int(m)]
}

// Merge 'other' into 't'.
// Will mutate (re-sort) both segments.
// Segments must have same time alignment.
func (t *SegmentsSmall) Merge(other SegmentsSmall) {
	if len(other) == 0 {
		return
	}
	a := *t
	if len(a) == 0 {
		*t = append(a, other...)
		return
	}
	var merged = make(SegmentsSmall, 0, len(a))
	a.SortByStartTime()
	other.SortByStartTime()
	// Add empty segments to a, so all in other are present
	for len(a) > 0 && len(other) > 0 {
		if len(other) == 0 {
			merged = append(merged, a...)
			break
		}
		if len(a) == 0 {
			merged = append(merged, other...)
			break
		}
		toMerge := other[0]
		idx := -1
		for i := range a {
			if a[i].Start.After(toMerge.Start) {
				// Store in previous index.
				break
			}
			idx = i
		}
		if idx == -1 {
			merged = append(merged, toMerge)
			other = other[1:]
		}
		if idx > 0 {
			merged = append(merged, a[:idx]...)
			a = a[idx:]
		}
		merged = append(merged, a[0].add(toMerge))
		other = other[1:]
	}
	merged.SortByStartTime()
	*t = merged
}

func (t *ThroughputSegmented) Merge(other ThroughputSegmented) {
	t.Segments.SortByStartTime()
	a, b := t.Segments, other.Segments
	if len(a) == 0 {
		a = other.Segments
		t.fillFromSegs()
		return
	}
	if len(b) == 0 {
		return
	}
	t.Segments.Merge(other.Segments)
	t.fillFromSegs()
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
	// Start time of the segment.
	Start time.Time `json:"start"`
	// Bytes per second during the time segment.
	BPS float64 `json:"bytes_per_sec"`

	// Objects per second during the time segment.
	OPS float64 `json:"obj_per_sec"`

	// Errors logged during the time segment.
	Errors int `json:"errors,omitempty"`
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

func (s *SegmentSmall) add(other SegmentSmall) SegmentSmall {
	s.Errors += other.Errors
	s.OPS += other.OPS
	s.BPS += other.BPS
	return *s
}

// StringLong returns a long string representation of the segment.
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

func (a *ThroughputSegmented) fill(segs bench.Segments, totalBytes int64) {
	// Copy by time.
	segs.SortByTime()
	smallSegs := cloneBenchSegments(segs)

	// Sort to get correct medians.
	if totalBytes > 0 {
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

func (a *ThroughputSegmented) fillFromSegs() {
	// Copy by time.
	segs := a.Segments
	var byBPS bool
	for _, seg := range segs {
		if seg.BPS > 0 {
			byBPS = true
			break
		}
	}
	// Sort to get correct medians.
	if byBPS {
		segs.SortByThroughput()
		a.SortedBy = "bps"
	} else {
		segs.SortByObjsPerSec()
		a.SortedBy = "ops"
	}

	fast := segs.Median(1)
	med := segs.Median(0.5)
	slow := segs.Median(0)

	*a = ThroughputSegmented{
		Segments:              segs,
		SortedBy:              a.SortedBy,
		SegmentDurationMillis: a.SegmentDurationMillis,
		FastestStart:          fast.Start,
		FastestBPS:            fast.BPS,
		FastestOPS:            fast.OPS,
		MedianStart:           med.Start,
		MedianBPS:             med.BPS,
		MedianOPS:             med.OPS,
		SlowestStart:          slow.Start,
		SlowestBPS:            slow.BPS,
		SlowestOPS:            slow.OPS,
	}
}
