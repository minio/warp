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
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"sort"
	"time"
)

// SegmentOptions describe options used to segment operations.
type SegmentOptions struct {
	From           time.Time
	PerSegDuration time.Duration
	AllThreads     bool
	MultiOp        bool
}

// A Segment represents totals of operations in a specific time segment
// starting at Start and ending before EndsBefore.
type Segment struct {
	OpType     string    `json:"op"`
	Host       string    `json:"host"`
	ObjsPerOp  int       `json:"objects_per_op"`
	TotalBytes int64     `json:"total_bytes"`
	FullOps    int       `json:"full_ops"`
	PartialOps int       `json:"partial_ops"`
	OpsStarted int       `json:"ops_started"`
	OpsEnded   int       `json:"ops_ended"`
	Objects    float64   `json:"objects"`
	Errors     int       `json:"errors"`
	ReqAvg     float64   `json:"req_avg_ms"` // Average duration of operations ending in segment.
	Start      time.Time `json:"start"`
	EndsBefore time.Time `json:"ends_before"`
}

// TTFB contains time to first byte stats.
type TTFB struct {
	Average     time.Duration
	Best        time.Duration
	P25         time.Duration
	Median      time.Duration
	P75         time.Duration
	P90         time.Duration
	P99         time.Duration
	Worst       time.Duration
	Percentiles [101]time.Duration `json:"percentiles_millis"`
}

// Segments is a slice of segment elements.
type Segments []Segment

// Clone returns a copy of the segment elements.
func (s Segments) Clone() Segments {
	res := make(Segments, len(s))
	copy(res, s)
	return res
}

// Total will return the total of active operations.
// See ActiveTimeRange how this is determined.
// Specify whether one operation for all threads should be skipped or just a single.
func (o Operations) Total(allThreads bool) Segment {
	start, end := o.ActiveTimeRange(allThreads)
	if start.Equal(end) {
		return Segment{}
	}
	return o.Segment(SegmentOptions{
		From:           start,
		PerSegDuration: end.Sub(start) - 1,
		AllThreads:     allThreads,
		MultiOp:        o.IsMixed(),
	})[0]
}

// TTFB returns time to first byte stats for all operations completely within the time segment.
func (o Operations) TTFB(start, end time.Time) TTFB {
	if start.After(end) || start.Equal(end) {
		return TTFB{}
	}

	filtered := o.FilterByHasTTFB(true).FilterInsideRange(start, end)
	if len(filtered) == 0 {
		return TTFB{}
	}
	filtered.SortByTTFB()

	res := TTFB{
		Average: 0,
		Best:    filtered.Median(0).TTFB(),
		P25:     filtered.Median(0.25).TTFB(),
		Median:  filtered.Median(0.5).TTFB(),
		P75:     filtered.Median(0.75).TTFB(),
		P90:     filtered.Median(0.9).TTFB(),
		P99:     filtered.Median(0.99).TTFB(),
		Worst:   filtered.Median(1).TTFB(),
	}
	for i := range res.Percentiles[:] {
		res.Percentiles[i] = filtered.Median(float64(i) / 100).TTFB()
	}

	for _, op := range filtered {
		ttfb := op.TTFB()
		res.Average += ttfb
	}
	res.Average /= time.Duration(len(filtered))
	return res
}

// OpThroughput returns the average throughput in B/s.
func (o Operations) OpThroughput() Throughput {
	var aDur time.Duration
	var aBytes int64
	for _, op := range o {
		if op.Size > 0 {
			aDur += op.Duration()
			aBytes += op.Size
		}
	}
	if aDur == 0 {
		return 0
	}
	return Throughput(aBytes) * Throughput(time.Second) / Throughput(aDur)
}

// Segment will segment the operations o.
// Operations should be of the same type.
// Operations will be sorted by start time.
func (o Operations) Segment(so SegmentOptions) Segments {
	o.SortByStartTime()
	if so.PerSegDuration <= 0 {
		return nil
	}
	start, end := o.ActiveTimeRange(so.AllThreads)
	if start.After(so.From) {
		so.From = start
	}
	var segments []Segment
	segStart := so.From
	host := ""
	if e := o.Endpoints(); len(e) == 1 {
		host = e[0]
	}
	for segStart.Before(end.Add(-so.PerSegDuration)) {
		s := Segment{
			OpType:     o.FirstOpType(),
			Host:       host,
			ObjsPerOp:  o.FirstObjPerOp(),
			TotalBytes: 0,
			FullOps:    0,
			PartialOps: 0,
			OpsStarted: 0,
			OpsEnded:   0,
			Objects:    0,
			Start:      segStart,
			EndsBefore: segStart.Add(so.PerSegDuration),
		}
		if so.MultiOp {
			s.OpType = ""
			s.ObjsPerOp = 0
		}
		// Search for the first entry
		first := 0
		for i, op := range o {
			if op.End.After(s.Start) {
				break
			}
			first = i
		}
		for _, op := range o[first:] {
			if op.Aggregate(&s) {
				break
			}
		}
		if s.OpsEnded > 0 {
			s.ReqAvg /= float64(s.OpsEnded)
		}
		segments = append(segments, s)
		segStart = segStart.Add(so.PerSegDuration)
	}
	return segments
}

// SpeedPerSec returns mb/s for the segment and the ops ended per second.
func (s Segment) SpeedPerSec() (mib, ops, objs float64) {
	scale := float64(s.EndsBefore.Sub(s.Start)) / float64(time.Second)
	mib = float64(s.TotalBytes) / (1024 * 1024) / scale
	ops = float64(s.OpsEnded) / scale
	objs = s.Objects / scale
	return
}

// Print segments to a supplied writer.
func (s Segments) Print(w io.Writer) error {
	for i, seg := range s {
		_, err := fmt.Fprintf(w, "%d: %v\n", i, seg)
		if err != nil {
			return err
		}
	}
	return nil
}

// CSV writes segments to a supplied writer as CSV data.
func (s Segments) CSV(w io.Writer) error {
	cw := csv.NewWriter(w)
	cw.Comma = '\t'
	err := cw.Write([]string{
		"index",
		"op",
		"host",
		"duration_s",
		"objects_per_op",
		"bytes",
		"full_ops",
		"partial_ops",
		"ops_started",
		"ops_ended",
		"errors",
		"mb_per_sec",
		"ops_ended_per_sec",
		"objs_per_sec",
		"reqs_ended_avg_ms",
		"start_time",
		"end_time",
	})
	if err != nil {
		return err
	}
	for i, seg := range s {
		err := seg.CSV(cw, i)
		if err != nil {
			return err
		}
	}
	cw.Flush()
	return nil
}

// CSV writes a CSV representation of the segment to the supplied writer.
func (s Segment) CSV(w *csv.Writer, idx int) error {
	mib, ops, objs := s.SpeedPerSec()
	return w.Write([]string{
		fmt.Sprint(idx),
		s.OpType,
		s.Host,
		fmt.Sprint(float64(s.EndsBefore.Sub(s.Start)) / float64(time.Second)),
		fmt.Sprint(s.ObjsPerOp),
		fmt.Sprint(s.TotalBytes),
		fmt.Sprint(s.FullOps),
		fmt.Sprint(s.PartialOps),
		fmt.Sprint(s.OpsStarted),
		fmt.Sprint(s.OpsEnded),
		fmt.Sprint(s.Errors),
		fmt.Sprint(mib),
		fmt.Sprint(ops),
		fmt.Sprint(objs),
		fmt.Sprint(s.ReqAvg),
		fmt.Sprint(s.Start),
		fmt.Sprint(s.EndsBefore),
	})
}

// String returns a string representation of the segment
func (s Segment) Duration() time.Duration {
	return s.EndsBefore.Sub(s.Start)
}

// String returns a string representation of the segment
func (s Segment) String() string {
	mib, _, objs := s.SpeedPerSec()
	speed := ""
	if mib > 0 {
		speed = fmt.Sprintf("%.02f MiB/s, ", mib)
	}
	return fmt.Sprintf("%s%.02f obj/s (%v, starting %v)",
		speed, objs, s.EndsBefore.Sub(s.Start).Round(time.Millisecond), s.Start.Format("15:04:05 MST"))
}

// ShortString returns a string representation of the segment without ops ended/s.
func (s Segment) ShortString() string {
	mib, _, objs := s.SpeedPerSec()
	speed := ""
	if mib > 0 {
		speed = fmt.Sprintf("%.02f MiB/s, ", mib)
	}
	return fmt.Sprintf("%s%.02f obj/s (%v)",
		speed, objs, s.EndsBefore.Sub(s.Start).Round(time.Millisecond))
}

// SortByThroughput sorts the segments by throughput.
// Slowest first.
func (s Segments) SortByThroughput() {
	sort.Slice(s, func(i, j int) bool {
		imb, _, _ := s[i].SpeedPerSec()
		jmb, _, _ := s[j].SpeedPerSec()
		return imb < jmb
	})
}

// SortByOpsEnded sorts the segments by the number of ops ended in segment.
// Lowest first.
func (s Segments) SortByOpsEnded() {
	sort.Slice(s, func(i, j int) bool {
		_, iops, _ := s[i].SpeedPerSec()
		_, jops, _ := s[j].SpeedPerSec()
		return iops < jops
	})
}

// SortByOpsEnded sorts the segments by the number of distributed objects processed.
// Lowest first.
func (s Segments) SortByObjsPerSec() {
	sort.Slice(s, func(i, j int) bool {
		_, _, io := s[i].SpeedPerSec()
		_, _, jo := s[j].SpeedPerSec()
		return io < jo
	})
}

// SortByTime sorts the segments by start time.
// Earliest first.
func (s Segments) SortByTime() {
	sort.Slice(s, func(i, j int) bool {
		return s[i].Start.Before(s[j].Start)
	})
}

// Median returns the m part median.
// m is clamped to the range 0 -> 1.
func (s Segments) Median(m float64) Segment {
	if len(s) == 0 {
		return Segment{}
	}
	m = math.Round(float64(len(s)) * m)
	m = math.Max(m, 0)
	m = math.Min(m, float64(len(s)-1))
	return s[int(m)]
}

// String returns a human printable version of the time to first byte.
func (t TTFB) String() string {
	if t.Average == 0 {
		return ""
	}
	return fmt.Sprintf("Average: %v, Median: %v, Best: %v, Worst: %v",
		t.Average.Round(time.Millisecond), t.Median.Round(time.Millisecond), t.Best.Round(time.Millisecond), t.Worst.Round(time.Millisecond))
}
