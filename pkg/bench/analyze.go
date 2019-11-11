package bench

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"sort"
	"time"
)

type SegmentOptions struct {
	From           time.Time
	PerSegDuration time.Duration
}

type Segment struct {
	OpType     string    `json:"op"`
	TotalBytes int64     `json:"total_bytes"`
	FullOps    int       `json:"full_ops"`
	PartialOps int       `json:"partial_ops"`
	OpsStarted int       `json:"ops_started"`
	OpsEnded   int       `json:"ops_ended"`
	Errors     int       `json:"errors"`
	Start      time.Time `json:"start"`
	EndsBefore time.Time `json:"ends_before"`
}

type Segments []Segment

// Total will return the total of active operations.
// See ActiveTimeRange how this is determined.
func (o Operations) Total() Segment {
	start, end := o.ActiveTimeRange()
	return o.Segment(SegmentOptions{
		From:           start,
		PerSegDuration: end.Sub(start) - 1,
	})[0]
}

// Segment will segment the operations o.
func (o Operations) Segment(so SegmentOptions) Segments {
	start, end := o.ActiveTimeRange()
	if start.After(so.From) {
		so.From = start
	}
	var segments []Segment
	segStart := so.From
	for segStart.Before(end.Add(-so.PerSegDuration)) {
		s := Segment{
			TotalBytes: 0,
			FullOps:    0,
			PartialOps: 0,
			OpsStarted: 0,
			OpsEnded:   0,
			Start:      segStart,
			EndsBefore: segStart.Add(so.PerSegDuration),
		}
		for _, op := range o {
			op.Aggregate(&s)
		}
		segments = append(segments, s)
		segStart = segStart.Add(so.PerSegDuration)
	}
	return segments
}

// SpeedPerSec returns mb/s for the segment and the ops ended per second.
func (s Segment) SpeedPerSec() (mb, ops float64) {
	mb = (float64(s.TotalBytes) / (1024 * 1024)) / (float64(s.EndsBefore.Sub(s.Start)) / (float64(time.Second)))
	ops = float64(s.OpsEnded) / (float64(s.EndsBefore.Sub(s.Start)) / (float64(time.Second)))
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

// Print segments to a supplied writer.
func (s Segments) CSV(w io.Writer) error {
	cw := csv.NewWriter(w)
	cw.Comma = '\t'
	err := cw.Write([]string{"index", "op", "start_time", "end_time", "duration_s", "bytes", "full_ops", "partial_ops", "ops_started", "ops_ended", "errors", "mib_per_sec", "ops_ended_per_sec"})
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
	mb, ops := s.SpeedPerSec()
	return w.Write([]string{
		fmt.Sprint(idx),
		s.OpType,
		fmt.Sprint(s.Start),
		fmt.Sprint(s.EndsBefore),
		fmt.Sprint(float64(s.EndsBefore.Sub(s.Start)) / float64(time.Second)),
		fmt.Sprint(s.TotalBytes),
		fmt.Sprint(s.FullOps),
		fmt.Sprint(s.PartialOps),
		fmt.Sprint(s.OpsStarted),
		fmt.Sprint(s.OpsEnded),
		fmt.Sprint(s.Errors),
		fmt.Sprint(mb),
		fmt.Sprint(ops),
	})
}

// String returns a string representation of the segment
func (s Segment) String() string {
	mb, ops := s.SpeedPerSec()
	return fmt.Sprintf("%v, %.02f MB/s, %.02f ops ended/s. Full Ops: %d, Partial Ops: %d, Started: %v",
		s.EndsBefore.Sub(s.Start), mb, ops, s.FullOps, s.PartialOps, s.Start)
}

// SortByThroughput sorts the segments by throughput.
// Slowest first.
func (s Segments) SortByThroughput() {
	sort.Slice(s, func(i, j int) bool {
		imb, _ := s[i].SpeedPerSec()
		jmb, _ := s[j].SpeedPerSec()
		return imb < jmb
	})
}

// SortByOpsEnded sorts the segments by the number of ops ended in segment.
// Lowest first.
func (s Segments) SortByOpsEnded() {
	sort.Slice(s, func(i, j int) bool {
		_, iops := s[i].SpeedPerSec()
		_, jops := s[j].SpeedPerSec()
		return iops < jops
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
