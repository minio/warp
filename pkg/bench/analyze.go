package bench

import (
	"fmt"
	"io"
	"math"
	"sort"
	"time"
)

type SegmentOptions struct {
	Op             string
	From           time.Time
	PerSegDuration time.Duration
}

type Segment struct {
	Op         string
	TotalBytes int64
	FullOps    int
	PartialOps int
	OpsStarted int
	OpsEnded   int
	Errors     int
	Start      time.Time
	EndsBefore time.Time
}

type Segments []Segment

// Total will return the total of active operations.
// See ActiveTimeRange how this is determined.
func (o Operations) Total() Segment {
	start, end := o.ActiveTimeRange()
	return o.Segment(SegmentOptions{
		Op:             "",
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
			Op:         so.Op,
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

// String returns a string representation of the segment
func (s Segment) String() string {
	type ss Segment
	mb, ops := s.SpeedPerSec()
	return fmt.Sprintf("%.02f MB/s, %.02f ops ended/s.\n(details): %+v", mb, ops, ss(s))
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
