/*
 * Warp (C) 2019 MinIO, Inc.
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
	"errors"
	"fmt"
	"math"
	"time"
)

// Comparison is a comparison between two benchmarks.
type Comparison struct {
	Op string

	TTFB *TTFBCmp
	Reqs CmpReqs

	Average CmpSegment
	Fastest CmpSegment
	Median  CmpSegment
	Slowest CmpSegment
}

// CmpSegment is s comparisons between two segments.
type CmpSegment struct {
	Before, After    *Segment
	ThroughputPerSec float64
	ObjPerSec        float64
	OpsEndedPerSec   float64
}

type CmpReqs struct {
	CmpRequests
	Before, After CmpRequests
}

func (c *CmpReqs) Compare(before, after Operations) {
	c.Before.fill(before)
	c.After.fill(after)
	a := c.After
	b := c.Before
	c.CmpRequests =
		CmpRequests{
			Average: a.Average - b.Average,
			Worst:   a.Worst - b.Worst,
			Best:    a.Best - b.Best,
			Median:  a.Median - b.Median,
			P25:     a.P25 - b.P25,
			P75:     a.P75 - b.P75,
			P90:     a.P90 - b.P90,
			P99:     a.P99 - b.P99,
		}
}

type CmpRequests struct {
	AvgObjSize int64
	Requests   int
	Average    time.Duration
	Best       time.Duration
	P25        time.Duration
	Median     time.Duration
	P75        time.Duration
	P90        time.Duration
	P99        time.Duration
	Worst      time.Duration
}

func (c *CmpRequests) fill(ops Operations) {
	ops.SortByDuration()
	c.Requests = len(ops)
	c.AvgObjSize = ops.AvgSize()
	c.Average = ops.AvgDuration()
	c.Best = ops.Median(0).Duration()
	c.P25 = ops.Median(0.25).Duration()
	c.Median = ops.Median(0.5).Duration()
	c.P75 = ops.Median(0.75).Duration()
	c.P90 = ops.Median(0.9).Duration()
	c.P99 = ops.Median(0.99).Duration()
	c.Worst = ops.Median(1).Duration()
}

// String returns a human readable representation of the TTFB comparison.
func (c *CmpReqs) String() string {
	if c == nil {
		return ""
	}
	return fmt.Sprintf("Avg: %s%v (%s%.f%%), P50: %s%v (%s%.f%%), P99: %s%v (%s%.f%%), Best: %s%v (%s%.f%%), Worst: %s%v (%s%.f%%)",
		plusPositiveD(c.Average),
		c.Average.Round(time.Millisecond/20),
		plusPositiveD(c.Average),
		100*(float64(c.After.Average)-float64(c.Before.Average))/float64(c.Before.Average),
		plusPositiveD(c.Median),
		c.Median,
		plusPositiveD(c.Median),
		100*(float64(c.After.Median)-float64(c.Before.Median))/float64(c.Before.Median),
		plusPositiveD(c.P99),
		c.P99,
		plusPositiveD(c.P99),
		100*(float64(c.After.P99)-float64(c.Before.P99))/float64(c.Before.P99),
		plusPositiveD(c.Best),
		c.Best,
		plusPositiveD(c.Best),
		100*(float64(c.After.Best)-float64(c.Before.Best))/float64(c.Before.Best),
		plusPositiveD(c.Worst),
		c.Worst,
		plusPositiveD(c.Worst),
		100*(float64(c.After.Worst)-float64(c.Before.Worst))/float64(c.Before.Worst),
	)
}

// Compare sets c to a comparison between before and after.
func (c *CmpSegment) Compare(before, after Segment) {
	c.Before = &before
	c.After = &after
	mbB, opsB, objsB := before.SpeedPerSec()
	mbA, opsA, objsA := after.SpeedPerSec()
	c.ObjPerSec = 100 * (objsA - objsB) / objsB
	c.OpsEndedPerSec = 100 * (opsA - opsB) / opsB
	if mbB > 0 {
		c.ThroughputPerSec = 100 * (mbA - mbB) / mbB
	} else {
		c.ThroughputPerSec = 0
	}
}

// String returns a string representation of the segment comparison.
func (c CmpSegment) String() string {
	speed := ""
	mibB, _, objsB := c.Before.SpeedPerSec()
	mibA, _, objsA := c.After.SpeedPerSec()

	if c.ThroughputPerSec != 0 {
		speed = fmt.Sprintf("%s%.02f%% (%s%.1f MiB/s) throughput, ",
			plusPositiveF(c.ThroughputPerSec), c.ThroughputPerSec,
			plusPositiveF(c.ThroughputPerSec), mibA-mibB,
		)
	}
	return fmt.Sprintf("%s%s%.02f%% (%s%.1f) obj/s",
		speed, plusPositiveF(c.ObjPerSec), c.ObjPerSec,
		plusPositiveF(objsA-objsB), objsA-objsB,
	)
}

func plusPositiveF(f float64) string {
	switch {
	case f > 0 && !math.IsInf(f, 1):
		return "+"
	default:
		return ""
	}
}

// TTFBCmp is a comparison between two TTFB runs.
type TTFBCmp struct {
	TTFB
	Before, After TTFB
}

// Compare will set t to the difference between before and after.
func (t TTFB) Compare(after TTFB) *TTFBCmp {
	if t.Average == 0 {
		return nil
	}
	return &TTFBCmp{
		TTFB: TTFB{
			Average: after.Average - t.Average,
			Worst:   after.Worst - t.Worst,
			Best:    after.Best - t.Best,
			Median:  after.Median - t.Median,
			P25:     after.P25 - t.P25,
			P75:     after.P75 - t.P75,
			P90:     after.P90 - t.P90,
			P99:     after.P99 - t.P99,
		},
		Before: t,
		After:  after,
	}
}

// String returns a human readable representation of the TTFB comparison.
func (t *TTFBCmp) String() string {
	if t == nil {
		return ""
	}
	return fmt.Sprintf("Avg: %s%v (%s%.f%%), P50: %s%v (%s%.f%%), P99: %s%v (%s%.f%%), Best: %s%v (%s%.f%%), Worst: %s%v (%s%.f%%)",
		plusPositiveD(t.Average),
		t.Average.Round(time.Millisecond/20),
		plusPositiveD(t.Average),
		100*(float64(t.After.Average)-float64(t.Before.Average))/float64(t.Before.Average),
		plusPositiveD(t.Median),
		t.Median,
		plusPositiveD(t.Median),
		100*(float64(t.After.Median)-float64(t.Before.Median))/float64(t.Before.Median),
		plusPositiveD(t.P99),
		t.P99,
		plusPositiveD(t.P99),
		100*(float64(t.After.P99)-float64(t.Before.P99))/float64(t.Before.P99),
		plusPositiveD(t.Best),
		t.Best,
		plusPositiveD(t.Best),
		100*(float64(t.After.Best)-float64(t.Before.Best))/float64(t.Before.Best),
		plusPositiveD(t.Worst),
		t.Worst,
		plusPositiveD(t.Worst),
		100*(float64(t.After.Worst)-float64(t.Before.Worst))/float64(t.Before.Worst),
	)
}

func plusPositiveD(d time.Duration) string {
	switch {
	case d > 0:
		return "+"
	default:
		return ""
	}
}

// Compare compares operations of a single operation type.
func Compare(before, after Operations, analysis time.Duration, allThreads bool) (*Comparison, error) {
	var res Comparison
	if before.FirstOpType() != after.FirstOpType() {
		return nil, fmt.Errorf("different operation types. before: %v, after %v", before.FirstOpType(), after.FirstOpType())
	}
	if analysis <= 0 {
		return nil, fmt.Errorf("invalid analysis duration: %v", analysis)
	}
	if len(before.Errors()) > 0 || len(after.Errors()) > 0 {
		return nil, fmt.Errorf("errors recorded in benchmark run. before: %v, after %d", len(before.Errors()), len(after.Errors()))
	}
	res.Op = before.FirstOpType()
	segment := func(ops Operations) (Segments, error) {
		segs := ops.Segment(SegmentOptions{
			From:           time.Time{},
			PerSegDuration: analysis,
			AllThreads:     allThreads,
		})
		if len(segs) <= 1 {
			return nil, errors.New("too few samples")
		}
		totals := ops.Total(allThreads)
		if totals.TotalBytes > 0 {
			segs.SortByThroughput()
		} else {
			segs.SortByObjsPerSec()
		}
		return segs, nil
	}
	bs, err := segment(before)
	if err != nil {
		return nil, fmt.Errorf("segmenting before: %w", err)
	}
	as, err := segment(after)
	if err != nil {
		return nil, fmt.Errorf("segmenting after: %w", err)
	}

	res.Median.Compare(bs.Median(0.5), as.Median(0.5))
	res.Slowest.Compare(bs.Median(0.0), as.Median(0.0))
	res.Fastest.Compare(bs.Median(1), as.Median(1))

	beforeTotals, beforeTTFB := before.Total(allThreads), before.TTFB(before.TimeRange())
	afterTotals, afterTTFB := after.Total(allThreads), after.TTFB(after.TimeRange())
	res.Reqs.Compare(before, after)

	res.Average.Compare(beforeTotals, afterTotals)
	res.TTFB = beforeTTFB.Compare(afterTTFB)
	return &res, nil
}
