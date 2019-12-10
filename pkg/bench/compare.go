/*
 * Warp (C) 2019 MinIO, Inc.
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
	"errors"
	"fmt"
	"math"
	"time"
)

// Comparison is a comparison between two benchmarks.
type Comparison struct {
	Op string

	TTFB *TTFBCmp

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
	mbB, _, objsB := c.Before.SpeedPerSec()
	mbA, _, objsA := c.After.SpeedPerSec()

	if c.ThroughputPerSec != 0 {
		speed = fmt.Sprintf("%s%.02f%% (%s%.1f MB/s) throughput, ",
			plusPositiveF(c.ThroughputPerSec), c.ThroughputPerSec,
			plusPositiveF(c.ThroughputPerSec), mbA-mbB,
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
	return fmt.Sprintf("Average: %s%v (%s%.f%%), Median: %s%v (%s%.f%%), Best: %s%v (%s%.f%%), Worst: %s%v (%s%.f%%)",
		plusPositiveD(t.Average),
		t.Average,
		plusPositiveD(t.Average),
		100*(float64(t.After.Average)-float64(t.Before.Average))/float64(t.Before.Average),
		plusPositiveD(t.Median),
		t.Median,
		plusPositiveD(t.Median),
		100*(float64(t.After.Median)-float64(t.Before.Median))/float64(t.Before.Median),
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
func Compare(before, after Operations, analysis time.Duration) (*Comparison, error) {
	var res Comparison
	if before.FirstOpType() != after.FirstOpType() {
		return nil, fmt.Errorf("different operation types. before: %v, after %d", before.FirstOpType(), after.FirstOpType())
	}
	if len(before.Errors()) > 0 || len(after.Errors()) > 0 {
		return nil, fmt.Errorf("errors recorded in benchmark run. before: %v, after %d", len(before.Errors()), len(after.Errors()))
	}
	res.Op = before.FirstOpType()

	segment := func(ops Operations) (Segments, error) {
		segs := ops.Segment(SegmentOptions{
			From:           time.Time{},
			PerSegDuration: analysis,
		})
		if len(segs) <= 1 {
			return nil, errors.New("too few samples")
		}
		totals := ops.Total(true)
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

	beforeTotals, beforeTTFB := before.Total(true), before.TTFB(before.TimeRange())
	afterTotals, afterTTFB := after.Total(true), after.TTFB(after.TimeRange())

	res.Average.Compare(beforeTotals, afterTotals)
	res.TTFB = beforeTTFB.Compare(afterTTFB)
	return &res, nil
}
