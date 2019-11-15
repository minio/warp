/*
 * Warp (C) 2019- MinIO, Inc.
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
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Operations []Operation

type Operation struct {
	OpType    string     `json:"type"`
	ObjPerOp  int        `json:"ops"`
	Start     time.Time  `json:"start"`
	FirstByte *time.Time `json:"first_byte"`
	End       time.Time  `json:"end"`
	Err       string     `json:"err"`
	Size      int64      `json:"size"`
	File      string     `json:"file"`
	Thread    uint16     `json:"thread"`
}

type Collector struct {
	ops   Operations
	rcv   chan Operation
	rcvWg sync.WaitGroup
}

func NewCollector() *Collector {
	r := &Collector{
		ops: make(Operations, 0, 10000),
		rcv: make(chan Operation, 1000),
	}
	r.rcvWg.Add(1)
	go func() {
		defer r.rcvWg.Done()
		for op := range r.rcv {
			r.ops = append(r.ops, op)
		}
	}()
	return r
}

func (c *Collector) Receiver() chan<- Operation {
	return c.rcv
}

func (c *Collector) Close() Operations {
	close(c.rcv)
	c.rcvWg.Wait()
	return c.ops
}

// Aggregate the operation into segment if it belongs there.
func (o Operation) Aggregate(s *Segment) {
	if len(s.OpType) > 0 && o.OpType != s.OpType {
		return
	}
	if o.End.Before(s.Start) {
		return
	}
	if o.Start.After(s.EndsBefore) || o.Start.Equal(s.EndsBefore) {
		return
	}
	startedInSegment := o.Start.After(s.Start) || o.Start.Equal(s.Start)
	endedInSegment := o.End.Before(s.EndsBefore)

	// Correct op, in time range.
	if startedInSegment && endedInSegment {
		if len(o.Err) != 0 {
			s.Errors++
			return
		}
		// We are completely within segment.
		s.TotalBytes += o.Size
		s.FullOps++
		s.OpsStarted++
		s.OpsEnded++
		s.ObjsPerOp = o.ObjPerOp
		s.Objects += float64(o.ObjPerOp)
		return
	}
	// Operation partially within segment.
	s.PartialOps++
	if startedInSegment {
		s.OpsStarted++
		if len(o.Err) != 0 {
			// Errors are only counted in segments they started in.
			return
		}

	}
	if endedInSegment {
		s.OpsEnded++
		if len(o.Err) != 0 {
			s.Errors++
			return
		}
	}

	opDur := o.End.Sub(o.Start)
	partStart := o.Start
	partEnd := o.End
	if !startedInSegment {
		partStart = s.Start
	}
	if !endedInSegment {
		partEnd = s.EndsBefore
	}
	partDur := partEnd.Sub(partStart)
	partSize := o.Size * int64(partDur) / int64(opDur)

	// If we overflow int64, fall back to float64
	if float64(o.Size)*float64(partDur) > math.MaxInt64 {
		partSize = int64(float64(o.Size) * float64(partDur) / float64(opDur))
	}

	// Sanity check
	if partSize < 0 || partSize > o.Size {
		panic(fmt.Errorf("invalid part size: %d (op: %+v seg:%+v)", partSize, o, s))
	}
	s.Objects += float64(o.ObjPerOp) * float64(partDur) / float64(opDur)
	s.TotalBytes += partSize
}

// TTFB returns the time to first byte or 0 if nothing was recorded.
func (o Operation) TTFB() time.Duration {
	if o.FirstByte == nil {
		return 0
	}
	return o.FirstByte.Sub(o.Start)
}

// SortByStartTime will sort the operations by start time.
// Earliest operations first.
func (o Operations) SortByStartTime() {
	sort.Slice(o, func(i, j int) bool {
		return o[i].Start.Before(o[j].Start)
	})
}

// SortByTTFB sorts by time to first byte.
// Smallest first.
func (o Operations) SortByTTFB() {
	sort.Slice(o, func(i, j int) bool {
		a, b := o[i], o[j]
		if a.FirstByte == nil || b.FirstByte == nil {
			return a.Start.Before(b.Start)
		}
		return a.FirstByte.Sub(a.Start) < b.FirstByte.Sub(b.Start)
	})
}

// FilterByHasTTFB returns operations that has or has not time to first byte.
func (o Operations) FilterByHasTTFB(hasTTFB bool) Operations {
	dst := make(Operations, 0, len(o))
	for _, o := range o {
		if (o.FirstByte != nil) == hasTTFB {
			dst = append(dst, o)
		}
	}
	return dst
}

// FilterInsideRange returns operations that are inside the specified time range.
func (o Operations) FilterInsideRange(start, end time.Time) Operations {
	dst := make(Operations, 0, len(o))
	for _, o := range o {
		if o.Start.Before(start) || o.End.After(end) {
			continue
		}
		dst = append(dst, o)
	}
	return dst
}

// FilterByOp returns operations of a specific type.
func (o Operations) FilterByOp(opType string) Operations {
	dst := make(Operations, 0, len(o))
	for _, o := range o {
		if o.OpType == opType {
			dst = append(dst, o)
		}
	}
	return dst
}

// ByOp separates the operations by op.
func (o Operations) ByOp() map[string]Operations {
	dst := make(map[string]Operations, 1)
	for _, o := range o {
		dst[o.OpType] = append(dst[o.OpType], o)
	}
	return dst
}

// FirstOpType returns the type of the first entry empty string if there are no ops.
func (o Operations) FirstOpType() string {
	if len(o) == 0 {
		return ""
	}
	return o[0].OpType
}

// FirstObjPerOp returns the number of objects per operation of the first entry, or 0 if there are no ops.
func (o Operations) FirstObjPerOp() int {
	if len(o) == 0 {
		return 0
	}
	return o[0].ObjPerOp
}

// TimeRange returns the full time range from start of first operation to end of the last.
func (o Operations) TimeRange() (start, end time.Time) {
	if len(o) == 0 {
		return
	}
	start = o[0].Start
	end = o[0].End
	for _, op := range o {
		if op.Start.Before(start) {
			start = op.Start
		}
		if end.Before(op.End) {
			end = op.End
		}
	}
	return
}

// ActiveTimeRange returns the "active" time range.
// All threads must have completed at least one request
// and the last start time of any thread.
// If there is no active time range both values will be the same.
func (o Operations) ActiveTimeRange() (start, end time.Time) {
	if len(o) == 0 {
		return
	}
	t := o.Threads()
	firstEnded := make(map[uint16]time.Time, t)
	lastStarted := make(map[uint16]time.Time, t)
	for _, op := range o {
		ended, ok := firstEnded[op.Thread]
		if !ok || ended.After(op.End) {
			firstEnded[op.Thread] = op.End
		}
		started, ok := lastStarted[op.Thread]
		if !ok || started.Before(op.Start) {
			lastStarted[op.Thread] = op.Start
		}
		// Set ended to largest value
		if end.Before(op.End) {
			end = op.End
		}
	}
	for _, ended := range firstEnded {
		if ended.After(start) {
			start = ended
		}
	}
	for _, started := range lastStarted {
		if end.After(started) {
			end = started
		}
	}
	if start.After(end) {
		return start, start
	}
	return
}

// Threads returns the number of threads found.
func (o Operations) Threads() int {
	if len(o) == 0 {
		return 0
	}
	maxT := uint16(0)
	for _, op := range o {
		if op.Thread > maxT {
			maxT = op.Thread
		}
	}
	return int(maxT)
}

// Errors returns the errors found.
func (o Operations) Errors() []string {
	if len(o) == 0 {
		return nil
	}
	errs := []string{}
	for _, op := range o {
		if len(op.Err) != 0 {
			errs = append(errs, op.Err)
		}
	}
	return errs
}

// CSV will write the operations to w as CSV.
func (o Operations) CSV(w io.Writer) error {
	bw := bufio.NewWriter(w)
	_, err := bw.WriteString("idx\tthread\top\tn_objects\tbytes\tfile\terror\tstart\tfirst_byte\tend\tduration_ns\n")
	if err != nil {
		return err
	}
	for i, op := range o {
		var ttfb string
		if op.FirstByte != nil {
			ttfb = op.FirstByte.Format(time.RFC3339Nano)
		}
		_, err := fmt.Fprintf(bw, "%d\t%d\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%d\n", i, op.Thread, op.OpType, op.ObjPerOp, op.Size, op.File, csvEscapeString(op.Err), op.Start.Format(time.RFC3339Nano), ttfb, op.End.Format(time.RFC3339Nano), op.End.Sub(op.Start)/time.Nanosecond)
		if err != nil {
			return err
		}
	}
	return bw.Flush()
}

// OperationsFromCSV will load operations from CSV.
func OperationsFromCSV(r io.Reader) (Operations, error) {
	var ops Operations
	cr := csv.NewReader(r)
	cr.Comma = '\t'
	cr.ReuseRecord = true
	header, err := cr.Read()
	if err != nil {
		return nil, err
	}
	fieldIdx := make(map[string]int)
	for i, s := range header {
		fieldIdx[s] = i
	}
	for {
		values, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(values) == 0 {
			continue
		}
		start, err := time.Parse(time.RFC3339Nano, values[fieldIdx["start"]])
		if err != nil {
			return nil, err
		}
		var ttfb *time.Time
		if fb := values[fieldIdx["first_byte"]]; fb != "" {
			t, err := time.Parse(time.RFC3339Nano, fb)
			if err != nil {
				return nil, err
			}
			ttfb = &t
		}
		end, err := time.Parse(time.RFC3339Nano, values[fieldIdx["end"]])
		if err != nil {
			return nil, err
		}
		size, err := strconv.ParseInt(values[fieldIdx["bytes"]], 10, 64)
		if err != nil {
			return nil, err
		}
		thread, err := strconv.ParseUint(values[fieldIdx["thread"]], 10, 16)
		if err != nil {
			return nil, err
		}
		objs, err := strconv.ParseInt(values[fieldIdx["n_objects"]], 10, 64)
		if err != nil {
			return nil, err
		}
		ops = append(ops, Operation{
			OpType:    values[fieldIdx["op"]],
			ObjPerOp:  int(objs),
			Start:     start,
			FirstByte: ttfb,
			End:       end,
			Err:       values[fieldIdx["error"]],
			Size:      size,
			File:      values[fieldIdx["file"]],
			Thread:    uint16(thread),
		})
	}
	return ops, nil
}
