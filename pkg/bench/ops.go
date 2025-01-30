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
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
)

type Operations []Operation

type Operation struct {
	Start      time.Time  `json:"start"`
	End        time.Time  `json:"end"`
	FirstByte  *time.Time `json:"first_byte"`
	OpType     string     `json:"type"`
	Err        string     `json:"err"`
	File       string     `json:"file,omitempty"`
	ClientID   string     `json:"client_id"`
	Endpoint   string     `json:"endpoint"`
	ObjPerOp   int        `json:"ops"`
	Size       int64      `json:"size"`
	Thread     uint16     `json:"thread"`
	Categories Categories `json:"cat"`
}

// Duration returns the duration o.End-o.Start
func (o Operation) Duration() time.Duration {
	return o.End.Sub(o.Start)
}

// Throughput is the throughput as bytes/second.
type Throughput float64

func (t Throughput) String() string {
	if t < 2<<10 {
		return fmt.Sprintf("%.1fB/s", float64(t))
	}
	if t < 2<<20 {
		return fmt.Sprintf("%.1fKiB/s", float64(t)/(1<<10))
	}
	if t < 10<<30 {
		return fmt.Sprintf("%.1fMiB/s", float64(t)/(1<<20))
	}
	if t < 10<<40 {
		return fmt.Sprintf("%.2fGiB/s", float64(t)/(1<<30))
	}
	return fmt.Sprintf("%.2fTiB/s", float64(t)/(1<<40))
}

// Float returns a rounded (to 0.1) float value of the throughput.
func (t Throughput) Float() float64 {
	return math.Round(float64(t)*10) / 10
}

func (o Operation) BytesPerSec() Throughput {
	if o.Size == 0 {
		return 0
	}
	d := o.Duration()
	if d <= 0 {
		return 0
	}
	return Throughput(o.Size*int64(time.Second)) / Throughput(d)
}

func (o Operation) String() string {
	return fmt.Sprintf("%s %s/(bucket)/%s, %v->%v, Size: %d, Error: %v", o.OpType, o.Endpoint, o.File, o.Start, o.End, o.Size, o.Err)
}

// Aggregate the operation into segment if it belongs there.
// Done returns true if operation is starting after segment ended.
func (o Operation) Aggregate(s *Segment) (done bool) {
	if o.Start.After(s.EndsBefore) || o.Start.Equal(s.EndsBefore) {
		return true
	}
	done = false
	if len(s.OpType) > 0 && o.OpType != s.OpType {
		return
	}
	if o.End.Before(s.Start) {
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
		s.ReqAvg += float64(o.End.Sub(o.Start)) / float64(time.Millisecond)
		return
	}
	// Operation partially within segment.
	s.PartialOps++
	if startedInSegment {
		s.OpsStarted++
		if len(o.Err) != 0 {
			// Errors are only counted in segments they ends in.
			return
		}

	}
	if endedInSegment {
		s.OpsEnded++
		if len(o.Err) != 0 {
			s.Errors++
			return
		}
		s.ReqAvg += float64(o.End.Sub(o.Start)) / float64(time.Millisecond)
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
	return done
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
	if sort.SliceIsSorted(o, func(i, j int) bool {
		return o[i].Start.Before(o[j].Start)
	}) {
		return
	}
	sort.Slice(o, func(i, j int) bool {
		return o[i].Start.Before(o[j].Start)
	})
}

// SortByEndTime will sort the operations by end time.
// Earliest operations first.
func (o Operations) SortByEndTime() {
	if sort.SliceIsSorted(o, func(i, j int) bool {
		return o[i].End.Before(o[j].End)
	}) {
		return
	}
	sort.Slice(o, func(i, j int) bool {
		return o[i].End.Before(o[j].End)
	})
}

// SortByEndpoint will sort the operations by end-point.
// Earliest operations first.
func (o Operations) SortByEndpoint() {
	if sort.SliceIsSorted(o, func(i, j int) bool {
		if o[i].Endpoint == o[j].Endpoint {
			return o[i].Start.Before(o[j].Start)
		}
		return o[i].Endpoint < o[j].Endpoint
	}) {
		return
	}
	sort.Slice(o, func(i, j int) bool {
		if o[i].Endpoint == o[j].Endpoint {
			return o[i].Start.Before(o[j].Start)
		}
		return o[i].Endpoint < o[j].Endpoint
	})
}

// SortByClient will sort the operations by client.
// Earliest operations first.
func (o Operations) SortByClient() {
	if sort.SliceIsSorted(o, func(i, j int) bool {
		if o[i].ClientID == o[j].ClientID {
			return o[i].Start.Before(o[j].Start)
		}
		return o[i].ClientID < o[j].ClientID
	}) {
		return
	}
	sort.Slice(o, func(i, j int) bool {
		if o[i].ClientID == o[j].ClientID {
			return o[i].Start.Before(o[j].Start)
		}
		return o[i].ClientID < o[j].ClientID
	})
}

// SortByOpType will sort the operations by operation type.
// Earliest operations first.
func (o Operations) SortByOpType() {
	if sort.SliceIsSorted(o, func(i, j int) bool {
		if o[i].OpType == o[j].OpType {
			return o[i].Start.Before(o[j].Start)
		}
		return o[i].OpType < o[j].OpType
	}) {
		return
	}
	sort.Slice(o, func(i, j int) bool {
		if o[i].OpType == o[j].OpType {
			return o[i].Start.Before(o[j].Start)
		}
		return o[i].OpType < o[j].OpType
	})
}

// SortByDuration will sort the operations by duration taken to complete.
// Fastest operations first.
func (o Operations) SortByDuration() {
	sort.Slice(o, func(i, j int) bool {
		a, b := o[i].End.UnixNano()-o[i].Start.UnixNano(), o[j].End.UnixNano()-o[j].Start.UnixNano()
		return a < b
	})
}

// SortByThroughput will sort the operations by throughput.
// Fastest operations first.
func (o Operations) SortByThroughput() {
	sort.Slice(o, func(i, j int) bool {
		a, b := &o[i], &o[j]
		aDur, bDur := a.End.UnixNano()-a.Start.UnixNano(), b.End.UnixNano()-b.Start.UnixNano()
		if a.Size == 0 || b.Size == 0 {
			return aDur < bDur
		}
		return float64(a.Size)/float64(aDur) > float64(b.Size)/float64(bDur)
	})
}

// SortByThroughputNonZero will sort the operations by throughput.
// Fastest operations first.
func (o Operations) SortByThroughputNonZero() Operations {
	o.SortByThroughput()
	for i, op := range o {
		if op.Duration() > 0 {
			return o[i:]
		}
	}
	return nil
}

// Median returns the m part median of the assumed sorted list of operations.
// m is clamped to the range 0 -> 1.
func (o Operations) Median(m float64) Operation {
	if len(o) == 0 {
		return Operation{}
	}
	m = math.Round(float64(len(o)) * m)
	m = math.Max(m, 0)
	m = math.Min(m, float64(len(o)-1)+1e-10)
	return o[int(m)]
}

// SortByTTFB sorts by time to first byte.
// Smallest first.
func (o Operations) SortByTTFB() {
	sort.Slice(o, func(i, j int) bool {
		a, b := &o[i], &o[j]
		if a.FirstByte == nil || b.FirstByte == nil {
			return a.Start.Before(b.Start)
		}
		return a.FirstByte.UnixNano()-a.Start.UnixNano() < b.FirstByte.UnixNano()-b.Start.UnixNano()
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
// Operations starting before start or ending after end are discarded.
func (o Operations) FilterInsideRange(start, end time.Time) Operations {
	dst := make(Operations, 0, len(o))
	for i := range o {
		op := &o[i]
		if op.Start.Before(start) || op.End.After(end) {
			continue
		}
		dst = append(dst, *op)
	}
	return dst
}

// FilterByOp returns operations of a specific type.
func (o Operations) FilterByOp(opType string) Operations {
	dst := make(Operations, 0, len(o))
	for _, o := range o {
		if o.OpType == opType || opType == "" {
			dst = append(dst, o)
		}
	}
	return dst
}

// SetClientID will set the client ID for all operations.
func (o Operations) SetClientID(id string) {
	for i := range o {
		o[i].ClientID = id
	}
}

// FilterByEndpoint returns operations run against a specific endpoint.
// Always returns a copy.
func (o Operations) FilterByEndpoint(endpoint string) Operations {
	dst := make(Operations, 0, len(o))
	for _, o := range o {
		if o.Endpoint == endpoint {
			dst = append(dst, o)
		}
	}
	return dst
}

// SortSplitByEndpoint will sort operations by endpoint and split by host.
func (o Operations) SortSplitByEndpoint() map[string]Operations {
	eps := o.Endpoints()
	o.SortByEndpoint()
	dst := make(map[string]Operations, len(eps))
	ep := ""
	start := 0
	for i, op := range o {
		if op.Endpoint == ep {
			continue
		}
		if ep != "" {
			dst[ep] = o[start:i]
		}
		ep = op.Endpoint
		start = i
	}
	if ep != "" {
		dst[ep] = o[start:]
	}

	return dst
}

// SortSplitByClient will sort operations by endpoint and split by host.
func (o Operations) SortSplitByClient(prefix string) map[string]Operations {
	clients := o.Clients()
	o.SortByClient()
	dst := make(map[string]Operations, clients)
	cl := ""
	start := 0
	for i, op := range o {
		if op.ClientID == cl {
			continue
		}
		if cl != "" {
			dst[cl] = o[start:i]
		}
		cl = op.ClientID
		start = i
	}
	if cl != "" {
		dst[cl] = o[start:]
	}
	if prefix != "" {
		dst2 := make(map[string]Operations, len(dst))
		for k, v := range dst {
			dst2[prefix+k] = v
		}
		return dst2
	}

	return dst
}

// SortSplitByOpType will sort operations by op + start time and split by op.
func (o Operations) SortSplitByOpType() map[string]Operations {
	o.SortByOpType()
	dst := make(map[string]Operations, 5)
	lastOp := ""
	start := 0
	for i, op := range o {
		if op.OpType == lastOp {
			continue
		}
		if lastOp != "" {
			dst[lastOp] = o[start:i]
		}
		lastOp = op.OpType
		start = i
	}
	if lastOp != "" {
		dst[lastOp] = o[start:]
	}

	return dst
}

// OpTypes returns a list of the operation types in the order they appear
// if not overlapping or in alphabetical order if mixed.
func (o Operations) OpTypes() []string {
	tmp := make(map[string]struct{}, 5)
	dst := make([]string, 0, 5)
	for _, o := range o {
		if _, ok := tmp[o.OpType]; !ok {
			dst = append(dst, o.OpType)
		}
		tmp[o.OpType] = struct{}{}
	}
	if o.isMixed(dst) {
		sort.Strings(dst)
	}
	return dst
}

// IsMixed returns true if different operation types are overlapping.
func (o Operations) IsMixed() bool {
	return o.isMixed(o.OpTypes())
}

// IsMultiTouch returns true if the same files are touched multiple times.
func (o Operations) IsMultiTouch() bool {
	seen := make(map[string]struct{}, len(o))
	for _, op := range o {
		if _, ok := seen[op.File]; ok {
			return true
		}
		seen[op.File] = struct{}{}
	}
	return false
}

// HasError returns whether one or more operations failed.
func (o Operations) HasError() bool {
	if len(o) == 0 {
		return false
	}
	for _, op := range o {
		if len(op.Err) > 0 {
			return true
		}
	}
	return false
}

// isMixed returns true if operation types are overlapping.
func (o Operations) isMixed(types []string) bool {
	if len(types) <= 1 {
		return false
	}
	for _, a := range types {
		aStart, aEnd := o.FilterByOp(a).TimeRange()
		for _, b := range types {
			if a == b {
				continue
			}
			bStart, bEnd := o.FilterByOp(b).TimeRange()
			firstEnd, secondStart := aEnd, bEnd

			if bStart.Before(aStart) {
				firstEnd, secondStart = bEnd, aStart
			}

			if firstEnd.After(secondStart) {
				return true
			}
		}
	}
	return false
}

// ByEndpoint separates the operations by endpoint.
func (o Operations) ByEndpoint() map[string]Operations {
	dst := make(map[string]Operations, 1)
	for _, o := range o {
		dst[o.Endpoint] = append(dst[o.Endpoint], o)
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

// FirstObjSize returns the size of the first entry, 0 if there are no ops.
func (o Operations) FirstObjSize() int64 {
	if len(o) == 0 {
		return 0
	}
	return o[0].Size
}

// FirstObjPerOp returns the number of objects per operation of the first entry, or 0 if there are no ops.
func (o Operations) FirstObjPerOp() int {
	if len(o) == 0 {
		return 0
	}
	return o[0].ObjPerOp
}

// MultipleSizes returns whether there are multiple operation sizes.
func (o Operations) MultipleSizes() bool {
	if len(o) == 0 {
		return false
	}
	sz := o[0].Size
	for _, op := range o {
		if len(op.Err) == 0 && op.Size != sz {
			return true
		}
	}
	return false
}

// MinMaxSize returns the minimum and maximum operation sizes.
func (o Operations) MinMaxSize() (minSize, maxSize int64) {
	if len(o) == 0 {
		return 0, 0
	}

	minSize = o[0].Size
	maxSize = o[0].Size
	for _, op := range o {
		if op.Size < minSize {
			minSize = op.Size
		}
		if op.Size > maxSize {
			maxSize = op.Size
		}
	}
	return minSize, maxSize
}

// AvgSize returns the average operation size.
func (o Operations) AvgSize() int64 {
	if len(o) == 0 {
		return 0
	}
	var total int64
	for _, op := range o {
		total += op.Size
	}
	return total / int64(len(o))
}

// AvgDuration returns the average operation duration.
func (o Operations) AvgDuration() time.Duration {
	if len(o) == 0 {
		return 0
	}
	var total time.Duration
	for _, op := range o {
		total += op.Duration()
	}
	return total / time.Duration(len(o))
}

// StdDev returns the standard deviation.
func (o Operations) StdDev() time.Duration {
	if len(o) <= 1 {
		return 0
	}
	avg := o.AvgDuration()
	var total float64
	for _, op := range o {
		delta := float64(avg - op.Duration())
		total += delta * delta
	}
	return time.Duration(math.Sqrt(total / float64(len(o)-1)))
}

// SizeSegment is a size segment.
type SizeSegment struct {
	Ops           Operations
	Smallest      int64
	SmallestLog10 int
	Biggest       int64
	BiggestLog10  int
}

// SizeString returns the size as a string.
func (s SizeSegment) SizeString() string {
	a, b := s.SizesString()
	return fmt.Sprint(a, " -> ", b)
}

// SizesString returns the lower and upper limit as strings.
func (s SizeSegment) SizesString() (lo, hi string) {
	if s.SmallestLog10 <= 0 || s.BiggestLog10 <= 0 {
		return humanize.IBytes(uint64(s.Smallest)), humanize.IBytes(uint64(s.Biggest))
	}
	return log10ToSize[s.SmallestLog10], log10ToSize[s.BiggestLog10]
}

var log10ToSize = map[int]string{
	0:  "",
	1:  "10B",
	2:  "100B",
	3:  "1KiB",
	4:  "10KiB",
	5:  "100KiB",
	6:  "1MiB",
	7:  "10MiB",
	8:  "100MiB",
	9:  "1GiB",
	10: "10GiB",
	11: "100GiB",
	12: "1TiB",
}

var log10ToLog2Size = map[int]int64{
	0:  1,
	1:  10,
	2:  100,
	3:  1 << 10,
	4:  10 << 10,
	5:  100 << 10,
	6:  1 << 20,
	7:  10 << 20,
	8:  100 << 20,
	9:  1 << 30,
	10: 10 << 30,
	11: 100 << 30,
	12: 1 << 40,
}

func (o Operations) SingleSizeSegment() SizeSegment {
	minSize, maxSize := o.MinMaxSize()
	var minL10, maxL10 int
	for minSize > log10ToLog2Size[minL10+1] {
		minL10++
	}
	for maxSize >= log10ToLog2Size[maxL10] {
		maxL10++
	}
	return SizeSegment{
		Smallest:      minSize,
		SmallestLog10: minL10,
		Biggest:       maxSize,
		BiggestLog10:  maxL10,
		Ops:           o,
	}
}

// SplitSizes will return log10 separated data.
// Specify the share of requests that must be in a segment to return it.
func (o Operations) SplitSizes(minShare float64) []SizeSegment {
	if !o.MultipleSizes() {
		return []SizeSegment{o.SingleSizeSegment()}
	}
	// FIXME: This allocs like crazy...
	var res []SizeSegment
	minSz, maxSz := o.MinMaxSize()
	if minSz == 0 {
		minSz = 1
	}
	minLog := int(math.Log10(float64(minSz)))
	maxLog := int(math.Log10(float64(maxSz)))
	cLog := minLog
	wantN := int(float64(len(o)) * minShare)
	seg := SizeSegment{
		Smallest:      log10ToLog2Size[cLog],
		SmallestLog10: cLog,
		Biggest:       0,
		Ops:           make(Operations, 0, wantN*2),
	}
	for cLog <= maxLog {
		cLog++
		seg.Biggest = log10ToLog2Size[cLog]
		seg.BiggestLog10 = cLog
		for _, op := range o {
			if op.Size >= seg.Smallest && op.Size < seg.Biggest {
				seg.Ops = append(seg.Ops, op)
			}
		}
		if len(seg.Ops) >= wantN {
			res = append(res, seg)
			seg = SizeSegment{
				Smallest:      log10ToLog2Size[cLog],
				SmallestLog10: cLog,
				Biggest:       0,
				Ops:           make(Operations, 0, wantN*2),
			}
		}
	}

	return res
}

// Duration returns the full duration from start of first operation to end of the last.
func (o Operations) Duration() time.Duration {
	start, end := o.TimeRange()
	return end.Sub(start)
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
func (o Operations) ActiveTimeRange(allThreads bool) (start, end time.Time) {
	if len(o) == 0 {
		return
	}
	// Only discard one.
	if !allThreads {
		startF := o[0].Start
		endF := o[0].End
		for _, op := range o {
			if op.End.Before(startF) {
				startF = op.End
			}
			if endF.Before(op.Start) {
				endF = op.Start
			}
		}
		start = endF
		end = startF
		for _, op := range o {
			if op.Start.After(startF) && op.Start.Before(start) {
				start = op.Start
			}
			if op.End.Before(endF) && op.End.After(end) {
				end = op.End
			}
		}
		if start.After(end) {
			return start, start
		}

		return
	}
	threads := o.Threads()
	firstEnded := make(map[uint16]time.Time, threads)
	lastStarted := make(map[uint16]time.Time, threads)
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
	return int(maxT) + 1
}

// OffsetThreads adds an offset to all thread ids and
// returns the next thread number.
func (o Operations) OffsetThreads(n uint16) uint16 {
	if len(o) == 0 {
		return 0
	}
	maxT := uint16(0)
	for i, op := range o {
		op.Thread += n
		if op.Thread > maxT {
			maxT = op.Thread
		}
		o[i] = op
	}
	return maxT + 1
}

// Hosts returns the number of servers.
func (o Operations) Hosts() int {
	if len(o) == 0 {
		return 0
	}
	endpoints := make(map[string]struct{}, 1)
	for _, op := range o {
		endpoints[op.Endpoint] = struct{}{}
	}
	return len(endpoints)
}

// Clients returns the number of clients.
func (o Operations) Clients() int {
	if len(o) == 0 {
		return 0
	}
	clients := make(map[string]struct{}, 10)
	for _, op := range o {
		clients[op.ClientID] = struct{}{}
	}
	return len(clients)
}

// Endpoints returns the endpoints as a sorted slice.
func (o Operations) Endpoints() []string {
	if len(o) == 0 {
		return nil
	}
	endpoints := make(map[string]struct{}, 1)
	for _, op := range o {
		endpoints[op.Endpoint] = struct{}{}
	}
	dst := make([]string, 0, len(endpoints))
	for k := range endpoints {
		dst = append(dst, k)
	}
	sort.Strings(dst)
	return dst
}

func (o Operations) ClientIDs(prefix string) []string {
	if len(o) == 0 {
		return nil
	}
	found := make(map[string]struct{}, 1)
	for _, op := range o {
		found[op.ClientID] = struct{}{}
	}
	dst := make([]string, 0, len(found))
	for k := range found {
		dst = append(dst, prefix+k)
	}
	sort.Strings(dst)
	return dst
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

// NErrors returns the number of errors found.
func (o Operations) NErrors() int {
	var n int
	for _, op := range o {
		if len(op.Err) != 0 {
			n++
		}
	}
	return n
}

// FilterSuccessful returns the successful requests.
func (o Operations) FilterSuccessful() Operations {
	if len(o) == 0 {
		return nil
	}
	failed := 0
	for _, op := range o {
		if len(op.Err) > 0 {
			failed++
		}
	}
	if failed == 0 {
		return o
	}
	if failed == len(o) {
		return nil
	}

	ok := make(Operations, 0, len(o)-failed)
	for _, op := range o {
		if len(op.Err) == 0 {
			ok = append(ok, op)
		}
	}
	return ok
}

// Clone the operations.
func (o Operations) Clone() Operations {
	c := make(Operations, len(o))
	copy(c, o)
	return c
}

// FilterFirst returns the first operation on any file.
func (o Operations) FilterFirst() Operations {
	if len(o) == 0 {
		return nil
	}
	o.SortByStartTime()
	ok := make(Operations, 0, 1000)
	seen := make(map[string]struct{}, len(o))
	for _, op := range o {
		if _, ok := seen[op.File]; ok {
			continue
		}
		seen[op.File] = struct{}{}
		ok = append(ok, op)
	}

	return ok
}

// FilterLast returns the last operation on any file.
func (o Operations) FilterLast() Operations {
	if len(o) == 0 {
		return nil
	}
	o.SortByStartTime()
	ok := make(Operations, 0, 1000)
	seen := make(map[string]struct{}, len(o))
	for i := len(o) - 1; i >= 0; i-- {
		op := o[i]
		if _, ok := seen[op.File]; ok {
			continue
		}
		seen[op.File] = struct{}{}
		ok = append(ok, op)
	}

	return ok
}

// FilterErrors returns all operations with errors.
func (o Operations) FilterErrors() Operations {
	if len(o) == 0 {
		return nil
	}
	errs := Operations{}
	for _, op := range o {
		if len(op.Err) != 0 {
			errs = append(errs, op)
		}
	}
	return errs
}

// CSV will write the operations to w as CSV.
// The comment, if any, is written at the end of the file, each line prefixed with '# '.
func (o Operations) CSV(w io.Writer, comment string) error {
	bw := bufio.NewWriter(w)
	_, err := bw.WriteString("idx\tthread\top\tclient_id\tn_objects\tbytes\tendpoint\tfile\terror\tstart\tfirst_byte\tend\tduration_ns\tcat\n")
	if err != nil {
		return err
	}

	for i, op := range o {
		if err := op.WriteCSV(bw, i); err != nil {
			return err
		}
	}
	if len(comment) > 0 {
		lines := strings.Split(comment, "\n")
		for _, txt := range lines {
			_, err := bw.WriteString("# " + txt + "\n")
			if err != nil {
				return err
			}
		}
	}

	return bw.Flush()
}

func (o Operation) WriteCSV(w io.Writer, i int) error {
	var ttfb string
	if o.FirstByte != nil {
		ttfb = o.FirstByte.Format(time.RFC3339Nano)
	}
	_, err := fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\n", i, o.Thread, o.OpType, o.ClientID, o.ObjPerOp, o.Size, csvEscapeString(o.Endpoint), o.File, csvEscapeString(o.Err), o.Start.Format(time.RFC3339Nano), ttfb, o.End.Format(time.RFC3339Nano), o.End.Sub(o.Start)/time.Nanosecond, o.Categories)
	return err
}

// OperationsFromCSV will load operations from CSV.
func OperationsFromCSV(r io.Reader, analyzeOnly bool, offset, limit int, log func(msg string, v ...interface{})) (Operations, error) {
	opCh := make(chan Operation, 1000)
	var ops Operations
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for op := range opCh {
			ops = append(ops, op)
		}
	}()
	if err := StreamOperationsFromCSV(r, analyzeOnly, offset, limit, log, opCh); err != nil {
		return nil, err
	}
	wg.Wait()
	return ops, nil
}

// StreamOperationsFromCSV will load operations from CSV.
func StreamOperationsFromCSV(r io.Reader, analyzeOnly bool, offset, limit int, log func(msg string, v ...interface{}), out chan<- Operation) error {
	defer close(out)
	cr := csv.NewReader(r)
	cr.Comma = '\t'
	cr.ReuseRecord = true
	cr.Comment = '#'
	header, err := cr.Read()
	if err != nil {
		return err
	}
	fieldIdx := make(map[string]int)
	for i, s := range header {
		fieldIdx[s] = i
	}
	clientMap := make(map[string]string, 16)
	cb := byte('a')
	getClient := func(c string) string {
		if !analyzeOnly {
			return c
		}
		if v, ok := clientMap[c]; ok {
			return v
		}
		clientMap[c] = string([]byte{cb})
		cb++
		return clientMap[c]
	}
	fileMap := func(s string) string {
		return s
	}
	if analyzeOnly {
		// When analyzing map file names to a number for less RAM.
		var i int
		m := make(map[string]int)
		fileMap = func(s string) string {
			if v, ok := m[s]; ok {
				return strconv.Itoa(v)
			}
			i++
			m[s] = i
			return strconv.Itoa(i)
		}
	}
	n := 0
	for {
		values, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(values) == 0 {
			continue
		}
		if offset > 0 {
			offset--
			continue
		}
		start, err := time.Parse(time.RFC3339Nano, values[fieldIdx["start"]])
		if err != nil {
			return err
		}
		var ttfb *time.Time
		if fb := values[fieldIdx["first_byte"]]; fb != "" {
			t, err := time.Parse(time.RFC3339Nano, fb)
			if err != nil {
				return err
			}
			ttfb = &t
		}
		end, err := time.Parse(time.RFC3339Nano, values[fieldIdx["end"]])
		if err != nil {
			return err
		}
		size, err := strconv.ParseInt(values[fieldIdx["bytes"]], 10, 64)
		if err != nil {
			return err
		}
		thread, err := strconv.ParseUint(values[fieldIdx["thread"]], 10, 16)
		if err != nil {
			return err
		}
		objs, err := strconv.ParseInt(values[fieldIdx["n_objects"]], 10, 64)
		if err != nil {
			return err
		}
		var cat Categories
		if idx, ok := fieldIdx["cat"]; ok {
			c, err := strconv.ParseUint(values[idx], 10, 64)
			if err != nil {
				return err
			}
			cat = Categories(c)
		}
		var endpoint, clientID string
		if idx, ok := fieldIdx["endpoint"]; ok {
			endpoint = values[idx]
		}
		if idx, ok := fieldIdx["client_id"]; ok {
			clientID = values[idx]
		}
		file := fileMap(values[fieldIdx["file"]])

		out <- Operation{
			OpType:     values[fieldIdx["op"]],
			ObjPerOp:   int(objs),
			Start:      start,
			FirstByte:  ttfb,
			End:        end,
			Err:        values[fieldIdx["error"]],
			Size:       size,
			File:       file,
			Thread:     uint16(thread),
			Endpoint:   endpoint,
			ClientID:   getClient(clientID),
			Categories: cat,
		}
		n++
		if log != nil && n%100000 == 0 {
			log("%d operations loaded. Timestamp: %v", n, start.Round(time.Second).Local())
		}
		if limit > 0 && n >= limit {
			break
		}
	}
	if log != nil {
		log("%d operations loaded... Done!", n)
	}
	return nil
}
