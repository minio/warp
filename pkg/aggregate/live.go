package aggregate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/bits"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/warp/pkg/bench"
)

type LiveAggregate struct {
	Title string
	// Total requests
	TotalRequests int `json:"total_requests"`
	// Total objects
	TotalObjects int `json:"total_objects"`
	// Total errors
	TotalErrors int `json:"total_errors"`
	// Total bytes
	TotalBytes int64 `json:"total_bytes"`

	// Unfiltered start time of this operation segment.
	StartTime time.Time `json:"start_time"`
	// Unfiltered end time of this operation segment.
	EndTime time.Time `json:"end_time"`

	// Subset of errors.
	FirstErrors []string `json:"first_errors"`
	// Numbers of hosts
	Hosts MapAsSlice `json:"hosts"`
	// Number of warp clients.
	Clients MapAsSlice `json:"clients"`

	// Throughput information.
	Throughput Throughput `json:"throughput"`

	throughput liveThroughput

	// Requests segmented
	Requests []RequestSegment `json:"requests"`

	requests liveRequests
}

type Requests struct {
}

type RequestSegment struct {
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`

	// Populated if requests are of difference object sizes.
	Multi *MultiSizedRequests `json:"multi_sized_requests,omitempty"`

	// Populated if requests are all of same object size.
	Single *SingleSizedRequests `json:"single_sized_requests,omitempty"`
}

type MapAsSlice map[string]struct{}

func (m MapAsSlice) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	var dst bytes.Buffer
	dst.WriteByte('[')
	x := m.Slice()
	for i, k := range x {
		dst.WriteByte('"')
		json.HTMLEscape(&dst, []byte(k))
		dst.WriteByte('"')
		if i < len(x)-1 {
			dst.WriteByte(',')
		}
	}
	dst.WriteByte(']')

	return dst.Bytes(), nil
}

// Slice returns the keys as a sorted slice.
func (m MapAsSlice) Slice() []string {
	x := make([]string, 0, len(m))
	for k := range m {
		x = append(x, k)
	}
	sort.Strings(x)
	return x
}

// Add operation to aggregate.
func (l *LiveAggregate) Add(o bench.Operation) {
	l.TotalRequests++
	l.TotalObjects += o.ObjPerOp
	l.TotalBytes += o.Size
	if o.ClientID != "" {
		if l.Clients == nil {
			l.Clients = make(map[string]struct{})
		}
		l.Clients[o.ClientID] = struct{}{}
	}
	if l.Hosts == nil {
		l.Hosts = make(map[string]struct{})
	}
	l.Hosts[o.Endpoint] = struct{}{}
	if l.StartTime.IsZero() || l.StartTime.After(o.Start) {
		l.StartTime = o.Start
	}
	if l.EndTime.Before(o.End) {
		l.EndTime = o.End
	}
	if o.Err != "" {
		if len(l.FirstErrors) < 10 {
			l.FirstErrors = append(l.FirstErrors, o.Err)
		}
		l.TotalErrors++
	}
	l.throughput.Add(o)
	l.requests.Add(o)
}

// Merge l2 into l.
func (l *LiveAggregate) Merge(l2 LiveAggregate) {
	l.Throughput.Merge(l2.Throughput)
}

// Update returns a temporary update without finalizing.
// The update will have no references to the live version.
func (l LiveAggregate) Update() LiveAggregate {
	var dst = l
	dst.Throughput = dst.throughput.asThroughput()
	dst.throughput = liveThroughput{}
	startTime := time.Unix(dst.requests.firstSeg, 0)
	dst.Requests = make([]RequestSegment, 0, len(l.requests.single)+len(l.requests.multi))
	for i := range l.requests.multi {
		dst.Requests = append(l.Requests, RequestSegment{StartTime: startTime, EndTime: startTime.Add(time.Second * requestSegmentsDur), Multi: &l.requests.multi[i]})
		startTime = startTime.Add(time.Second * requestSegmentsDur)
	}
	for i := range l.requests.single {
		dst.Requests = append(l.Requests, RequestSegment{StartTime: startTime, EndTime: startTime.Add(time.Second * requestSegmentsDur), Single: &l.requests.single[i]})
		startTime = startTime.Add(time.Second * requestSegmentsDur)
	}
	dst.requests = liveRequests{}
	dst.Clients = MapAsSlice{}
	dst.Hosts = MapAsSlice{}
	dst.FirstErrors = slices.Clone(l.FirstErrors)

	return dst
}

func (l *LiveAggregate) Finalize() {
	l.Throughput = l.throughput.asThroughput()
	l.requests.cycle()
	startTime := time.Unix(l.requests.firstSeg, 0)
	for i := range l.requests.multi {
		l.Requests = append(l.Requests, RequestSegment{StartTime: startTime, EndTime: startTime.Add(time.Second * requestSegmentsDur), Multi: &l.requests.multi[i]})
		startTime = startTime.Add(time.Second * requestSegmentsDur)
	}
	for i := range l.requests.single {
		l.Requests = append(l.Requests, RequestSegment{StartTime: startTime, EndTime: startTime.Add(time.Second * requestSegmentsDur), Single: &l.requests.single[i]})
		startTime = startTime.Add(time.Second * requestSegmentsDur)
	}
}

type Realtime struct {
	Total         LiveAggregate                     `json:"total"`
	ByOpType      map[string]*LiveAggregate         `json:"by_op_type,omitempty"`
	ByHost        map[string]*LiveAggregate         `json:"by_host,omitempty"`
	ByObjLog2Size map[int]*LiveAggregate            `json:"by_obj_log_2_size,omitempty"`
	ByClient      map[string]*LiveAggregate         `json:"by_client,omitempty"`
	ByCategory    map[bench.Category]*LiveAggregate `json:"by_category,omitempty"`
}

func finalizeValues[K comparable](m map[K]*LiveAggregate) {
	for _, v := range m {
		if v != nil {
			v.Finalize()
		}
	}
}

func (r *Realtime) Finalize() {
	finalizeValues(r.ByOpType)
	finalizeValues(r.ByHost)
	finalizeValues(r.ByObjLog2Size)
	finalizeValues(r.ByClient)
	finalizeValues(r.ByCategory)
	r.Total.Finalize()
}

func newRealTime() Realtime {
	return Realtime{
		ByOpType:      make(map[string]*LiveAggregate),
		ByHost:        make(map[string]*LiveAggregate),
		ByObjLog2Size: make(map[int]*LiveAggregate),
		ByClient:      make(map[string]*LiveAggregate),
	}
}

// Live collects operations and update requests.
func Live(ops <-chan bench.Operation, updates <-chan UpdateReq) *Realtime {
	a := newRealTime()
	var reset atomic.Bool
	var update atomic.Pointer[Realtime]
	if updates != nil {
		done := make(chan struct{})
		defer close(done)
		go func() {
			t := time.NewTicker(time.Second / 4)
			defer t.Stop()
			for {
				select {
				case r := <-updates:
					if r.Reset {
						reset.Store(true)
						update.Store(nil)
						continue
					}
					select {
					case r.C <- update.Load():
					default:
						fmt.Println("could not send")
					}
				case <-done:
					return
				case <-t.C: // reload value
				}
			}
		}()
	}
	lastUpdate := time.Now()
	for op := range ops {
		if reset.CompareAndSwap(true, false) {
			a = newRealTime()
		}
		var wg sync.WaitGroup
		wg.Add(6)
		// 1
		go func() {
			defer wg.Done()
			byOp := a.ByOpType[op.OpType]
			if byOp == nil {
				byOp = &LiveAggregate{Title: "Operation: " + op.OpType}
				a.ByOpType[op.OpType] = byOp
			}
			byOp.Add(op)
		}()
		// 2
		go func() {
			defer wg.Done()
			byHost := a.ByHost[op.Endpoint]
			if byHost == nil {
				byHost = &LiveAggregate{Title: "Host: " + op.Endpoint}
				a.ByHost[op.Endpoint] = byHost
			}
			byHost.Add(op)
		}()
		// 3
		go func() {
			defer wg.Done()
			if op.ClientID != "" {
				byClient := a.ByClient[op.ClientID]
				if byClient == nil {
					byClient = &LiveAggregate{Title: "Client: " + op.ClientID}
					a.ByClient[op.ClientID] = byClient
				}
				byClient.Add(op)
			}
		}()
		// 4
		go func() {
			defer wg.Done()
			l2Size := bits.Len64(uint64(op.Size))
			bySize := a.ByObjLog2Size[l2Size]
			if bySize == nil {
				bySize = &LiveAggregate{Title: fmt.Sprintf("Size: %d->%d", 1>>(l2Size-1), (1>>l2Size)-1)}
				a.ByObjLog2Size[l2Size] = bySize
			}
			bySize.Add(op)
		}()
		// 5
		go func() {
			defer wg.Done()
			a.Total.Add(op)
		}()
		// 6
		go func() {
			defer wg.Done()
			if op.Categories != 0 {
				cats := op.Categories.Split()
				if a.ByCategory == nil {
					a.ByCategory = make(map[bench.Category]*LiveAggregate, len(cats))
				}
				for _, cat := range cats {
					byCat := a.ByCategory[cat]
					if byCat == nil {
						byCat = &LiveAggregate{Title: "Category: " + cat.String()}
						a.ByCategory[cat] = byCat
					}
					byCat.Add(op)
				}
			}
		}()
		wg.Wait()
		if updates != nil && time.Since(lastUpdate) > time.Second {
			u := Realtime{Total: a.Total.Update(), ByOpType: make(map[string]*LiveAggregate, len(a.ByOpType))}
			for k, v := range a.ByOpType {
				if v != nil {
					clone := v.Update()
					u.ByOpType[k] = &clone
				}
			}
			update.Store(&u)
			lastUpdate = time.Now()
		}
	}
	a.Finalize()
	return &a
}

type liveThroughput struct {
	// Unit time (seconds) of first segment
	segmentsStart int64

	segments []liveSegments
}

type liveSegments struct {
	// Spread across segments from start to end
	ops        float64
	objs       float64
	bytes      float64
	opsStarted int
	opsEnded   int
	fullOps    int
	partialOps int

	// For requests that started in this segment.
	errors int
	reqDur time.Duration
	ttfb   time.Duration
}

func (l *liveThroughput) Add(o bench.Operation) {
	startUnixNano := o.Start.UnixNano()
	startUnix := startUnixNano / int64(time.Second)
	if len(l.segments) == 0 {
		l.segments = make([]liveSegments, 0, 100)
		l.segmentsStart = startUnix
	}
	if startUnixNano < l.segmentsStart {
		// Drop...
		return
	}
	endUnixNano := o.End.UnixNano()
	endUnix := endUnixNano / int64(time.Second)
	durNanos := endUnixNano - startUnixNano

	startSeg := int(startUnix - l.segmentsStart)
	endSeg := int(endUnix - l.segmentsStart + 1)
	if endSeg > len(l.segments) {
		// Append empty segments
		l.segments = append(l.segments, make([]liveSegments, endSeg-len(l.segments))...)
	}
	segs := l.segments[startSeg:endSeg]
	for i := range segs {
		seg := &l.segments[startSeg+i]
		if i == 0 {
			seg.opsStarted++
			seg.reqDur += o.Duration()
			if len(o.Err) > 0 {
				seg.errors++
			}
			seg.ttfb += o.TTFB()
		}
		if i == len(segs)-1 {
			seg.opsEnded++
		}
		// Happy path - doesn't cross segments
		if len(segs) == 1 {
			seg.fullOps++
			seg.ops += 1
			seg.objs += float64(o.ObjPerOp)
			seg.bytes += float64(o.Size)
			continue
		}

		seg.partialOps++
		segStartNano := (startUnix + int64(i)) * int64(time.Second)
		segEndNano := (startUnix + int64(i) + 1) * int64(time.Second)
		var nanosInSeg = int64(time.Second)
		if startUnixNano >= segStartNano {
			nanosInSeg = segEndNano - startUnixNano
		}
		if endUnixNano <= segEndNano {
			nanosInSeg = segEndNano - endUnixNano
		}
		if nanosInSeg > 0 {
			fraction := float64(nanosInSeg) / float64(durNanos)
			seg.objs += float64(o.ObjPerOp) * fraction
			seg.bytes += float64(o.Size) * fraction
			seg.ops += fraction
		}
	}
}

func (l liveThroughput) asThroughput() Throughput {
	var t Throughput
	t.StartTime = time.Unix(l.segmentsStart, 0)
	t.EndTime = time.Unix(l.segmentsStart+int64(len(l.segments))+1, 0)
	segs := l.segments
	// Remove first and last...
	const removeN = 1
	if len(segs) < removeN*2 {
		segs = segs[:]
		return t
	} else {
		segs = segs[removeN : len(segs)-removeN]
	}
	var ts ThroughputSegmented
	segments := make(bench.Segments, 0, len(segs))
	for i, seg := range segs {
		t.Errors += seg.errors
		t.Bytes += seg.bytes
		t.Objects += seg.objs
		t.Operations += seg.opsStarted
		segments = append(segments, bench.Segment{
			Start:      time.Unix(l.segmentsStart+int64(i), 0),
			EndsBefore: time.Unix(l.segmentsStart+int64(i+1), -1),
			OpType:     "",
			Host:       "",
			OpsStarted: seg.opsStarted,
			PartialOps: seg.partialOps,
			FullOps:    seg.fullOps,
			OpsEnded:   seg.opsEnded,
			Objects:    seg.ops,
			Errors:     seg.errors,
			ReqAvg:     float64(seg.reqDur) / float64(seg.opsStarted), // TODO: CHECK 0
			TotalBytes: int64(seg.bytes),
			ObjsPerOp:  int(seg.objs / seg.ops),
		})
	}

	if len(segs) > 0 {
		ts.fill(segments, int64(t.BytesPS()))
		ts.SegmentDurationMillis = 1000 * len(segs)
		t.MeasureDurationMillis = len(l.segments) * 1000
	}
	t.Segmented = &ts

	return t
}

// requestSegmentsDur is the request segment duration.
const requestSegmentsDur = 10

type liveRequests struct {
	// Unit time (seconds) of first segment
	currStart int64
	firstSeg  int64
	ops       bench.Operations
	isMulti   *bool

	single []SingleSizedRequests
	multi  []MultiSizedRequests
}

func (l *liveRequests) Add(o bench.Operation) {
	logTime := o.End.Unix()
	if l.firstSeg == 0 {
		l.firstSeg = logTime
		l.currStart = logTime
	}
	if logTime < l.currStart {
		// If slightly out of order, add to current
		logTime = l.currStart
	}
	if logTime-l.currStart < requestSegmentsDur {
		l.ops = append(l.ops, o)
		return
	}
	l.cycle()
	l.currStart += requestSegmentsDur
	l.ops = l.ops[:0]
	for l.currStart+requestSegmentsDur < logTime {
		if *l.isMulti {
			l.multi = append(l.multi, MultiSizedRequests{Skipped: true})
		} else {
			l.single = append(l.single, SingleSizedRequests{Skipped: true})
		}
		l.currStart += requestSegmentsDur
	}
	l.ops = append(l.ops, o)
}

func (l *liveRequests) cycle() {
	if len(l.ops) == 0 {
		return
	}
	if l.isMulti == nil {
		im := l.ops.MultipleSizes()
		l.isMulti = &im
	}
	if *l.isMulti {
		var tmp MultiSizedRequests
		tmp.fill(l.ops)
		// Remove some bonus fields...
		for k := range tmp.BySize {
			tmp.BySize[k].BpsPct = nil
			if tmp.BySize[k].FirstByte != nil {
				tmp.BySize[k].FirstByte.PercentilesMillis = nil
			}
		}
		tmp.ByHost = nil
		l.multi = append(l.multi, tmp)
	} else {
		var tmp SingleSizedRequests
		tmp.fill(l.ops)
		// Remove some fields that are excessive...
		tmp.ByHost = nil
		tmp.DurPct = nil
		if tmp.FirstByte != nil {
			tmp.FirstByte.PercentilesMillis = nil
		}
		l.single = append(l.single, tmp)
	}
}
