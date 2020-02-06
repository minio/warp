package aggregate

import (
	"fmt"
	"math"
	"time"

	"github.com/minio/warp/pkg/bench"
)

type Aggregated struct {
	Type                  string                `json:"type"`
	Mixed                 bool                  `json:"mixed"`
	Operations            []Operation           `json:"operations,omitempty"`
	MixedServerStats      *Throughput           `json:"mixed_server_stats,omitempty"`
	MixedThroughputByHost map[string]Throughput `json:"mixed_throughput_by_host,omitempty"`
	segmentDur            time.Duration
}

func (a Aggregated) HasDuration(d time.Duration) bool {
	return a.segmentDur == d
}

// Operation returns statistics for a single operation type.
type Operation struct {
	// Operation type
	Type string `json:"type"`
	// Skipped if too little data
	Skipped bool `json:"skipped"`
	// Unfiltered start time of this operation segment.
	StartTime time.Time `json:"start_time"`
	// Unfiltered end time of this operation segment.
	EndTime time.Time `json:"end_time"`
	// Objects per operation.
	ObjectsPerOperation int `json:"objects_per_operation"`
	// Concurrency - total number of threads running.
	Concurrency int `json:"concurrency"`
	// Numbers of hosts
	Hosts int `json:"hosts"`
	// Populated if requests are all of same object size.
	SingleSizedRequests *SingleSizedRequests `json:"single_sized_requests,omitempty"`
	// Populated if requests are of difference object sizes.
	MultiSizedRequests *MultiSizedRequests `json:"multi_sized_requests,omitempty"`
	// Total errors recorded.
	Errors int `json:"errors"`
	// Subset of errors.
	FirstErrors []string `json:"first_errors"`
	// Throughput information.
	Throughput Throughput `json:"throughput"`
	// Throughput by host.
	ThroughputByHost map[string]Throughput `json:"throughput_by_host"`
}

// SingleSizedRequests contains statistics when all objects have the same size.
type SingleSizedRequests struct {
	// Skipped if too little data.
	Skipped bool `json:"skipped"`
	// Object size per operation. Can be 0.
	ObjSize int64 `json:"obj_size"`
	// Total number of requests.
	Requests int `json:"requests"`
	// Average request duration.
	DurAvgMillis int `json:"dur_avg_millis"`
	// Median request duration.
	DurMedianMillis int `json:"dur_median_millis"`
	// 90% request time.
	Dur90Millis int `json:"dur_90_millis"`
	// 99% request time.
	Dur99Millis int `json:"dur_99_millis"`
	// Fastest request time.
	FastestMillis int `json:"fastest_millis"`
	// Slowest request time.
	SlowestMillis int `json:"slowest_millis"`
	// Time to first byte if applicable.
	FirstByte *TTFB `json:"first_byte,omitempty"`
	// Request times by host.
	ByHost map[string]SingleSizedRequests `json:"by_host,omitempty"`
}

func (a *SingleSizedRequests) fill(ops bench.Operations) {
	start, end := ops.TimeRange()
	ops.SortByDuration()
	a.Requests = len(ops)
	a.ObjSize = ops.FirstObjSize()
	a.DurAvgMillis = durToMillis(ops.AvgDuration())
	a.DurMedianMillis = durToMillis(ops.Median(0.5).Duration())
	a.Dur90Millis = durToMillis(ops.Median(0.9).Duration())
	a.Dur99Millis = durToMillis(ops.Median(0.99).Duration())
	a.SlowestMillis = durToMillis(ops.Median(1).Duration())
	a.FastestMillis = durToMillis(ops.Median(0).Duration())
	a.FirstByte = TtfbFromBench(ops.TTFB(start, end))
}

type RequestSizeRange struct {
	// Number of requests in this range.
	Requests int `json:"requests"`
	// Minimum size in request size range.
	MinSize       int    `json:"min_size"`
	MinSizeString string `json:"min_size_string"`
	// Maximum size in request size range (not included).
	MaxSize       int    `json:"max_size"`
	MaxSizeString string `json:"max_size_string"`
	// Average payload size of requests in bytes.
	AvgObjSize        int `json:"avg_obj_size"`
	AvgDurationMillis int `json:"avg_duration_millis"`

	// Stats:
	BpsAverage float64 `json:"bps_average"`
	BpsMedian  float64 `json:"bps_median"`
	Bps90      float64 `json:"bps_90"`
	Bps99      float64 `json:"bps_99"`
	BpsFastest float64 `json:"bps_fastest"`
	BpsSlowest float64 `json:"bps_slowest"`

	// Time to first byte if applicable.
	FirstByte *TTFB `json:"first_byte,omitempty"`
}

func (r *RequestSizeRange) fill(s bench.SizeSegment) {
	r.Requests = len(s.Ops)
	r.MinSize = int(s.Smallest)
	r.MaxSize = int(s.Biggest)
	r.MinSizeString, r.MaxSizeString = s.SizesString()
	r.AvgObjSize = int(s.Ops.AvgSize())
	r.AvgDurationMillis = durToMillis(s.Ops.AvgDuration())
	s.Ops.SortByThroughput()
	r.BpsAverage = s.Ops.OpThroughput().Float()
	r.BpsMedian = s.Ops.Median(0.5).BytesPerSec().Float()
	r.Bps90 = s.Ops.Median(0.9).BytesPerSec().Float()
	r.Bps99 = s.Ops.Median(0.99).BytesPerSec().Float()
	r.BpsFastest = s.Ops.Median(0.0).BytesPerSec().Float()
	r.BpsSlowest = s.Ops.Median(1).BytesPerSec().Float()
}

// MultiSizedRequests contains statistics when objects have the same different size.
type MultiSizedRequests struct {
	// Skipped if too little data.
	Skipped bool `json:"skipped"`
	// Total number of requests.
	Requests int `json:"requests"`
	// Average object size
	AvgObjSize int64 `json:"avg_obj_size"`

	BySize []RequestSizeRange `json:"by_size"`

	ByHost map[string]RequestSizeRange `json:"by_host,omitempty"`
}

func (a *MultiSizedRequests) fill(ops bench.Operations) {
	start, end := ops.TimeRange()
	a.Requests = len(ops)
	if len(ops) == 0 {
		a.Skipped = true
		return
	}
	a.AvgObjSize = ops.AvgSize()
	sizes := ops.SplitSizes(0.05)
	a.BySize = make([]RequestSizeRange, 0, len(sizes))
	for _, s := range sizes {
		var r RequestSizeRange
		r.fill(s)
		r.FirstByte = TtfbFromBench(s.Ops.TTFB(start, end))
		// Store
		a.BySize = append(a.BySize, r)
	}
}

// TTFB contains times to first byte if applicable.
type TTFB struct {
	AverageMillis int `json:"average_millis"`
	MedianMillis  int `json:"median_millis"`
	FastestMillis int `json:"fastest_millis"`
	SlowestMillis int `json:"slowest_millis"`
}

// String returns a human printable version of the time to first byte.
func (t TTFB) String() string {
	if t.AverageMillis == 0 {
		return ""
	}
	return fmt.Sprintf("Average: %v, Median: %v, Best: %v, Worst: %v",
		time.Duration(t.AverageMillis)*time.Millisecond,
		time.Duration(t.MedianMillis)*time.Millisecond,
		time.Duration(t.FastestMillis)*time.Millisecond,
		time.Duration(t.SlowestMillis)*time.Millisecond)
}

// Throughput contains throughput.
type Throughput struct {
	// Errors recorded.
	Errors int `json:"errors"`
	// Time period of the throughput measurement.
	MeasureDurationMillis int `json:"measure_duration_millis"`
	// Start time of the measurement.
	StartTime time.Time `json:"start_time"`
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
	speed := ""
	if t.AverageBPS > 0 {
		speed = fmt.Sprintf("%.02f MiB/s, ", t.AverageBPS/(1<<20))
	}
	return fmt.Sprintf("%s%.02f obj/s (%v, starting %v)",
		speed, t.AverageOPS, time.Duration(t.MeasureDurationMillis)*time.Millisecond, t.StartTime.Format("15:04:05 MST"))
}

func (t *Throughput) fill(total bench.Segment) {
	mib, _, objs := total.SpeedPerSec()
	*t = Throughput{
		Operations:            total.FullOps,
		MeasureDurationMillis: durToMillis(total.EndsBefore.Sub(total.Start)),
		StartTime:             total.Start,
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

func BPSorOPS(bps, ops float64) string {
	if bps > 0 {
		return bench.Throughput(bps).String()
	}
	return fmt.Sprintf("%0.2f obj/s", ops)
}

type SegmentSmall struct {
	BPS    float64   `json:"bytes_per_sec"`
	OPS    float64   `json:"obj_per_sec"`
	Errors int       `json:"errors,omitempty"`
	Start  time.Time `json:"start"`
}

func CloneBenchSegments(s bench.Segments) []SegmentSmall {
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

// String returns a string representation of the segment
func (s SegmentSmall) StringLong(d time.Duration) string {
	speed := ""
	if s.BPS > 0 {
		speed = bench.Throughput(s.BPS).String() + ", "
	}
	return fmt.Sprintf("%s%.02f obj/s (%v, starting %v)",
		speed, s.OPS, d, s.Start.Format("15:04:05 MST"))
}

func (a *ThroughputSegmented) fill(segs bench.Segments, total bench.Segment) {
	// Copy by time.
	segs.SortByTime()
	smallSegs := CloneBenchSegments(segs)

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

// Aggregate returns statistics when only a single operation was running concurrently.
func Aggregate(o bench.Operations, segmentDur, skipDur time.Duration) Aggregated {
	o.SortByStartTime()
	types := o.OpTypes()
	a := Aggregated{
		Type:                  "single",
		Mixed:                 false,
		Operations:            nil,
		MixedServerStats:      nil,
		MixedThroughputByHost: nil,
		segmentDur:            segmentDur,
	}
	res := make([]Operation, 0, len(types))
	isMixed := o.IsMixed()
	// Fill mixed only parts...
	if isMixed {
		a.Mixed = true
		a.Type = "mixed"
		ops := o.FilterInsideRange(o.ActiveTimeRange(true))
		total := ops.Total(false)
		a.MixedServerStats = &Throughput{}
		a.MixedServerStats.fill(total)
		segs := o.Segment(bench.SegmentOptions{
			From:           time.Time{},
			PerSegDuration: segmentDur,
			AllThreads:     true,
			MultiOp:        true,
		})
		if len(segs) > 1 {
			a.MixedServerStats.Segmented = &ThroughputSegmented{
				SegmentDurationMillis: durToMillis(segmentDur),
			}
			a.MixedServerStats.Segmented.fill(segs, total)
		}

		eps := o.Endpoints()
		a.MixedThroughputByHost = make(map[string]Throughput, len(eps))
		for _, ep := range eps {
			ops := o.FilterByEndpoint(ep)
			t := Throughput{}
			t.fill(ops.Total(false))
			a.MixedThroughputByHost[ep] = t
		}
	}

	for _, typ := range types {
		a := Operation{}
		a.Type = typ
		ops := o.FilterByOp(typ)
		if skipDur > 0 {
			start, end := ops.TimeRange()
			start = start.Add(skipDur)
			ops = ops.FilterInsideRange(start, end)
		}

		segs := ops.Segment(bench.SegmentOptions{
			From:           time.Time{},
			PerSegDuration: segmentDur,
			AllThreads:     !isMixed,
		})
		if len(segs) <= 1 {
			a.Skipped = true
			continue
		}
		total := ops.Total(!isMixed)
		a.StartTime, a.EndTime = ops.TimeRange()
		a.Throughput.fill(total)
		a.Throughput.Segmented = &ThroughputSegmented{
			SegmentDurationMillis: durToMillis(segmentDur),
		}
		a.Throughput.Segmented.fill(segs, total)

		a.ObjectsPerOperation = ops.FirstObjPerOp()
		a.Concurrency = ops.Threads()
		a.Hosts = ops.Hosts()

		if errs := ops.Errors(); len(errs) > 0 {
			a.Errors = len(errs)
			for _, err := range errs {
				if len(a.FirstErrors) >= 10 {
					break
				}
				a.FirstErrors = append(a.FirstErrors, err)
			}
		}
		if !ops.MultipleSizes() {
			a.SingleSizedRequests = RequestAnalysisSingleSized(ops, !isMixed)
		} else {
			a.MultiSizedRequests = RequestAnalysisMultiSized(ops, !isMixed)
		}

		eps := ops.Endpoints()
		a.ThroughputByHost = make(map[string]Throughput, len(eps))
		for _, ep := range eps {
			ops := ops.FilterByEndpoint(ep)
			total := ops.Total(false)
			var host Throughput
			host.fill(total)

			segs := ops.Segment(bench.SegmentOptions{
				From:           time.Time{},
				PerSegDuration: segmentDur,
				AllThreads:     false,
			})

			if len(segs) > 1 {
				host.Segmented = &ThroughputSegmented{
					SegmentDurationMillis: durToMillis(segmentDur),
				}
				host.Segmented.fill(segs, total)
			}
			a.ThroughputByHost[ep] = host
		}

		res = append(res, a)
	}
	a.Operations = res
	return a
}

// TtfbFromBench converts from bench.TTFB
func TtfbFromBench(t bench.TTFB) *TTFB {
	if t.Average <= 0 {
		return nil
	}
	return &TTFB{
		AverageMillis: durToMillis(t.Average),
		MedianMillis:  durToMillis(t.Median),
		FastestMillis: durToMillis(t.Best),
		SlowestMillis: durToMillis(t.Worst),
	}
}

func durToMillis(d time.Duration) int {
	return int(d.Round(time.Millisecond) / time.Millisecond)
}

func RequestAnalysisSingleSized(o bench.Operations, allThreads bool) *SingleSizedRequests {
	var res SingleSizedRequests

	// Single type, require one operation per thread.
	start, end := o.ActiveTimeRange(allThreads)
	active := o.FilterInsideRange(start, end)

	if len(active) == 0 {
		res.Skipped = true
		return &res
	}
	res.fill(active)
	res.ByHost = RequestAnalysisHostsSingleSized(o)

	return &res
}

func RequestAnalysisMultiSized(o bench.Operations, allThreads bool) *MultiSizedRequests {
	var res MultiSizedRequests
	// Single type, require one operation per thread.
	start, end := o.ActiveTimeRange(false)
	active := o.FilterInsideRange(start, end)

	res.Requests = len(active)
	if len(active) == 0 {
		res.Skipped = true
		return &res
	}
	res.fill(active)
	res.ByHost = RequestAnalysisHostsMultiSized(active)
	return &res
}

func RequestAnalysisHostsSingleSized(o bench.Operations) map[string]SingleSizedRequests {
	eps := o.Endpoints()
	res := make(map[string]SingleSizedRequests, len(eps))
	for _, ep := range eps {
		filtered := o.FilterByEndpoint(ep)
		if len(filtered) <= 1 {
			continue
		}
		filtered.SortByDuration()
		a := SingleSizedRequests{}
		a.fill(filtered)
		res[ep] = a
	}
	return res
}

func RequestAnalysisHostsMultiSized(o bench.Operations) map[string]RequestSizeRange {
	eps := o.Endpoints()
	res := make(map[string]RequestSizeRange, len(eps))
	start, end := o.TimeRange()
	for _, ep := range eps {
		filtered := o.FilterByEndpoint(ep)
		if len(filtered) <= 1 {
			continue
		}
		a := RequestSizeRange{}
		a.fill(filtered.SingleSizeSegment())
		a.FirstByte = TtfbFromBench(filtered.TTFB(start, end))
		res[ep] = a
	}
	return res
}
