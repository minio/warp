package aggregate

import (
	"time"

	"github.com/minio/warp/pkg/bench"
)

// Operation returns statistics for a single operation type.
type Operation struct {
	// Operation type
	Type string `json:"type"`
	// Skipped if too little data
	Skipped bool `json:"skipped"`
	// Objects per operation.
	ObjectsPerOperation int `json:"objects_per_operation"`
	// Concurrency - total number of threads running.
	Concurrency int `json:"concurrency"`
	// Numbers of hosts
	Hosts int `json:"hosts"`
	// Populated if requests are all of similar object size.
	SingleSizedRequests *SingleSizeRequests `json:"single_sized_requests,omitempty"`
	// Total errors recorded.
	Errors int `json:"errors"`
	// Subset of errors.
	FirstErrors []string `json:"first_errors"`
	// Throughput information.
	Throughput Throughput `json:"throughput"`
	// Throughput by host.
	ThroughputByHost map[string]Throughput `json:"throughput_by_host"`
}

// SingleSizeRequests contains statistics when all objects have the same size.
type SingleSizeRequests struct {
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
	ByHost map[string]SingleSizeRequests `json:"by_host,omitempty"`
}

func (a *SingleSizeRequests) fill(ops bench.Operations) {
	start, end := ops.TimeRange()
	ops.SortByDuration()
	a.ObjSize = ops.FirstObjSize()
	a.DurAvgMillis = durToMillis(ops.AvgDuration())
	a.DurMedianMillis = durToMillis(ops.Median(0.5).Duration())
	a.Dur90Millis = durToMillis(ops.Median(0.9).Duration())
	a.Dur99Millis = durToMillis(ops.Median(0.99).Duration())
	a.SlowestMillis = durToMillis(ops.Median(1).Duration())
	a.FastestMillis = durToMillis(ops.Median(0).Duration())
	a.FirstByte = TtfbFromBench(ops.TTFB(start, end))
}

// TTFB contains times to first byte if applicable.
type TTFB struct {
	AverageMillis int `json:"average_millis"`
	MedianMillis  int `json:"median_millis"`
	FastestMillis int `json:"fastest_millis"`
	SlowestMillis int `json:"slowest_millis"`
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
	// Time segmented Throughput.
	Segmented *ThroughputSegmented `json:"segmented,omitempty"`
}

func (t *Throughput) fill(total bench.Segment) {
	mib, _, objs := total.SpeedPerSec()
	*t = Throughput{
		MeasureDurationMillis: durToMillis(total.EndsBefore.Sub(total.Start)),
		StartTime:             total.Start,
		AverageBPS:            mib * (1 << 20),
		AverageOPS:            objs,
		Errors:                total.Errors,
	}
}

// ThroughputSegmented contains time segmented throughput statics.
type ThroughputSegmented struct {
	// Time of each segment.
	MeasureDurationMillis int `json:"measure_duration_millis"`
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

func (a *ThroughputSegmented) fill(segs bench.Segments) {
	fast := segs.Median(1)
	med := segs.Median(0.5)
	slow := segs.Median(0)

	bps := func(s bench.Segment) float64 {
		mib, _, _ := s.SpeedPerSec()
		return mib * (1 << 20)
	}
	ops := func(s bench.Segment) float64 {
		_, _, objs := s.SpeedPerSec()
		return objs
	}

	*a = ThroughputSegmented{
		MeasureDurationMillis: a.MeasureDurationMillis,
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

// SingleOp returns statistics when only a single operation was running concurrently.
func SingleOp(o bench.Operations, segmentDur, skipDur time.Duration) []Operation {
	o.SortByStartTime()
	types := o.OpTypes()
	res := make([]Operation, 0, len(types))
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
			AllThreads:     true,
		})
		if len(segs) <= 1 {
			a.Skipped = true
			continue
		}
		total := ops.Total(true)
		if total.TotalBytes > 0 {
			segs.SortByThroughput()
		} else {
			segs.SortByObjsPerSec()
		}
		a.Throughput.fill(total)
		a.Throughput.Segmented = &ThroughputSegmented{
			MeasureDurationMillis: durToMillis(segmentDur),
		}
		a.Throughput.Segmented.fill(segs)

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
			reqs := RequestAnalysisSingleOp(ops)
			a.SingleSizedRequests = &reqs
		} else {
			// TODO:
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
				if total.TotalBytes > 0 {
					segs.SortByThroughput()
				} else {
					segs.SortByObjsPerSec()
				}

				host.Segmented = &ThroughputSegmented{
					MeasureDurationMillis: durToMillis(segmentDur),
				}
				host.Segmented.fill(segs)
			}
			a.ThroughputByHost[ep] = host
		}

		res = append(res, a)
	}
	return res
}

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

func RequestAnalysisSingleOp(o bench.Operations) SingleSizeRequests {
	var res SingleSizeRequests
	start, end := o.ActiveTimeRange(false)
	active := o.FilterInsideRange(start, end)
	// Single type, require one operation per thread.
	start, end = o.ActiveTimeRange(true)
	active = o.FilterInsideRange(start, end)

	res.Requests = len(active)
	if len(active) == 0 {
		res.Skipped = true
		return res
	}
	res.fill(active)
	res.ByHost = RequestAnalysisHostsSingleOp(o)

	return res
}

func RequestAnalysisHostsSingleOp(o bench.Operations) map[string]SingleSizeRequests {
	eps := o.Endpoints()
	res := make(map[string]SingleSizeRequests, len(eps))
	for _, ep := range eps {
		filtered := o.FilterByEndpoint(ep)
		if len(filtered) <= 1 {
			continue
		}
		filtered.SortByDuration()
		a := SingleSizeRequests{}
		a.fill(filtered)
		res[ep] = a
	}
	return res
}

func RequestAnalysisMultiOp(o bench.Operations) {
	/*
		console.Print("\nRequests considered: ", len(active), ". Multiple sizes, average ", active.AvgSize(), " bytes:\n")
		console.SetColor("Print", color.New(color.FgWhite))

		if len(active) == 0 {
			console.Println("Not enough requests")
		}

		sizes := active.SplitSizes(0.05)
		for _, s := range sizes {
			active := s.Ops

			console.SetColor("Print", color.New(color.FgHiWhite))
			console.Print("\nRequest size ", s.SizeString(), ". Requests - ", len(active), ":\n")
			console.SetColor("Print", color.New(color.FgWhite))

			active.SortByThroughput()
			console.Print(""+
				" * Throughput: Average: ", active.OpThroughput(),
				", 50%: ", active.Median(0.5).BytesPerSec(),
				", 90%: ", active.Median(0.9).BytesPerSec(),
				", 99%: ", active.Median(0.99).BytesPerSec(),
				", Fastest: ", active.Median(0).BytesPerSec(),
				", Slowest: ", active.Median(1).BytesPerSec(),
				"\n")
			ttfb := active.TTFB(start, end)
			if ttfb.Average > 0 {
				console.Println(" * First Byte:", ttfb)
			}
		}
	*/
}
