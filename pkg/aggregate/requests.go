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

package aggregate

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/minio/warp/pkg/bench"
)

// SingleSizedRequests contains statistics when all objects have the same size.
type SingleSizedRequests struct {
	// Request times by host.
	ByHost map[string]SingleSizedRequests `json:"by_host,omitempty"`

	// FirstAccess is filled if the same object is accessed multiple times.
	// This records the first access of the object.
	FirstAccess *SingleSizedRequests `json:"first_access,omitempty"`

	// FirstAccess is filled if the same object is accessed multiple times.
	// This records the last access of the object.
	LastAccess *SingleSizedRequests `json:"last_access,omitempty"`

	// Time to first byte if applicable.
	FirstByte *TTFB `json:"first_byte,omitempty"`

	// Host names, sorted.
	HostNames MapAsSlice `json:"host_names,omitempty"`

	// DurPct is duration percentiles (milliseconds).
	DurPct *[101]float64 `json:"dur_percentiles_millis,omitempty"`

	// Median request duration.
	DurMedianMillis float64 `json:"dur_median_millis"`

	// Fastest request time.
	FastestMillis float64 `json:"fastest_millis"`

	// Slowest request time.
	SlowestMillis float64 `json:"slowest_millis"`

	// StdDev is the standard deviation of requests.
	StdDev float64 `json:"std_dev_millis"`

	// 99% request time.
	Dur99Millis float64 `json:"dur_99_millis"`

	// 90% request time.
	Dur90Millis float64 `json:"dur_90_millis"`

	// Average request duration.
	DurAvgMillis float64 `json:"dur_avg_millis"`

	// Total number of requests.
	Requests int `json:"requests"`

	// Object size per operation. Can be 0.
	ObjSize int64 `json:"obj_size"`

	// Skipped if too little data.
	Skipped bool `json:"skipped,omitempty"`

	// MergedEntries is a counter for the number of merged entries contained in this result.
	MergedEntries int `json:"merged_entries"`
}

func (a SingleSizedRequests) StringByN() string {
	n := float64(a.MergedEntries)
	if a.Requests == 0 || n == 0 {
		return ""
	}
	invN := 1 / n
	reqs := a

	fmtMillis := func(v float64) string {
		if v*invN > float64(100*time.Millisecond) {
			dur := time.Duration(v * float64(time.Millisecond) * invN).Round(time.Millisecond)
			return dur.String()
		}
		return fmt.Sprintf("%.1fms", v*invN)
	}
	return fmt.Sprint(
		"Avg: ", fmtMillis(reqs.DurAvgMillis),
		", 50%: ", fmtMillis(reqs.DurMedianMillis),
		", 90%: ", fmtMillis(reqs.Dur90Millis),
		", 99%: ", fmtMillis(reqs.Dur99Millis),
		// These are not accumulated.
		", Fastest: ", fmtMillis(reqs.FastestMillis*n),
		", Slowest: ", fmtMillis(reqs.SlowestMillis*n),
		", StdDev: ", fmtMillis(reqs.StdDev),
	)
}

func (a *SingleSizedRequests) add(b SingleSizedRequests) {
	if b.Skipped {
		return
	}
	a.Requests += b.Requests
	a.ObjSize += b.ObjSize
	a.DurAvgMillis += b.DurAvgMillis
	a.DurMedianMillis += b.DurMedianMillis
	if a.MergedEntries == 0 {
		a.FastestMillis = b.FastestMillis
	} else {
		a.FastestMillis = min(a.FastestMillis, b.FastestMillis)
	}
	a.SlowestMillis = max(a.SlowestMillis, b.SlowestMillis)
	a.Dur99Millis += b.Dur99Millis
	a.Dur90Millis += b.Dur90Millis
	a.StdDev += b.StdDev
	a.MergedEntries += b.MergedEntries
	a.HostNames.AddMap(b.HostNames)
	if a.ByHost == nil && len(b.ByHost) > 0 {
		a.ByHost = make(map[string]SingleSizedRequests, len(b.ByHost))
	}
	for k, v := range b.ByHost {
		x := a.ByHost[k]
		x.add(v)
		a.ByHost[k] = x
	}
	if a.DurPct == nil && b.DurPct == nil {
		a.DurPct = &[101]float64{}
	}
	if b.DurPct != nil {
		for i := range b.DurPct {
			a.DurPct[i] += b.DurPct[i]
		}
	}
	if b.FirstAccess != nil {
		if a.FirstAccess == nil {
			a.FirstAccess = &SingleSizedRequests{}
		}
		a.FirstAccess.add(*b.FirstAccess)
	}
	if b.LastAccess != nil {
		if a.LastAccess == nil {
			a.LastAccess = &SingleSizedRequests{}
		}
		a.LastAccess.add(*b.LastAccess)
	}
	if b.FirstByte != nil {
		if a.FirstByte == nil {
			a.FirstByte = &TTFB{}
		}
		a.FirstByte.add(*b.FirstByte)
	}
}

func (a *SingleSizedRequests) fill(ops bench.Operations) {
	start, end := ops.TimeRange()
	ops.SortByDuration()
	a.Requests = len(ops)
	a.ObjSize = ops.FirstObjSize()
	a.DurAvgMillis = durToMillisF(ops.AvgDuration())
	a.StdDev = durToMillisF(ops.StdDev())
	a.DurMedianMillis = durToMillisF(ops.Median(0.5).Duration())
	a.Dur90Millis = durToMillisF(ops.Median(0.9).Duration())
	a.Dur99Millis = durToMillisF(ops.Median(0.99).Duration())
	a.SlowestMillis = durToMillisF(ops.Median(1).Duration())
	a.FastestMillis = durToMillisF(ops.Median(0).Duration())
	a.FirstByte = TtfbFromBench(ops.TTFB(start, end))
	a.DurPct = &[101]float64{}
	a.MergedEntries = 1
	for i := range a.DurPct[:] {
		a.DurPct[i] = durToMillisF(ops.Median(float64(i) / 100).Duration())
	}
}

func (a *SingleSizedRequests) fillFirstLast(ops bench.Operations) {
	if !ops.IsMultiTouch() {
		return
	}
	var first, last SingleSizedRequests
	o := ops.FilterFirst()
	first.fill(o)
	a.FirstAccess = &first
	o = ops.FilterLast()
	last.fill(o)
	a.LastAccess = &last
}

type RequestSizeRange struct {
	// Time to first byte if applicable.
	FirstByte *TTFB `json:"first_byte,omitempty"`

	// FirstAccess is filled if the same object is accessed multiple times.
	// This records the first touch of the object.
	FirstAccess *RequestSizeRange `json:"first_access,omitempty"`

	MinSizeString string `json:"min_size_string"`
	MaxSizeString string `json:"max_size_string"`

	// BpsPct is BPS percentiles.
	BpsPct *[101]float64 `json:"bps_percentiles,omitempty"`

	BpsMedian         float64 `json:"bps_median"`
	AvgDurationMillis float64 `json:"avg_duration_millis"`

	// Stats:
	BpsAverage float64 `json:"bps_average"`
	// Number of requests in this range.
	Requests   int     `json:"requests"`
	Bps90      float64 `json:"bps_90"`
	Bps99      float64 `json:"bps_99"`
	BpsFastest float64 `json:"bps_fastest"`
	BpsSlowest float64 `json:"bps_slowest"`

	// Average payload size of requests in bytes.
	AvgObjSize int `json:"avg_obj_size"`
	// Maximum size in request size range (not included).
	MaxSize int `json:"max_size"`
	// Minimum size in request size range.
	MinSize int `json:"min_size"`

	// MergedEntries is a counter for the number of merged entries contained in this result.
	MergedEntries int `json:"merged_entries"`
}

func (s RequestSizeRange) String() string {
	if s.MergedEntries <= 0 || s.Requests == 0 {
		return ""
	}
	return fmt.Sprint("Average: ", bench.Throughput(s.BpsAverage),
		", 50%: ", bench.Throughput(s.BpsMedian),
		", 90%: ", bench.Throughput(s.Bps90),
		", 99%: ", bench.Throughput(s.Bps99),
		", Fastest: ", bench.Throughput(s.BpsFastest),
		", Slowest: ", bench.Throughput(s.BpsSlowest),
	)
}

func (s RequestSizeRange) StringByN() string {
	if s.MergedEntries <= 0 || s.Requests == 0 {
		return ""
	}
	mul := 1 / float64(s.MergedEntries)
	return fmt.Sprint("Avg: ", bench.Throughput(s.BpsAverage*mul),
		", 50%: ", bench.Throughput(s.BpsMedian*mul),
		", 90%: ", bench.Throughput(s.Bps90*mul),
		", 99%: ", bench.Throughput(s.Bps99*mul),
		", Fastest: ", bench.Throughput(s.BpsFastest*mul),
		", Slowest: ", bench.Throughput(s.BpsSlowest*mul),
	)
}

func (s *RequestSizeRange) fill(ss bench.SizeSegment) {
	ops := ss.Ops.SortByThroughputNonZero()
	if len(ops) == 0 {
		return
	}
	s.Requests = len(ops)
	s.MinSize = int(ss.Smallest)
	s.MaxSize = int(ss.Biggest)
	s.MinSizeString, s.MaxSizeString = ss.SizesString()
	s.AvgObjSize = int(ops.AvgSize())
	s.AvgDurationMillis = durToMillisF(ops.AvgDuration())
	s.BpsAverage = ops.OpThroughput().Float()
	s.BpsMedian = ops.Median(0.5).BytesPerSec().Float()
	s.Bps90 = ops.Median(0.9).BytesPerSec().Float()
	s.Bps99 = ops.Median(0.99).BytesPerSec().Float()
	s.BpsFastest = ops.Median(0.0).BytesPerSec().Float()
	s.BpsSlowest = ops.Median(1).BytesPerSec().Float()
	s.BpsPct = &[101]float64{}
	for i := range s.BpsPct[:] {
		s.BpsPct[i] = ops.Median(float64(i) / 100).BytesPerSec().Float()
	}
	s.MergedEntries = 1
}

func (s *RequestSizeRange) add(b RequestSizeRange) {
	s.Requests += b.Requests
	if b.FirstByte != nil {
		if s.FirstByte == nil {
			s.FirstByte = &TTFB{}
		}
		s.FirstByte.add(*b.FirstByte)
	}
	// Min/Max should be set
	s.AvgObjSize += b.AvgObjSize
	s.AvgDurationMillis += b.AvgDurationMillis
	s.BpsAverage += b.BpsAverage
	s.BpsMedian += b.BpsMedian
	s.BpsFastest += b.BpsFastest
	s.BpsSlowest += b.BpsSlowest
	if b.BpsPct != nil {
		if s.BpsPct == nil {
			s.BpsPct = &[101]float64{}
		}
		s.BpsPct = b.BpsPct
	}
	s.MergedEntries += b.MergedEntries
}

func (s *RequestSizeRange) fillFirstAccess(ss bench.SizeSegment) {
	if !ss.Ops.IsMultiTouch() {
		return
	}
	ss.Ops = ss.Ops.FilterFirst()
	a := RequestSizeRange{}
	a.fill(ss)
	s.FirstAccess = &a
}

// RequestSizeRanges is an array of RequestSizeRange
type RequestSizeRanges []RequestSizeRange

// SortbySize will sort the ranges by size.
func (r RequestSizeRanges) SortbySize() {
	sort.Slice(r, func(i, j int) bool {
		return r[i].MinSize < r[j].MinSize
	})
}

// FindMatching will find a matching range, or create a new range.
// New entries will not me added to r.
func (r RequestSizeRanges) FindMatching(want RequestSizeRange) (v *RequestSizeRange, found bool) {
	for i := range r {
		if want.MinSize >= r[i].MinSize && want.MaxSize <= r[i].MaxSize {
			return &r[i], true
		}
	}
	return &RequestSizeRange{
		MinSizeString: want.MinSizeString,
		MaxSizeString: want.MaxSizeString,
		MaxSize:       want.MaxSize,
		MinSize:       want.MinSize,
	}, false
}

// MultiSizedRequests contains statistics when objects have the same different size.
type MultiSizedRequests struct {
	// ByHost contains request information by host.
	// This data is not segmented.
	ByHost map[string]RequestSizeRange `json:"by_host,omitempty"`

	// BySize contains request times separated by sizes
	BySize RequestSizeRanges `json:"by_size"`

	// Total number of requests.
	Requests int `json:"requests"`

	// Average object size
	AvgObjSize int64 `json:"avg_obj_size"`

	// Skipped if too little data.
	Skipped bool `json:"skipped,omitempty"`

	// MergedEntries is a counter for the number of merged entries contained in this result.
	MergedEntries int `json:"merged_entries"`
}

func (a *MultiSizedRequests) add(b MultiSizedRequests) {
	if b.Skipped {
		return
	}
	if a.ByHost == nil {
		a.ByHost = make(map[string]RequestSizeRange)
	}
	for ep, v := range b.ByHost {
		av := a.ByHost[ep]
		av.add(v)
		a.ByHost[ep] = av
	}
	a.Requests += b.Requests
	a.AvgObjSize += b.AvgObjSize
	for _, toMerge := range b.BySize {
		dst, found := a.BySize.FindMatching(toMerge)
		dst.add(toMerge)
		if !found {
			a.BySize = append(a.BySize, toMerge)
			a.BySize.SortbySize()
		}
	}
	a.MergedEntries += b.MergedEntries
}

func (a *MultiSizedRequests) fill(ops bench.Operations, fillFirstAccess bool) {
	start, end := ops.TimeRange()
	a.Requests = len(ops)
	if len(ops) == 0 || end.Sub(start) < 100*time.Millisecond {
		a.Skipped = true
		return
	}
	a.AvgObjSize = ops.AvgSize()
	sizes := ops.SplitSizes(0.05)
	a.BySize = make([]RequestSizeRange, len(sizes))
	a.MergedEntries = 1
	var wg sync.WaitGroup
	wg.Add(len(sizes))
	for i := range sizes {
		go func(i int) {
			defer wg.Done()
			s := sizes[i]
			var r RequestSizeRange
			r.fill(s)
			if fillFirstAccess {
				r.fillFirstAccess(s)
			}
			r.FirstByte = TtfbFromBench(s.Ops.TTFB(start, end))
			// Store
			a.BySize[i] = r
		}(i)
	}
	wg.Wait()
}

// RequestAnalysisSingleSized performs analysis where all objects have equal size.
func RequestAnalysisSingleSized(o bench.Operations, allThreads bool) *SingleSizedRequests {
	var res SingleSizedRequests

	// Single type, require one operation per thread.
	start, end := o.ActiveTimeRange(allThreads)
	active := o.FilterInsideRange(start, end)

	if len(active) == 0 {
		res.Skipped = true
		return &res
	}
	res.MergedEntries = 1
	res.fill(active)
	res.fillFirstLast(o)
	res.HostNames.SetSlice(o.Endpoints())
	res.ByHost = RequestAnalysisHostsSingleSized(o)
	if len(res.HostNames) != len(res.ByHost) {
		res.HostNames.SetSlice(o.ClientIDs(clientAsHostPrefix))
	}
	return &res
}

// RequestAnalysisHostsSingleSized performs host analysis where all objects have equal size.
func RequestAnalysisHostsSingleSized(o bench.Operations) map[string]SingleSizedRequests {
	eps := o.SortSplitByEndpoint()
	if len(eps) == 1 {
		cl := o.SortSplitByClient(clientAsHostPrefix)
		if len(cl) > 1 {
			eps = cl
		}
		if len(eps) == 1 {
			return nil
		}
	}
	res := make(map[string]SingleSizedRequests, len(eps))
	var wg sync.WaitGroup
	var mu sync.Mutex
	wg.Add(len(eps))
	for ep, ops := range eps {
		go func(ep string, ops bench.Operations) {
			defer wg.Done()
			if len(ops) <= 1 {
				return
			}
			a := SingleSizedRequests{}
			a.fill(ops)
			mu.Lock()
			res[ep] = a
			mu.Unlock()
		}(ep, ops)
	}
	wg.Wait()
	return res
}

// RequestAnalysisMultiSized performs analysis where objects have different sizes.
func RequestAnalysisMultiSized(o bench.Operations, allThreads bool) *MultiSizedRequests {
	var res MultiSizedRequests
	// Single type, require one operation per thread.
	start, end := o.ActiveTimeRange(allThreads)
	active := o.FilterInsideRange(start, end)

	res.Requests = len(active)
	if len(active) == 0 {
		res.Skipped = true
		return &res
	}
	res.fill(active, true)
	res.ByHost = RequestAnalysisHostsMultiSized(active)
	res.MergedEntries = 1
	return &res
}

// RequestAnalysisHostsMultiSized performs host analysis where objects have different sizes.
func RequestAnalysisHostsMultiSized(o bench.Operations) map[string]RequestSizeRange {
	start, end := o.TimeRange()
	eps := o.SortSplitByEndpoint()
	if len(eps) == 1 {
		cl := o.SortSplitByClient(clientAsHostPrefix)
		if len(cl) > 1 {
			eps = cl
		}
		if len(eps) == 1 {
			return nil
		}
	}

	res := make(map[string]RequestSizeRange, len(eps))
	var wg sync.WaitGroup
	var mu sync.Mutex
	wg.Add(len(eps))
	for ep, ops := range eps {
		go func(ep string, ops bench.Operations) {
			defer wg.Done()
			if len(ops) <= 1 {
				return
			}
			a := RequestSizeRange{}
			a.fill(ops.SingleSizeSegment())
			a.FirstByte = TtfbFromBench(ops.TTFB(start, end))
			mu.Lock()
			res[ep] = a
			mu.Unlock()
		}(ep, ops)
	}
	wg.Wait()
	return res
}

// durToMillis converts a duration to milliseconds.
// Rounded to nearest.
func durToMillis(d time.Duration) int {
	return int(d.Round(time.Millisecond) / time.Millisecond)
}

func durToMillisF(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
