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
	"sync"
	"time"

	"github.com/minio/warp/pkg/bench"
)

// SingleSizedRequests contains statistics when all objects have the same size.
type SingleSizedRequests struct {
	// Request times by host.
	ByHost     map[string]SingleSizedRequests `json:"by_host,omitempty"`
	LastAccess *SingleSizedRequests           `json:"last_access,omitempty"`

	// FirstAccess is filled if the same object is accessed multiple times.
	// This records the first touch of the object.
	FirstAccess *SingleSizedRequests `json:"first_access,omitempty"`

	// Time to first byte if applicable.
	FirstByte *TTFB `json:"first_byte,omitempty"`

	// Host names, sorted.
	HostNames []string

	// DurPct is duration percentiles.
	DurPct [101]int `json:"dur_percentiles_millis"`

	// Median request duration.
	DurMedianMillis int `json:"dur_median_millis"`

	// Fastest request time.
	FastestMillis int `json:"fastest_millis"`

	// Slowest request time.
	SlowestMillis int `json:"slowest_millis"`

	// StdDev is the standard deviation of requests.
	StdDev int `json:"std_dev_millis"`

	// 99% request time.
	Dur99Millis int `json:"dur_99_millis"`

	// 90% request time.
	Dur90Millis int `json:"dur_90_millis"`

	// Average request duration.
	DurAvgMillis int `json:"dur_avg_millis"`

	// Total number of requests.
	Requests int `json:"requests"`

	// Object size per operation. Can be 0.
	ObjSize int64 `json:"obj_size"`

	// Skipped if too little data.
	Skipped bool `json:"skipped"`
}

func (a *SingleSizedRequests) fill(ops bench.Operations) {
	start, end := ops.TimeRange()
	ops.SortByDuration()
	a.Requests = len(ops)
	a.ObjSize = ops.FirstObjSize()
	a.DurAvgMillis = durToMillis(ops.AvgDuration())
	a.StdDev = durToMillis(ops.StdDev())
	a.DurMedianMillis = durToMillis(ops.Median(0.5).Duration())
	a.Dur90Millis = durToMillis(ops.Median(0.9).Duration())
	a.Dur99Millis = durToMillis(ops.Median(0.99).Duration())
	a.SlowestMillis = durToMillis(ops.Median(1).Duration())
	a.FastestMillis = durToMillis(ops.Median(0).Duration())
	a.FirstByte = TtfbFromBench(ops.TTFB(start, end))
	for i := range a.DurPct[:] {
		a.DurPct[i] = durToMillis(ops.Median(float64(i) / 100).Duration())
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
	BpsPct [101]float64 `json:"bps_percentiles"`

	BpsMedian         float64 `json:"bps_median"`
	AvgDurationMillis int     `json:"avg_duration_millis"`

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
	for i := range r.BpsPct[:] {
		r.BpsPct[i] = s.Ops.Median(float64(i) / 100).BytesPerSec().Float()
	}
}

func (r *RequestSizeRange) fillFirst(s bench.SizeSegment) {
	if !s.Ops.IsMultiTouch() {
		return
	}
	s.Ops = s.Ops.FilterFirst()
	a := RequestSizeRange{}
	a.fill(s)
	a.FirstByte = TtfbFromBench(s.Ops.TTFB(s.Ops.TimeRange()))

	r.FirstAccess = &a
}

// MultiSizedRequests contains statistics when objects have the same different size.
type MultiSizedRequests struct {
	// ByHost contains request information by host.
	ByHost map[string]RequestSizeRange `json:"by_host,omitempty"`

	// BySize contains request times separated by sizes
	BySize []RequestSizeRange `json:"by_size"`

	// HostNames are the host names, sorted.
	HostNames []string

	// Total number of requests.
	Requests int `json:"requests"`
	// Average object size
	AvgObjSize int64 `json:"avg_obj_size"`

	// Skipped if too little data.
	Skipped bool `json:"skipped"`
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
	a.BySize = make([]RequestSizeRange, len(sizes))
	var wg sync.WaitGroup
	wg.Add(len(sizes))
	for i := range sizes {
		go func(i int) {
			defer wg.Done()
			s := sizes[i]
			var r RequestSizeRange
			r.fill(s)
			r.fillFirst(s)
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
	res.fill(active)
	res.fillFirstLast(o)
	res.HostNames = o.Endpoints()
	res.ByHost = RequestAnalysisHostsSingleSized(o)
	if len(res.HostNames) != len(res.ByHost) {
		res.HostNames = o.ClientIDs()
	}
	return &res
}

// RequestAnalysisHostsSingleSized performs host analysis where all objects have equal size.
func RequestAnalysisHostsSingleSized(o bench.Operations) map[string]SingleSizedRequests {
	eps := o.SortSplitByEndpoint()
	if len(eps) == 1 {
		cl := o.SortSplitByClient()
		if len(cl) > 1 {
			eps = cl
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
	res.fill(active)
	res.ByHost = RequestAnalysisHostsMultiSized(active)
	res.HostNames = active.Endpoints()
	if len(res.HostNames) != len(res.ByHost) {
		res.HostNames = o.ClientIDs()
	}
	return &res
}

// RequestAnalysisHostsMultiSized performs host analysis where objects have different sizes.
func RequestAnalysisHostsMultiSized(o bench.Operations) map[string]RequestSizeRange {
	start, end := o.TimeRange()
	eps := o.SortSplitByEndpoint()
	if len(eps) == 1 {
		cl := o.SortSplitByClient()
		if len(cl) > 1 {
			eps = cl
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
