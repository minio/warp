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
	"sync"
	"time"

	"github.com/minio/warp/pkg/bench"
)

// Aggregated contains aggregated data for a single benchmark run.
type Aggregated struct {
	Type       string      `json:"type"`
	Mixed      bool        `json:"mixed"`
	Operations []Operation `json:"operations,omitempty"`
	// MixedServerStats and MixedThroughputByHost is populated only when data is mixed.
	MixedServerStats      *Throughput           `json:"mixed_server_stats,omitempty"`
	MixedThroughputByHost map[string]Throughput `json:"mixed_throughput_by_host,omitempty"`
}

// Operation returns statistics for a single operation type.
type Operation struct {
	// Operation type
	Type string `json:"type"`
	// N is the number of operations.
	N int `json:"n"`
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
	// Number of warp clients.
	Clients int `json:"clients"`
	// Numbers of hosts
	Hosts int `json:"hosts"`
	// HostNames are sorted names of hosts
	HostNames []string `json:"host_names"`
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

// SegmentDurFn accepts a total time and should return the duration used for each segment.
type SegmentDurFn func(total time.Duration) time.Duration

type Options struct {
	Prefiltered bool
	DurFunc     SegmentDurFn
	SkipDur     time.Duration
}

// Aggregate returns statistics when only a single operation was running concurrently.
func Aggregate(o bench.Operations, opts Options) Aggregated {
	o.SortByStartTime()
	types := o.OpTypes()
	a := Aggregated{
		Type:                  "single",
		Mixed:                 false,
		Operations:            nil,
		MixedServerStats:      nil,
		MixedThroughputByHost: nil,
	}
	isMixed := o.IsMixed()
	opts.Prefiltered = opts.Prefiltered || o.HasError()

	// Fill mixed only parts...
	if isMixed {
		a.Mixed = true
		a.Type = "mixed"
		var ops bench.Operations
		errs := o.FilterErrors()
		if len(errs) == 0 {
			start, end := o.ActiveTimeRange(!opts.Prefiltered)
			start.Add(opts.SkipDur)
			o = o.FilterInsideRange(start, end)
			ops = o
		} else {
			if opts.SkipDur > 0 {
				start, end := o.TimeRange()
				start.Add(opts.SkipDur)
				o = o.FilterInsideRange(start, end)
			}
			ops = o.FilterSuccessful()
		}

		total := ops.Total(false)
		total.Errors = len(errs)
		a.MixedServerStats = &Throughput{}
		a.MixedServerStats.fill(total)

		segmentDur := opts.DurFunc(total.Duration())
		segs := ops.Segment(bench.SegmentOptions{
			From:           time.Time{},
			PerSegDuration: segmentDur,
			AllThreads:     false,
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
		var wg sync.WaitGroup
		var mu sync.Mutex
		wg.Add(len(eps))
		for i := range eps {
			go func(i int) {
				defer wg.Done()
				ep := eps[i]
				ops := ops.FilterByEndpoint(ep)
				t := Throughput{}
				t.fill(ops.Total(false))
				if len(errs) > 0 {
					errs := errs.FilterByEndpoint(ep)
					t.Errors = len(errs)
				}
				mu.Lock()
				a.MixedThroughputByHost[ep] = t
				mu.Unlock()
			}(i)
		}
		wg.Wait()
		opts.Prefiltered = true
		opts.SkipDur = 0
	}

	res := make([]Operation, len(types))
	var wg sync.WaitGroup
	wg.Add(len(types))
	for i := range types {
		go func(i int) {
			typ := types[i]
			a := Operation{}
			// Save a and mark as done.
			defer func() {
				res[i] = a
				wg.Done()
			}()
			a.Type = typ
			ops := o.FilterByOp(typ)
			if opts.SkipDur > 0 {
				start, end := ops.TimeRange()
				start = start.Add(opts.SkipDur)
				ops = ops.FilterInsideRange(start, end)
			}
			errs := ops.FilterErrors()
			if len(errs) > 0 {
				a.Errors = len(errs)
				for _, err := range errs {
					if len(a.FirstErrors) >= 10 {
						break
					}
					a.FirstErrors = append(a.FirstErrors, fmt.Sprintf("%s, %s: %v", err.Endpoint, err.End.Round(time.Second), err.Err))
				}
			}

			segmentDur := opts.DurFunc(ops.Duration())
			segs := ops.Segment(bench.SegmentOptions{
				From:           time.Time{},
				PerSegDuration: segmentDur,
				AllThreads:     !opts.Prefiltered,
				MultiOp:        false,
			})
			a.N = len(ops)
			if len(segs) <= 1 {
				a.Skipped = true
				return
			}

			allOps := ops
			// Remove errored request from further analysis
			if len(errs) > 0 {
				ops = ops.FilterSuccessful()
				if len(ops) == 0 {
					a.Skipped = true
					return
				}
			}
			total := ops.Total(!opts.Prefiltered)
			a.StartTime, a.EndTime = ops.TimeRange()
			a.Throughput.fill(total)
			a.Throughput.Segmented = &ThroughputSegmented{
				SegmentDurationMillis: durToMillis(segmentDur),
			}
			a.Throughput.Segmented.fill(segs, total)
			a.ObjectsPerOperation = ops.FirstObjPerOp()
			a.Concurrency = ops.Threads()
			a.Clients = ops.Clients()
			a.Hosts = ops.Hosts()
			a.HostNames = ops.Endpoints()

			if !ops.MultipleSizes() {
				a.SingleSizedRequests = RequestAnalysisSingleSized(ops, !opts.Prefiltered)
			} else {
				a.MultiSizedRequests = RequestAnalysisMultiSized(ops, !opts.Prefiltered)
			}

			eps := ops.Endpoints()
			a.ThroughputByHost = make(map[string]Throughput, len(eps))
			var epMu sync.Mutex
			var epWg sync.WaitGroup
			epWg.Add(len(eps))
			for _, ep := range eps {
				go func(ep string) {
					defer epWg.Done()
					// Use all ops to include errors.
					ops := allOps.FilterByEndpoint(ep)

					segs := ops.Segment(bench.SegmentOptions{
						From:           time.Time{},
						PerSegDuration: segmentDur,
						AllThreads:     false,
					})

					var host Throughput
					errs := ops.FilterErrors()
					if len(errs) > 0 {
						ops = ops.FilterSuccessful()
						if len(ops) == 0 {
							return
						}
					}
					total := ops.Total(false)
					total.Errors = len(errs)
					host.fill(total)
					if len(segs) > 1 {
						host.Segmented = &ThroughputSegmented{
							SegmentDurationMillis: durToMillis(segmentDur),
						}
						host.Segmented.fill(segs, total)
					}
					epMu.Lock()
					a.ThroughputByHost[ep] = host
					epMu.Unlock()
				}(ep)
			}
			epWg.Wait()
		}(i)
	}
	wg.Wait()
	a.Operations = res
	return a
}
