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
	// segmentDur records the duration used for segmenting the data.
	segmentDur time.Duration
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

		if errs := ops.FilterErrors(); len(errs) > 0 {
			a.Errors = len(errs)
			for _, err := range errs {
				if len(a.FirstErrors) >= 10 {
					break
				}
				a.FirstErrors = append(a.FirstErrors, fmt.Sprintf("%s, %s: %v", err.Endpoint, err.End.Round(time.Second), err.Err))
			}
		}

		// Remove errored request from further analysis
		allOps := ops
		ops = ops.FilterSuccessful()
		if len(ops) == 0 {
			a.Skipped = true
			continue
		}

		segs := ops.Segment(bench.SegmentOptions{
			From:           time.Time{},
			PerSegDuration: segmentDur,
			AllThreads:     !isMixed,
		})
		if len(segs) <= 1 {
			a.Skipped = true
			res = append(res, a)
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

		if !ops.MultipleSizes() {
			a.SingleSizedRequests = RequestAnalysisSingleSized(ops, !isMixed)
		} else {
			a.MultiSizedRequests = RequestAnalysisMultiSized(ops, !isMixed)
		}

		eps := ops.Endpoints()
		a.ThroughputByHost = make(map[string]Throughput, len(eps))
		for _, ep := range eps {
			// Use all ops to include errors.
			ops := allOps.FilterByEndpoint(ep)
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

// HasDuration returns whether the aggregation has been created with the specified duration.
func (a Aggregated) HasDuration(d time.Duration) bool {
	return a.segmentDur == d
}
