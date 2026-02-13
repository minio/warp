/*
 * Warp (C) 2019-2025 MinIO, Inc.
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
	"bytes"
	"fmt"
	"io"
	"maps"
	"math/bits"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fatih/color"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
)

const currentVersion = 2

// Realtime is a collection of realtime aggregated data.
type Realtime struct {
	DataVersion int    `json:"v"`
	Commandline string `json:"commandline"`
	Final       bool   `json:"final"`
	WarpVersion string `json:"warp_version,omitempty"`
	WarpCommit  string `json:"warp_commit,omitempty"`
	WarpDate    string `json:"warp_date,omitempty"`

	Total    LiveAggregate             `json:"total"`
	ByOpType map[string]*LiveAggregate `json:"by_op_type,omitempty"`
	// Per-host aggregates for time-series visualization.
	ByHost        map[string]*LiveAggregate `json:"by_host,omitempty"`
	ByObjLog2Size map[int]*LiveAggregate    `json:"by_obj_log_2_size,omitempty"`
	// Per-client aggregates for time-series visualization.
	ByClient   map[string]*LiveAggregate         `json:"by_client,omitempty"`
	ByCategory map[bench.Category]*LiveAggregate `json:"by_category,omitempty"`
}

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
	// Concurrency is the number of threads seen.
	Concurrency int `json:"concurrency"`

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

	throughput liveThroughput

	// Throughput information.
	Throughput Throughput `json:"throughput"`

	// ThroughputByHost information. Without segments.
	ThroughputByHost map[string]Throughput `json:"throughput_by_host"`

	// ThroughputByClient information. Without segments.
	ThroughputByClient map[string]Throughput `json:"throughput_by_client"`

	// Requests segmented.
	// Indexed by client.
	Requests map[string]RequestSegments `json:"requests_by_client"`

	requests map[string]liveRequests

	threadIDs map[uint32]struct{}
}

type RequestSegment struct {
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`

	// Populated if requests are of difference object sizes.
	Multi *MultiSizedRequests `json:"multi_sized_requests,omitempty"`

	// Populated if requests are all of same object size.
	Single *SingleSizedRequests `json:"single_sized_requests,omitempty"`
}

type RequestSegments []RequestSegment

const maxFirstErrors = 10

// Add operation to aggregate.
func (l *LiveAggregate) Add(o bench.Operation) {
	l.TotalRequests++
	l.TotalObjects += o.ObjPerOp
	l.TotalBytes += o.Size
	if o.ClientID != "" {
		l.Clients.Add(o.ClientID)
	}
	if l.threadIDs == nil {
		l.threadIDs = make(map[uint32]struct{})
	}
	if _, ok := l.threadIDs[o.Thread]; !ok {
		l.threadIDs[o.Thread] = struct{}{}
	}
	l.Hosts.Add(o.Endpoint)
	if l.StartTime.IsZero() || l.StartTime.After(o.Start) {
		l.StartTime = o.Start
	}
	if l.EndTime.Before(o.End) {
		l.EndTime = o.End
	}
	if o.Err != "" {
		if len(l.FirstErrors) < maxFirstErrors {
			l.FirstErrors = append(l.FirstErrors, o.Err)
		}
		l.TotalErrors++
	}
	if l.ThroughputByHost == nil {
		l.ThroughputByHost = make(map[string]Throughput)
	}
	if l.ThroughputByClient == nil {
		l.ThroughputByClient = make(map[string]Throughput)
	}
	l.ThroughputByHost[o.Endpoint] = l.ThroughputByHost[o.Endpoint].Add(o)
	l.ThroughputByClient[o.ClientID] = l.ThroughputByClient[o.ClientID].Add(o)
	l.throughput.Add(o)
	if l.requests == nil {
		l.requests = make(map[string]liveRequests, 10)
	}
	req := l.requests[o.ClientID]
	req.Add(o)
	l.requests[o.ClientID] = req
}

// Merge l2 into l.
func (l *LiveAggregate) Merge(l2 LiveAggregate) {
	l.Throughput.Merge(l2.Throughput)
	if l.Requests == nil {
		l.Requests = make(map[string]RequestSegments)
	}
	if len(l2.Requests) > 0 {
		maps.Copy(l.Requests, l2.Requests)
	}
	l.Concurrency += l2.Concurrency
	l.TotalBytes += l2.TotalBytes
	l.TotalObjects += l2.TotalObjects
	l.TotalErrors += l2.TotalErrors
	l.FirstErrors = append(l.FirstErrors, l2.FirstErrors...)
	if len(l.FirstErrors) > maxFirstErrors {
		l.FirstErrors = l.FirstErrors[:maxFirstErrors]
	}
	l.Clients.AddMap(l2.Clients)
	l.Hosts.AddMap(l2.Hosts)
	l.TotalRequests += l2.TotalRequests
	if l.StartTime.IsZero() || l2.StartTime.Before(l.StartTime) {
		l.StartTime = l2.StartTime
	}
	if l2.EndTime.After(l.EndTime) {
		l.EndTime = l2.EndTime
	}
	if l.ThroughputByHost == nil && len(l2.ThroughputByHost) != 0 {
		l.ThroughputByHost = l2.ThroughputByHost
	} else {
		for k, v := range l2.ThroughputByHost {
			v0 := l.ThroughputByHost[k]
			v0.Merge(v)
			l.ThroughputByHost[k] = v0
		}
	}
	if l.ThroughputByClient == nil && len(l2.ThroughputByClient) != 0 {
		l.ThroughputByClient = l2.ThroughputByClient
	} else {
		for k, v := range l2.ThroughputByClient {
			v0 := l.ThroughputByClient[k]
			v0.Merge(v)
			l.ThroughputByClient[k] = v0
		}
	}
	if l.Title == "" && l2.Title != "" {
		l.Title = l2.Title
	}
}

// Update returns a temporary update without finalizing.
// The update will have no references to the live version.
func (l LiveAggregate) Update() LiveAggregate {
	dst := l
	dst.Throughput = dst.throughput.asThroughput()
	// Clear maps...
	dst.ThroughputByHost = nil
	dst.ThroughputByClient = nil
	dst.throughput = liveThroughput{}
	dst.Concurrency = len(l.threadIDs)

	// TODO: PROBABLY NOT NEEDED AND FASTER TO REMOVE...
	dst.Requests = make(map[string]RequestSegments, len(l.requests))
	for clientID, req := range l.requests {
		startTime := time.Unix(req.firstSeg, 0)
		if req.client != "" {
			reqs := make([]RequestSegment, 0, len(req.single)+len(req.multi))
			for i := range req.multi {
				reqs = append(reqs, RequestSegment{StartTime: startTime, EndTime: startTime.Add(time.Second * requestSegmentsDur), Multi: &req.multi[i]})
				startTime = startTime.Add(time.Second * requestSegmentsDur)
			}
			for i := range req.single {
				reqs = append(reqs, RequestSegment{StartTime: startTime, EndTime: startTime.Add(time.Second * requestSegmentsDur), Single: &req.single[i]})
				startTime = startTime.Add(time.Second * requestSegmentsDur)
			}
			dst.Requests[clientID] = reqs
		}
	}

	dst.requests = nil
	dst.Clients = l.Clients.Clone()
	dst.Hosts = l.Hosts.Clone()
	dst.FirstErrors = slices.Clone(l.FirstErrors)
	dst.Title += " (update)"

	return dst
}

func (l *LiveAggregate) Finalize() {
	l.Throughput = l.throughput.asThroughput()
	for clientID, reqs := range l.requests {
		reqs.cycle()
		startTime := time.Unix(reqs.firstSeg, 0)
		dst := l.Requests[clientID]
		l.Concurrency = len(l.threadIDs)
		for i := range reqs.multi {
			dst = append(dst, RequestSegment{StartTime: startTime, EndTime: startTime.Add(time.Second * requestSegmentsDur), Multi: &reqs.multi[i]})
			startTime = startTime.Add(time.Second * requestSegmentsDur)
		}
		for i := range reqs.single {
			dst = append(dst, RequestSegment{StartTime: startTime, EndTime: startTime.Add(time.Second * requestSegmentsDur), Single: &reqs.single[i]})
			startTime = startTime.Add(time.Second * requestSegmentsDur)
		}
		if l.Requests == nil {
			l.Requests = make(map[string]RequestSegments, 1)
		}
		l.Requests[clientID] = dst
	}
	l.Title += " (Final)"
}

// ReportOptions provides options to report generation.
type ReportOptions struct {
	Details  bool
	Color    bool
	SkipReqs bool
	OnlyOps  map[string]struct{}
}

func (o ReportOptions) printfColor(dst io.Writer) func(ca color.Attribute, format string, args ...any) {
	return func(ca color.Attribute, format string, args ...any) {
		if !o.Color {
			fmt.Fprintf(dst, format, args...)
			return
		}
		color.New(ca).Fprintf(dst, format, args...)
	}
}

func (o ReportOptions) printf(ca color.Attribute, format string, args ...any) string {
	if !o.Color {
		return fmt.Sprintf(format, args...)
	}
	return color.New(ca).Sprintf(format, args...)
}

func (o ReportOptions) col(ca color.Attribute, s string) string {
	if !o.Color {
		return s
	}
	return color.New(ca).Sprint(s)
}

func (l LiveAggregate) Report(op string, o ReportOptions) string {
	dst := bytes.NewBuffer(make([]byte, 0, 1024))
	printfColor := o.printfColor(dst)
	col := o.col
	details := o.Details
	data := l

	if data.Throughput.Segmented == nil || len(data.Throughput.Segmented.Segments) < 2 {
		printfColor(color.FgHiYellow, "Skipping %s too few samples. Longer benchmark run required for reliable results.\n\n", op)
		if data.TotalErrors > 0 {
			printfColor(color.FgHiRed, "Errors: %d\n", data.TotalErrors)
			if details {
				console.SetColor("Print", color.New(color.FgWhite))
				printfColor(color.FgWhite, "- First Errors:\n")
				for _, err := range data.FirstErrors {
					printfColor(color.FgWhite, " * %s\n", err)
				}
			}
			dst.WriteByte('\n')
		}
		return dst.String()
	}

	opCol := o.printf(color.FgHiYellow, "%s", op)
	if details {
		hostsString := ""
		if len(data.Hosts) > 1 {
			hostsString = fmt.Sprintf(" Hosts: %d.", len(data.Hosts))
		}
		if len(data.Clients) > 1 {
			hostsString = fmt.Sprintf("%s Warp Instances: %d.", hostsString, len(data.Clients))
		}
		sz := ""
		if data.TotalBytes > 0 {
			sz = fmt.Sprintf("Size: %d bytes. ", data.TotalBytes/int64(data.TotalObjects))
		}
		printfColor(color.FgWhite, "Report: %v (%d reqs). Ran %v\n", opCol, data.TotalRequests, data.Throughput.StringDuration())
		printfColor(color.FgWhite, " * Objects per request: %d. %vConcurrency: %d.%s\n",
			data.TotalObjects/data.TotalRequests, sz,
			data.Concurrency, hostsString)
	} else {
		printfColor(color.FgHiWhite, "Report: %s. Concurrency: %d. Ran: %v\n", opCol, data.Concurrency, time.Duration(data.Throughput.MeasureDurationMillis)*time.Millisecond)
	}
	printfColor(color.FgWhite, " * Average: %v\n", col(color.FgWhite, data.Throughput.StringDetails(details)))
	if data.TotalErrors > 0 {
		printfColor(color.FgHiRed, " * Errors: %d\n", data.TotalErrors)
		if details {
			console.SetColor("Print", color.New(color.FgWhite))
			printfColor(color.FgWhite, " - First Errors:\n")
			for _, err := range data.FirstErrors {
				printfColor(color.FgWhite, "   * %s\n", err)
			}
		}
	}

	if !o.SkipReqs {
		ss, ms := mergeRequests(data.Requests)
		if ss.MergedEntries > 0 {
			printfColor(color.FgWhite, " * Reqs: %s\n", ss.StringByN())
			if ss.FirstByte != nil {
				printfColor(color.FgWhite, " * TTFB: %v\n", ss.FirstByte.StringByN(ss.MergedEntries))
			}
		}
		if ms.MergedEntries > 0 {
			ms.BySize.SortbySize()
			for _, s := range ms.BySize {
				printfColor(color.FgWhite, "\nRequest size %s -> %s . Requests: %d\n", s.MinSizeString, s.MaxSizeString, s.Requests)
				printfColor(color.FgWhite, " * Reqs: %s \n", s.StringByN())
				if s.FirstByte != nil {
					printfColor(color.FgWhite, " * TTFB: %s\n", s.FirstByte.StringByN(s.MergedEntries))
				}
			}
		}
	}
	dst.WriteByte('\n')

	if len(data.Hosts) > 1 {
		printfColor(color.FgHiWhite, "Throughput by host:\n")
		for _, ep := range data.Hosts.Slice() {
			tp := data.ThroughputByHost[ep]
			printfColor(color.FgWhite, " * %s:", ep)
			printfColor(color.FgHiWhite, " Avg: %v", tp.StringDetails(details))
			if tp.Errors > 0 {
				printfColor(color.FgHiRed, " - Errors: %d", data.TotalErrors)
			}
			dst.WriteByte('\n')
		}
		dst.WriteByte('\n')
	}

	if len(data.Clients) > 1 {
		printfColor(color.FgHiWhite, "Throughput by client:\n")
		for i, client := range data.Clients.Slice() {
			tp := data.ThroughputByClient[client]
			printfColor(color.FgWhite, "Client %d throughput: ", i+1)
			printfColor(color.FgHiWhite, "%s\n", tp.StringDetails(o.Details))
			if o.SkipReqs || !o.Details {
				continue
			}
			var ss SingleSizedRequests
			var ms MultiSizedRequests
			for _, seg := range data.Requests[client] {
				if seg.Single != nil {
					ss.add(*seg.Single)
				}
				if seg.Multi != nil {
					ms.add(*seg.Multi)
				}
			}
			if ss.MergedEntries > 0 {
				printfColor(color.FgWhite, " * Reqs: %s", ss.StringByN())
				if ss.FirstByte != nil {
					printfColor(color.FgWhite, "\n * TTFB: %v\n", ss.FirstByte.StringByN(ss.MergedEntries))
				} else {
					dst.WriteByte('\n')
				}
			}
			if ms.MergedEntries > 0 {
				ms.BySize.SortbySize()
				for _, s := range ms.BySize {
					printfColor(color.FgWhite, "\nRequest size %s -> %s . Requests: %d\n", s.MinSizeString, s.MaxSizeString, s.Requests)
					printfColor(color.FgWhite, " * Reqs: %s", s.StringByN())
					if s.FirstByte != nil {
						printfColor(color.FgWhite, ", TTFB: %s\n", s.FirstByte.StringByN(s.MergedEntries))
					} else {
						dst.WriteByte('\n')
					}
				}
			}
			dst.WriteByte('\n')
		}
		dst.WriteByte('\n')
	}

	if segs := data.Throughput.Segmented; segs != nil {
		dur := time.Millisecond * time.Duration(segs.SegmentDurationMillis)
		printfColor(color.FgHiWhite, "Throughput, split into %d x 1s:\n", len(segs.Segments))
		printfColor(color.FgWhite, " * Fastest: %v\n", SegmentSmall{BPS: segs.FastestBPS, OPS: segs.FastestOPS, Start: segs.FastestStart}.StringLong(dur, details))
		printfColor(color.FgWhite, " * 50%% Median: %v\n", SegmentSmall{BPS: segs.MedianBPS, OPS: segs.MedianOPS, Start: segs.MedianStart}.StringLong(dur, details))
		printfColor(color.FgWhite, " * Slowest: %v\n", SegmentSmall{BPS: segs.SlowestBPS, OPS: segs.SlowestOPS, Start: segs.SlowestStart}.StringLong(dur, details))
	}
	return dst.String()
}

func (r *Realtime) Report(o ReportOptions) *bytes.Buffer {
	dst := bytes.NewBuffer(make([]byte, 0, 1024))
	printfColor := o.printfColor(dst)

	wroteOps := 0
	allOps := stringKeysSorted(r.ByOpType)
	for _, op := range allOps {
		if len(o.OnlyOps) > 0 {
			if _, ok := o.OnlyOps[strings.ToUpper(op)]; !ok {
				continue
			}
		}
		data := r.ByOpType[op]
		if wroteOps > 0 {
			printfColor(color.FgHiBlue, "\n──────────────────────────────────\n\n")
		}
		dst.WriteString(data.Report(op, o))
		wroteOps++
	}
	dst.WriteByte('\n')
	if len(allOps) > 1 && !r.overLappingOps() {
		if len(allOps) > 1 {
			if wroteOps > 0 {
				printfColor(color.FgHiBlue, "\n──────────────────────────────────\n\n")
			}
		}
		o.SkipReqs = len(allOps) > 1
		dst.WriteString(r.Total.Report("Total", o))
		o.SkipReqs = false
	}

	return dst
}

func finalizeValues[K comparable](m map[K]*LiveAggregate) {
	for _, v := range m {
		if v != nil {
			v.Finalize()
		}
	}
}

func mergeValues[K comparable](toP *map[K]*LiveAggregate, from map[K]*LiveAggregate) {
	to := *toP
	if to == nil {
		to = make(map[K]*LiveAggregate, len(from))
	}
	for k, v := range from {
		if v != nil {
			dst := to[k]
			if dst == nil {
				dst = &LiveAggregate{Title: v.Title}
			}
			dst.Merge(*v)
			to[k] = dst
		}
	}
	*toP = to
}

func (r *Realtime) Finalize() {
	finalizeValues(r.ByOpType)
	finalizeValues(r.ByHost)
	finalizeValues(r.ByObjLog2Size)
	finalizeValues(r.ByClient)
	finalizeValues(r.ByCategory)
	r.Total.Finalize()
	r.Final = true
}

func (r *Realtime) Merge(other *Realtime) {
	if other == nil {
		return
	}
	mergeValues(&r.ByOpType, other.ByOpType)
	mergeValues(&r.ByHost, other.ByHost)
	mergeValues(&r.ByObjLog2Size, other.ByObjLog2Size)
	mergeValues(&r.ByClient, other.ByClient)
	mergeValues(&r.ByCategory, other.ByCategory)
	r.Total.Merge(other.Total)
	r.Final = other.Final && (r.Final || r.Total.TotalRequests == 0)
	setIfEmpty := func(dst *string, alt string) {
		if dst == nil {
			return
		}
		if *dst == "" {
			*dst = alt
		}
	}
	setIfEmpty(&r.WarpCommit, other.WarpCommit)
	setIfEmpty(&r.WarpDate, other.WarpDate)
	setIfEmpty(&r.WarpVersion, other.WarpVersion)
	setIfEmpty(&r.Commandline, other.Commandline)
	if r.DataVersion == 0 {
		r.DataVersion = other.DataVersion
	}
}

func newRealTime() Realtime {
	return Realtime{
		DataVersion:   currentVersion,
		Total:         LiveAggregate{Title: "Total"},
		ByOpType:      make(map[string]*LiveAggregate),
		ByHost:        make(map[string]*LiveAggregate),
		ByObjLog2Size: make(map[int]*LiveAggregate),
		ByClient:      make(map[string]*LiveAggregate),
		ByCategory:    make(map[bench.Category]*LiveAggregate),
	}
}

// Live collects operations and update requests.
func Live(ops <-chan bench.Operation, updates chan UpdateReq, clientID string, extra []chan<- bench.Operation) *Realtime {
	a := newRealTime()
	var reset atomic.Bool
	var update atomic.Pointer[Realtime]
	if updates != nil {
		done := make(chan struct{})
		defer func() {
			close(done)
			for _, c := range extra {
				close(c)
			}
		}()
		go func() {
			var finalQ []UpdateReq
			defer func() {
				if len(finalQ) > 0 {
					go func() {
						for _, r := range finalQ {
							updates <- r
						}
					}()
				}
			}()
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
					if r.Final {
						finalQ = append(finalQ, r)
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
		dur := op.End.Sub(op.Start)
		if dur < 0 {
			op.End = op.Start
			op.Err += "Negative duration"
		} else {
			// Rewrite op.End in case a non-monotonic adjustment has been made.
			op.End = op.Start.Add(dur)
		}
		if reset.CompareAndSwap(true, false) {
			a = newRealTime()
		}
		if clientID != "" {
			op.ClientID = clientID
		}
		var wg sync.WaitGroup
		wg.Add(len(extra))
		for _, c := range extra {
			go func() {
				c <- op
				wg.Done()
			}()
		}
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
			if op.Size == 0 {
				return
			}
			l2Size := bits.Len64(uint64(op.Size))
			bySize := a.ByObjLog2Size[l2Size]
			if bySize == nil {
				start := 0
				if l2Size > 0 {
					start = 1 >> (l2Size - 1)
				}
				bySize = &LiveAggregate{Title: fmt.Sprintf("Size: %d->%d", start, (1<<l2Size)-1)}
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
			u := Realtime{
				Total:       a.Total.Update(),
				ByOpType:    make(map[string]*LiveAggregate, len(a.ByOpType)),
				ByHost:      make(map[string]*LiveAggregate, len(a.ByHost)),
				ByClient:    make(map[string]*LiveAggregate, len(a.ByClient)),
				DataVersion: currentVersion,
			}
			for k, v := range a.ByOpType {
				if v != nil {
					clone := v.Update()
					u.ByOpType[k] = &clone
				}
			}
			for k, v := range a.ByHost {
				if v != nil {
					clone := v.Update()
					u.ByHost[k] = &clone
				}
			}
			for k, v := range a.ByClient {
				if v != nil {
					clone := v.Update()
					u.ByClient[k] = &clone
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
	if startUnix < l.segmentsStart {
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
			seg.ops++
			seg.objs += float64(o.ObjPerOp)
			seg.bytes += float64(o.Size)
			continue
		}

		seg.partialOps++
		segStartNano := (startUnix + int64(i)) * int64(time.Second)
		segEndNano := (startUnix + int64(i) + 1) * int64(time.Second)
		nanosInSeg := int64(time.Second)
		if startUnixNano >= segStartNano {
			nanosInSeg = segEndNano - startUnixNano
		}
		if endUnixNano <= segEndNano {
			nanosInSeg = endUnixNano - segStartNano
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
	t.EndTime = time.Unix(l.segmentsStart+int64(len(l.segments)), 0)
	segs := l.segments
	// Remove first and last...
	const removeN = 2
	if len(segs) <= removeN*2 {
		return t
	}
	segs = segs[removeN : len(segs)-removeN]
	t.StartTime = time.Unix(l.segmentsStart, 0).Add(removeN * time.Second)
	t.EndTime = t.StartTime.Add(time.Duration(len(l.segments)) * time.Second)

	var ts ThroughputSegmented
	segments := make(bench.Segments, 0, len(segs))
	for i, seg := range segs {
		t.Errors += seg.errors
		t.Bytes += seg.bytes
		t.Objects += seg.objs
		t.Operations += seg.opsStarted
		var reqAvg float64
		objsPerOp := 1
		if reqAvg > 0 {
			reqAvg = float64(seg.reqDur) / float64(seg.opsStarted)
		}
		if seg.ops > 0 && seg.objs > 0 {
			objsPerOp = int(seg.objs / seg.ops)
		}
		segments = append(segments, bench.Segment{
			Start:      time.Unix(l.segmentsStart+int64(i), 0),
			EndsBefore: time.Unix(l.segmentsStart+int64(i+1), -1),
			OpType:     "",
			Host:       "",
			OpsStarted: seg.opsStarted,
			PartialOps: seg.partialOps,
			FullOps:    seg.fullOps,
			OpsEnded:   seg.opsEnded,
			Objects:    seg.objs,
			Errors:     seg.errors,
			ReqAvg:     reqAvg,
			TotalBytes: int64(seg.bytes),
			ObjsPerOp:  objsPerOp,
		})
	}

	if len(segs) > 0 {
		ts.fill(segments, int64(t.BytesPS()))
		ts.SegmentDurationMillis = 1000
		t.MeasureDurationMillis = len(segs) * 1000
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
	client    string

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
	if l.client == "" {
		l.client = o.ClientID
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
		tmp.fill(l.ops, false)
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
	l.ops = l.ops[:0]
}

// stringKeysSorted returns the keys as a sorted string slice.
func stringKeysSorted[K string, V any](m map[K]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	return keys
}

func (r Realtime) overLappingOps() bool {
	byOp := r.ByOpType
	if len(byOp) <= 1 {
		return false
	}
	for opA, dA := range byOp {
		for opB, dB := range byOp {
			if opA == opB {
				continue
			}
			if dA.StartTime.After(dB.EndTime) || dA.EndTime.Before(dB.StartTime) {
				return true
			}
		}
	}
	return false
}

func mergeRequests(data map[string]RequestSegments) (ss SingleSizedRequests, ms MultiSizedRequests) {
	for _, reqs := range data {
		for _, seg := range reqs {
			if seg.Single != nil {
				ss.add(*seg.Single)
			}
			if seg.Multi != nil {
				ms.add(*seg.Multi)
			}
		}
	}
	return ss, ms
}
