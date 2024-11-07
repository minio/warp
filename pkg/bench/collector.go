/*
 * Warp (C) 2019-2023 MinIO, Inc.
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
	"context"
	"math"
	"sync"
	"time"

	"github.com/minio/pkg/v2/console"
)

type Collector interface {
	// AutoTerm will check if throughput is within 'threshold' (0 -> ) for wantSamples,
	// when the current operations are split into 'splitInto' segments.
	// The minimum duration for the calculation can be set as well.
	// Segment splitting may cause less than this duration to be used.
	AutoTerm(ctx context.Context, op string, threshold float64, wantSamples, splitInto int, minDur time.Duration) context.Context

	// Receiver returns the receiver of input
	Receiver() chan<- Operation

	// AddOutput allows to add additional inputs.
	AddOutput(...chan<- Operation)

	// Close the collector
	Close()
}

type OpsCollector func() Operations

func EmptyOpsCollector() Operations {
	return Operations{}
}

type collector struct {
	rcv   chan Operation
	ops   Operations
	rcvWg sync.WaitGroup
	extra []chan<- Operation
	// The mutex protects the ops above.
	// Once ops have been added, they should no longer be modified.
	opsMu sync.Mutex
}

// NewOpsCollector returns a collector that will collect all operations in memory.
// After calling Close the returned function can be used to retrieve the operations.
func NewOpsCollector() (Collector, OpsCollector) {
	r := &collector{
		ops: make(Operations, 0, 10000),
		rcv: make(chan Operation, 1000),
	}
	r.rcvWg.Add(1)
	go func() {
		defer r.rcvWg.Done()
		for op := range r.rcv {
			for _, ch := range r.extra {
				ch <- op
			}
			r.opsMu.Lock()
			r.ops = append(r.ops, op)
			r.opsMu.Unlock()
		}
	}()
	return r, func() Operations {
		r.Close()
		return r.ops
	}
}

// NewNullCollector collects operations, but discards them.
func NewNullCollector() Collector {
	r := &collector{
		ops: make(Operations, 0),
		rcv: make(chan Operation, 1000),
	}
	r.rcvWg.Add(1)
	go func() {
		defer r.rcvWg.Done()
		for op := range r.rcv {
			for _, ch := range r.extra {
				ch <- op
			}
		}
	}()
	return r
}

// AutoTerm will check if throughput is within 'threshold' (0 -> ) for wantSamples,
// when the current operations are split into 'splitInto' segments.
// The minimum duration for the calculation can be set as well.
// Segment splitting may cause less than this duration to be used.
func (c *collector) AutoTerm(ctx context.Context, op string, threshold float64, wantSamples, splitInto int, minDur time.Duration) context.Context {
	if wantSamples >= splitInto {
		panic("wantSamples >= splitInto")
	}
	if splitInto == 0 {
		panic("splitInto == 0 ")
	}
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		ticker := time.NewTicker(time.Second)

	checkloop:
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
			}
			// Time to check if we should terminate.
			c.opsMu.Lock()
			// copies
			ops := c.ops.FilterByOp(op)
			c.opsMu.Unlock()
			start, end := ops.ActiveTimeRange(true)
			if end.Sub(start) <= minDur*time.Duration(splitInto)/time.Duration(wantSamples) {
				// We don't have enough.
				continue
			}
			segs := ops.Segment(SegmentOptions{
				From:           start,
				PerSegDuration: end.Sub(start) / time.Duration(splitInto),
				AllThreads:     true,
			})
			if len(segs) < wantSamples {
				continue
			}
			// Use last segment as our base.
			mb, _, objs := segs[len(segs)-1].SpeedPerSec()
			// Only use the segments we are interested in.
			segs = segs[len(segs)-wantSamples : len(segs)-1]
			for _, seg := range segs {
				segMB, _, segObjs := seg.SpeedPerSec()
				if mb > 0 {
					if math.Abs(mb-segMB) > threshold*mb {
						continue checkloop
					}
					continue
				}
				if math.Abs(objs-segObjs) > threshold*objs {
					continue checkloop
				}
			}
			// All checks passed.
			if mb > 0 {
				console.Eraseline()
				console.Printf("\rThroughput %0.01fMiB/s within %f%% for %v. Assuming stability. Terminating benchmark.\n",
					mb, threshold*100,
					segs[0].Duration().Round(time.Millisecond)*time.Duration(len(segs)+1))
			} else {
				console.Eraseline()
				console.Printf("\rThroughput %0.01f objects/s within %f%% for %v. Assuming stability. Terminating benchmark.\n",
					objs, threshold*100,
					segs[0].Duration().Round(time.Millisecond)*time.Duration(len(segs)+1))
			}
			return
		}
	}()
	return ctx
}

func (c *collector) Receiver() chan<- Operation {
	return c.rcv
}

func (c *collector) Close() {
	if c.rcv != nil {
		close(c.rcv)
		c.rcvWg.Wait()
		c.rcv = nil
		for _, ch := range c.extra {
			close(ch)
		}
		c.extra = nil
	}
	return
}

func (c *collector) AddOutput(x ...chan<- Operation) {
	c.extra = append(c.extra, x...)
}

/*
// aCollector is an aggregate Collector.
type aCollector struct {
	rcv   chan Operation
	wg    sync.WaitGroup
	extra []chan<- Operation
	res   *aggregate.RTAggregate
}

func NewAggregateCollector() Collector {
	a := make(chan Operation, 1000)
	r := &aCollector{
		rcv:   make(chan Operation, 1000),
		extra: []chan<- Operation{a},
	}
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		for op := range r.rcv {
			for _, ch := range r.extra {
				ch <- op
			}
		}
		for _, ch := range r.extra {
			close(ch)
		}
	}()

	go func() {
		defer r.wg.Done()
		r.res = aggregate.Live(a)
	}()
	return r
}

// Receiver returns the receiver.
func (c *aCollector) Receiver() chan<- Operation {
	return c.rcv
}

func (c *aCollector) Close() Operations {
	close(c.rcv)
	return nil
}

func (c *aCollector) AddOutput(x ...chan<- Operation) {
	c.extra = append(c.extra, x...)
}

func (c *aCollector) AutoTerm(ctx context.Context, op string, threshold float64, wantSamples, splitInto int, minDur time.Duration) context.Context {
	// TODO:
	return ctx
}
*/
