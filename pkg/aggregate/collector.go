/*
 * Warp (C) 2019-2024 MinIO, Inc.
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
	"context"
	"math"
	"sync"
	"time"

	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
)

// LiveCollector return a collector, and a channel that will return the
// current aggregate on the channel whenever it is requested.
func LiveCollector(ctx context.Context, updates chan UpdateReq, clientID string) bench.Collector {
	c := collector{
		rcv: make(chan bench.Operation, 1000),
	}
	// Unbuffered, so we can send on unblock.
	if updates == nil {
		updates = make(chan UpdateReq, 1000)
	}
	c.updates = updates
	go func() {
		final := Live(c.rcv, updates, clientID)
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-updates:
				select {
				case req.C <- final:
				default:
				}
			}
		}
	}()
	return &c
}

// UpdateReq is a request for an update.
// The latest will be sent on the provided channel, or nil if none is available yet.
// If the provided channel blocks no update will be sent.
type UpdateReq struct {
	C     chan<- *Realtime `json:"-"`
	Reset bool             `json:"reset"` // Does not return result.
	Final bool             `json:"final"` // Blocks until final value is ready.
}

type collector struct {
	mu      sync.Mutex
	rcv     chan bench.Operation
	extra   []chan<- bench.Operation
	updates chan<- UpdateReq
	doneFn  []context.CancelFunc
}

func (c *collector) AutoTerm(ctx context.Context, op string, threshold float64, wantSamples, _ int, minDur time.Duration) context.Context {
	return AutoTerm(ctx, op, threshold, wantSamples, minDur, c.updates)
}

// AutoTerm allows to set auto-termination on a context.
func AutoTerm(ctx context.Context, op string, threshold float64, wantSamples int, minDur time.Duration, updates chan<- UpdateReq) context.Context {
	if updates == nil {
		return ctx
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
			respCh := make(chan *Realtime, 1)
			req := UpdateReq{C: respCh, Reset: false, Final: false}
			updates <- req
			resp := <-respCh
			if resp == nil {
				continue
			}

			ops := resp.ByOpType[op]
			if op == "" {
				ops = &resp.Total
			}
			if ops == nil || ops.Throughput.Segmented == nil {
				continue
			}
			start, end := ops.StartTime, ops.EndTime
			if end.Sub(start) <= minDur {
				// We don't have enough.
				continue
			}
			if len(ops.Throughput.Segmented.Segments) < wantSamples {
				continue
			}
			segs := ops.Throughput.Segmented.Segments
			// Use last segment as our base.
			lastSeg := segs[len(segs)-1]
			mb, objs := lastSeg.BPS, lastSeg.OPS
			// Only use the segments we are interested in.
			segs = segs[len(segs)-wantSamples : len(segs)-1]
			for _, seg := range segs {
				segMB, segObjs := seg.BPS, seg.OPS
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
					time.Duration(ops.Throughput.Segmented.SegmentDurationMillis*(len(segs)+1))*time.Millisecond)
			} else {
				console.Eraseline()
				console.Printf("\rThroughput %0.01f objects/s within %f%% for %v. Assuming stability. Terminating benchmark.\n",
					objs, threshold*100,
					time.Duration(ops.Throughput.Segmented.SegmentDurationMillis*(len(segs)+1))*time.Millisecond)
			}
			return
		}
	}()
	return ctx
}

func (c *collector) Receiver() chan<- bench.Operation {
	return c.rcv
}

func (c *collector) AddOutput(operations ...chan<- bench.Operation) {
	c.extra = append(c.extra, operations...)
}

func (c *collector) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rcv != nil {
		close(c.rcv)
		c.rcv = nil
	}
	for _, cancel := range c.doneFn {
		cancel()
	}
	c.doneFn = nil
}
