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
	"sync"
	"time"

	"github.com/minio/warp/pkg/bench"
)

// LiveCollector return a collector, and a channel that will return the
// current aggregate on the channel whenever it is requested.
func LiveCollector(ctx context.Context, updates chan UpdateReq) bench.Collector {
	c := collector{
		rcv: make(chan bench.Operation, 1000),
	}
	// Unbuffered, so we can send on unblock.
	if updates == nil {
		updates = make(chan UpdateReq, 1000)
	}
	c.updates = updates
	go func() {
		final := Live(c.rcv, updates)
		for {
			select {
			case <-ctx.Done():
				close(c.rcv)
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
	Reset bool             `json:"reset"`
}

type collector struct {
	mu      sync.Mutex
	rcv     chan bench.Operation
	extra   []chan<- bench.Operation
	updates chan<- UpdateReq
	doneFn  []context.CancelFunc
}

func (c *collector) AutoTerm(ctx context.Context, op string, threshold float64, wantSamples, splitInto int, minDur time.Duration) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.doneFn = append(c.doneFn, cancel)
	c.mu.Unlock()
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
	if c.rcv == nil {
		close(c.rcv)
	}
	for _, cancel := range c.doneFn {
		cancel()
	}
	c.doneFn = nil
}
