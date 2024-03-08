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

package cli

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/http"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/warp/pkg/bench"
)

func newInfluxDB(ctx *cli.Context, wg *sync.WaitGroup) chan<- bench.Operation {
	u, err := parseInfluxURL(ctx)
	if err != nil {
		fatalIf(probe.NewError(err), "unable to parse influxdb parameter")
	}
	token := ""
	if u.User != nil {
		token = u.User.Username()
	}
	var tagValues url.Values
	if len(u.RawQuery) > 0 {
		tagValues, err = url.ParseQuery(u.RawQuery)
		errorIf(probe.NewError(err), "unable to parse tags")
	}
	tags := make(map[string]string, len(tagValues)+1)
	for key, tag := range tagValues {
		if len(tag) > 0 && len(key) > 0 {
			tags[key] = tag[0]
		}
	}
	tags["warp_id"] = pRandASCII(8)

	// Create a new client using an InfluxDB server base URL and an authentication token
	serverURL := u.Scheme + "://" + u.Host
	client := influxdb2.NewClientWithOptions(serverURL, token, influxdb2.DefaultOptions().SetMaxRetryTime(1000).SetMaxRetries(2))
	{
		to, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		ok, err := client.Ping(to)
		if !ok {
			errorIf(probe.NewError(err), "unable to reach influxdb")
		}
	}
	// Use blocking write client for writes to desired bucket
	path := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	writeAPI := client.WriteAPI(path[1], path[0])
	writeAPI.SetWriteFailedCallback(func(_ string, err http.Error, _ uint) bool {
		errorIf(probe.NewError(&err), "unable to write to influxdb")
		return false
	})
	ch := make(chan bench.Operation, 10000)
	wg.Add(1)
	go func() {
		defer func() {
			writeAPI.Flush()
			wg.Done()
		}()
		hosts := make(map[string]map[string]aggregatedStats, 100)
		totalOp := make(map[string]aggregatedStats, 5)
		for op := range ch {
			host := hosts[op.Endpoint]
			var hostStats aggregatedStats
			if host == nil {
				host = make(map[string]aggregatedStats, 5)
				hosts[op.Endpoint] = host
			} else {
				hostStats = host[op.OpType]
			}
			total := totalOp[op.OpType]
			hostStats.add(op)
			total.add(op)

			// Store
			totalOp[op.OpType] = total
			host[op.OpType] = hostStats

			// Send
			pTot := total.point(op)
			pHost := hostStats.point(op)

			for key, tag := range tags {
				pTot.AddTag(key, tag)
				pHost.AddTag(key, tag)
			}
			pHost.AddTag("endpoint", op.Endpoint)
			writeAPI.WritePoint(pHost)
			writeAPI.WritePoint(pTot)
		}
		// Send summaries
		for host, ops := range hosts {
			for op, stats := range ops {
				p := stats.summary(op)

				for key, tag := range tags {
					p.AddTag(key, tag)
				}
				p.AddTag("endpoint", host)
				writeAPI.WritePoint(p)
			}
		}
		for op, stats := range totalOp {
			p := stats.summary(op)

			for key, tag := range tags {
				p.AddTag(key, tag)
			}
			p.AddTag("endpoint", "")
			writeAPI.WritePoint(p)
		}
	}()
	return ch
}

func parseInfluxURL(ctx *cli.Context) (*url.URL, error) {
	s := ctx.String("influxdb")
	if s == "" {
		return nil, nil
	}
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "":
		return nil, errors.New("influxdb: no scheme specified (http/https)")
	case "http", "https":
	default:
		return nil, fmt.Errorf("influxdb: unknown scheme %s - must be http/https", u.Scheme)
	}
	path := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(path) != 2 {
		return nil, fmt.Errorf("influxdb: unexpected path. Want 'bucket/org', got '%s'", strings.TrimPrefix(u.Path, "/"))
	}
	if len(path[0]) == 0 {
		return nil, errors.New("influxdb: empty bucket specified")
	}
	if len(u.RawQuery) > 0 {
		_, err = url.ParseQuery(u.RawQuery)
		if err != nil {
			return nil, err
		}
	}

	// org can be empty
	// token can be empty
	return u, nil
}

type aggregatedStats struct {
	bytes   int64
	objects int
	ops     int
	errors  int

	// requests
	reqDur time.Duration
	reqMin time.Duration
	reqMax time.Duration

	// time to first byte
	ttfb    time.Duration
	ttfbMin time.Duration
	ttfbMax time.Duration
}

func (a *aggregatedStats) add(o bench.Operation) {
	a.ops++
	if o.Err != "" {
		// Do not add more
		a.errors++
		return
	}

	a.bytes += o.Size
	a.objects += o.ObjPerOp

	dur := o.End.Sub(o.Start)
	a.reqDur += dur
	if dur > a.reqMax {
		a.reqMax = dur
	}
	if a.reqMin == 0 || dur < a.reqMin {
		a.reqMin = dur
	}
	if o.FirstByte != nil {
		ttfb := o.FirstByte.Sub(o.Start)
		a.ttfb += ttfb
		if ttfb > a.ttfbMax {
			a.ttfbMax = ttfb
		}
		if a.ttfbMin == 0 || dur < a.ttfbMin {
			a.ttfbMin = dur
		}
	}
}

func (a aggregatedStats) point(op bench.Operation) *write.Point {
	p := influxdb2.NewPointWithMeasurement("warp")
	p.AddTag("op", op.OpType)
	p.AddField("requests", a.ops)
	p.AddField("objects", a.objects)
	p.AddField("bytes_total", a.bytes)
	p.AddField("errors", a.errors)
	p.AddField("request_total_secs", float64(a.reqDur)/float64(time.Second))
	if a.ttfb > 0 {
		p.AddField("request_ttfb_total_secs", float64(a.ttfb)/float64(time.Second))
	}
	return p
}

func (a aggregatedStats) summary(opType string) *write.Point {
	p := influxdb2.NewPointWithMeasurement("warp_run_summary")
	p.AddTag("op", opType)
	p.AddField("requests", a.ops)
	p.AddField("objects", a.objects)
	p.AddField("bytes_total", a.bytes)
	p.AddField("errors", a.errors)
	p.AddField("request_total_secs", float64(a.reqDur)/float64(time.Second))
	if a.ops-a.errors > 0 {
		p.AddField("request_avg_secs", float64(a.reqDur)/float64(time.Second)/float64(a.ops-a.errors))
	}
	p.AddField("request_min_secs", float64(a.reqMin)/float64(time.Second))
	p.AddField("request_max_secs", float64(a.reqMax)/float64(time.Second))
	if a.ttfb > 0 {
		p.AddField("request_ttfb_total_secs", float64(a.ttfb)/float64(time.Second))
		if a.ops-a.errors > 0 {
			p.AddField("request_ttfb_avg_secs", float64(a.ttfb)/float64(time.Second)/float64(a.ops-a.errors))
		}
		p.AddField("request_ttfb_min_secs", float64(a.ttfbMin)/float64(time.Second))
		p.AddField("request_ttfb_max_secs", float64(a.ttfbMax)/float64(time.Second))
	}
	return p
}
