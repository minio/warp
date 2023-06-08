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
	var tags url.Values
	if len(u.RawQuery) > 0 {
		tags, err = url.ParseQuery(u.RawQuery)
		errorIf(probe.NewError(err), "unable to parse tags")
	}

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
	writeAPI.SetWriteFailedCallback(func(batch string, error http.Error, retryAttempts uint) bool {
		errorIf(probe.NewError(err), "unable to write to influxdb")
		return false
	})
	ch := make(chan bench.Operation, 10000)
	wg.Add(1)
	go func() {
		n := 0
		defer func() {
			writeAPI.Flush()
			wg.Done()
		}()
		for op := range ch {
			p := influxdb2.NewPointWithMeasurement("request")
			p.SetTime(op.Start)
			for key, tag := range tags {
				if len(tag) > 0 && len(key) > 0 {
					p.AddTag(key, tag[0])
				}
			}
			dur := op.End.Sub(op.Start)
			p.AddTag("op", op.OpType)
			p.AddTag("client_id", op.ClientID)
			p.AddTag("endpoint", op.Endpoint)
			p.AddTag("file", op.File)
			p.AddField("thread", op.Thread)
			p.AddField("n_objects", op.ObjPerOp)
			p.AddField("bytes_sec", op.Size*int64(time.Second)/int64(dur))
			p.AddField("start", op.Start)
			p.AddField("end", op.End)
			p.AddField("dur_nanos", int64(dur/time.Nanosecond))
			if op.Err != "" {
				p.AddField("error", op.Err)
			}
			writeAPI.WritePoint(p)
			n++
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
