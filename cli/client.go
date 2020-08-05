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

package cli

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	md5simd "github.com/minio/md5-simd"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/minio/pkg/ellipses"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/warp/pkg"
)

type hostSelectType string

const (
	hostSelectTypeRoundrobin hostSelectType = "roundrobin"
	hostSelectTypeWeighed    hostSelectType = "weighed"
)

func getClientOpts(ctx *cli.Context, endpoint warpEndpoint) *minio.Options {
	creds := credentials.NewStaticV4(ctx.String("access-key"), ctx.String("secret-key"), ctx.String("session-token"))

	bucketLookupType := minio.BucketLookupAuto
	switch ctx.String("path") {
	case "off":
		bucketLookupType = minio.BucketLookupDNS
	case "on":
		bucketLookupType = minio.BucketLookupPath
	}

	return &minio.Options{
		Creds:        creds,
		Secure:       endpoint.Scheme == "https",
		Region:       ctx.String("region"),
		BucketLookup: bucketLookupType,
		CustomMD5:    md5simd.NewServer().NewHash,
		Transport:    clientTransport(ctx),
	}
}

func newClient(ctx *cli.Context) func() (cl *minio.Client, done func()) {
	endpoints := parseEndpoints(ctx.Args().First())

	switch len(endpoints) {
	case 0:
		fatalIf(probe.NewError(errors.New("no endpoint defined")), "Unable to create MinIO client")
	case 1:
		clnts, err := getClient(ctx, endpoints...)
		fatalIf(probe.NewError(err), "Unable to create MinIO client")

		return func() (*minio.Client, func()) {
			return clnts[0], func() {}
		}
	}

	clients, err := getClient(ctx, endpoints...)
	fatalIf(probe.NewError(err), "Unable to create MinIO client")

	hostSelect := hostSelectType(ctx.String("host-select"))
	switch hostSelect {
	case hostSelectTypeRoundrobin:
		// Do round-robin.
		var current int
		var mu sync.Mutex
		return func() (*minio.Client, func()) {
			mu.Lock()
			now := current % len(clients)
			current++
			mu.Unlock()
			return clients[now], func() {}
		}
	case hostSelectTypeWeighed:
		// Keep track of handed out clients.
		// Select random between the clients that have the fewest handed out.
		var mu sync.Mutex
		running := make([]int, len(endpoints))
		lastFinished := make([]time.Time, len(endpoints))
		{
			// Start with a random host
			now := time.Now()
			off := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(endpoints))
			for i := range lastFinished {
				t := now
				t.Add(time.Duration(i + off%len(endpoints)))
				lastFinished[i] = t
			}
		}
		find := func() int {
			min := math.MaxInt32
			for _, n := range running {
				if n < min {
					min = n
				}
			}
			earliest := time.Now().Add(time.Second)
			earliestIdx := 0
			for i, n := range running {
				if n == min {
					if lastFinished[i].Before(earliest) {
						earliest = lastFinished[i]
						earliestIdx = i
					}
				}
			}
			return earliestIdx
		}
		return func() (*minio.Client, func()) {
			mu.Lock()
			idx := find()
			running[idx]++
			mu.Unlock()
			return clients[idx], func() {
				mu.Lock()
				lastFinished[idx] = time.Now()
				running[idx]--
				if running[idx] < 0 {
					// Will happen if done is called twice.
					panic("client running index < 0")
				}
				mu.Unlock()
			}
		}
	}
	console.Fatalln("unknown host-select:", hostSelect)
	return nil
}

// getClient creates a client with the specified host and the options set in the context.
func getClient(ctx *cli.Context, endpoints ...warpEndpoint) (clnts []*minio.Client, err error) {
	for _, endpoint := range endpoints {
		var cl *minio.Client
		cl, err = minio.New(endpoint.Host, getClientOpts(ctx, endpoint))
		if err != nil {
			return nil, err
		}
		cl.SetAppInfo(appName, pkg.Version)
		clnts = append(clnts, cl)
	}
	return clnts, nil
}

func clientTransport(ctx *cli.Context) http.RoundTripper {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 10 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   ctx.Int("concurrent"),
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 2 * time.Minute,
		// Set this value so that the underlying transport round-tripper
		// doesn't try to auto decode the body of objects with
		// content-encoding set to `gzip`.
		//
		// Refer:
		//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
		DisableCompression: true,
	}
	if ctx.Bool("tls") {
		// Keep TLS config.
		tlsConfig := &tls.Config{
			RootCAs: mustGetSystemCertPool(),
			// Can't use SSLv3 because of POODLE and BEAST
			// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
			// Can't use TLSv1.1 because of RC4 cipher usage
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true,
		}
		tr.TLSClientConfig = tlsConfig

		// Because we create a custom TLSClientConfig, we have to opt-in to HTTP/2.
		// See https://github.com/golang/go/issues/14275
		//
		// TODO: Enable http2.0 when upstream issues related to HTTP/2 are fixed.
		//
		// if e = http2.ConfigureTransport(tr); e != nil {
		// 	return nil, probe.NewError(e)
		// }
	}
	return tr
}

type warpEndpoint struct {
	*url.URL
	Bucket string
	Prefix string
}

const slashSeparator = "/"

// path2BucketObjectWithBasePath returns bucket and prefix, if any,
// of a 'path'. basePath is trimmed from the front of the 'path'.
func path2BucketObjectWithBasePath(basePath, path string) (bucket, prefix string) {
	path = strings.TrimPrefix(path, basePath)
	path = strings.TrimPrefix(path, slashSeparator)
	m := strings.Index(path, slashSeparator)
	if m < 0 {
		return path, ""
	}
	return path[:m], path[m+len(slashSeparator):]
}

func path2BucketObject(s string) (bucket, prefix string) {
	return path2BucketObjectWithBasePath("", s)
}

// parseEndpoints will parse the endpoints parameter given.
func parseEndpoints(eps string) []warpEndpoint {
	endpoints := strings.Split(eps, ",")
	dst := make([]warpEndpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if !ellipses.HasEllipses(endpoint) {
			u, err := url.Parse(endpoint)
			if err != nil {
				fatalIf(probe.NewError(err).Trace(endpoint), "Unable to parse endpoint")
			}
			bucket, prefix := path2BucketObject(u.Path)
			dst = append(dst, warpEndpoint{URL: u, Bucket: bucket, Prefix: prefix})
			continue
		}
		patterns, perr := ellipses.FindEllipsesPatterns(endpoint)
		if perr != nil {
			fatalIf(probe.NewError(perr), "Unable to parse ellipses in endpoint parameter")
		}
		for _, p := range patterns {
			for _, ep := range p.Expand() {
				u, err := url.Parse(ep)
				if err != nil {
					fatalIf(probe.NewError(err).Trace(ep), "Unable to parse endpoint after ellipses expansion")
				}
				bucket, prefix := path2BucketObject(u.Path)
				dst = append(dst, warpEndpoint{URL: u, Bucket: bucket, Prefix: prefix})
			}
		}
	}
	return dst
}

// mustGetSystemCertPool - return system CAs or empty pool in case of error (or windows)
func mustGetSystemCertPool() *x509.CertPool {
	pool, err := x509.SystemCertPool()
	if err != nil {
		return x509.NewCertPool()
	}
	return pool
}

func newAdminClient(ctx *cli.Context) *madmin.AdminClient {
	endpoints := parseEndpoints(ctx.Args().First())
	if len(endpoints) == 0 {
		fatalIf(probe.NewError(errors.New("no endpoint defined")), "Unable to create MinIO admin client")
	}
	mopts := getClientOpts(ctx, endpoints[0])
	admin, err := madmin.NewWithOptions(endpoints[0].Host, &madmin.Options{
		Creds:  mopts.Creds,
		Secure: mopts.Secure,
	})
	fatalIf(probe.NewError(err), "Unable to create MinIO admin client")
	admin.SetCustomTransport(clientTransport(ctx))
	admin.SetAppInfo(appName, pkg.Version)
	return admin
}
