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

package cli

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/cli"
)

var netDialer = &net.Dialer{
	Timeout:   10 * time.Second,
	KeepAlive: 10 * time.Second,
}

// Global counter for round-robin IP selection
var ipSelectionCounter uint64

// Cache for DNS results per hostname to ensure consistent round-robin.
// Reduces overhead of repeated lookups during high-concurrency benchmarks.
var dnsCache sync.Map // map[string][]string - hostname -> []IPs

// resolveAndRotate picks an IP from DNS records in a round-robin fashion.
// It returns the IP:Port for dialing and the original Hostname for SNI.
func resolveAndRotate(dialCtx context.Context, addr string) (newAddr, host string, err error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, "", err
	}

	var ipList []string
	if cached, ok := dnsCache.Load(host); ok {
		ipList = cached.([]string)
	} else {
		resolver := &net.Resolver{PreferGo: true}
		ips, err := resolver.LookupIPAddr(dialCtx, host)
		if err != nil {
			return addr, host, err
		}
		for _, ip := range ips {
			ipList = append(ipList, ip.String())
		}
		if len(ipList) > 0 {
			dnsCache.Store(host, ipList)
		}
	}

	if len(ipList) > 0 {
		idx := atomic.AddUint64(&ipSelectionCounter, 1) - 1
		selectedIP := ipList[idx%uint64(len(ipList))]
		return net.JoinHostPort(selectedIP, port), host, nil
	}
	return addr, host, nil
}

type transportOption func(transport *http.Transport)

func withTLSConfig(tlsConfig *tls.Config) transportOption {
	return func(transport *http.Transport) {
		transport.TLSClientConfig = tlsConfig
	}
}

func withDialTLSContext(dialer func(ctx context.Context, network, addr string) (net.Conn, error)) transportOption {
	return func(transport *http.Transport) {
		transport.DialTLSContext = dialer
	}
}

func newClientTransport(ctx *cli.Context, options ...transportOption) http.RoundTripper {
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConnsPerHost:   ctx.Int("concurrent"),
		WriteBufferSize:       ctx.Int("sndbuf"), // Configure beyond 4KiB default buffer size.
		ReadBufferSize:        ctx.Int("rcvbuf"), // Configure beyond 4KiB default buffer size.
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
		DisableKeepAlives:  ctx.Bool("disable-http-keepalive"),
		// Because we create a custom TLSClientConfig, we have to opt-in to HTTP/2.
		// See https://github.com/golang/go/issues/14275
		ForceAttemptHTTP2: ctx.Bool("http2"),
	}

	tr.DialContext = func(dialCtx context.Context, network, addr string) (net.Conn, error) {
		newAddr, host, _ := resolveAndRotate(dialCtx, addr)

		// Ensure SNI is set to the original host so TLS verification passes
		// when connecting via IP address.
		if tr.TLSClientConfig != nil && tr.TLSClientConfig.ServerName == "" {
			tr.TLSClientConfig.ServerName = host
		}

		return netDialer.DialContext(dialCtx, network, newAddr)
	}

	for _, option := range options {
		option(tr)
	}

	return tr
}
