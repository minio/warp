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
	"time"

	"github.com/minio/cli"
)

var netDialer = &net.Dialer{
	Timeout:   10 * time.Second,
	KeepAlive: 10 * time.Second,
}

// makeDialer returns a Dialer optionally bound to localIP.
func makeDialer(localIP string) *net.Dialer {
	d := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 10 * time.Second,
	}
	if localIP != "" {
		d.LocalAddr = &net.TCPAddr{IP: net.ParseIP(localIP)}
	}
	return d
}

type transportOption func(transport *http.Transport)

// withLocalAddr returns a transportOption that binds outbound TCP connections
// to localIP, ensuring they egress via the NIC that owns that address.
func withLocalAddr(localIP string) transportOption {
	return func(transport *http.Transport) {
		if localIP == "" {
			return
		}
		transport.DialContext = makeDialer(localIP).DialContext
	}
}

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

func newClientTransport(ctx *cli.Context, endpoint string, options ...transportOption) http.RoundTripper {
	isTLS := ctx.Bool("tls") || ctx.Bool("ktls")
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
	// Only set DialContext manually when IP pinning is enabled (endpoint != "")
	// This ensures proxies work out-of-the-box for standard runs.
	if endpoint != "" {
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				host = addr
				port = "80"
				if isTLS {
					port = "443"
				}
			}
			// If addr is actually the proxy (provided by ProxyFromEnvironment),
			// host will NOT match our target. We should not pin the proxy's IP.
			// We only pin if we aren't using a proxy for this specific request.
			dialAddr := addr
			targetHost, _, err := net.SplitHostPort(endpoint)
			if err != nil {
				targetHost = endpoint
			}
			// Only pin if the target host (IP/Domain) actually differs
			if targetHost != host {
				dialAddr = net.JoinHostPort(targetHost, port)
			}
			return netDialer.DialContext(ctx, network, dialAddr)
		}
	}
	for _, option := range options {
		option(tr)
	}

	return tr
}
