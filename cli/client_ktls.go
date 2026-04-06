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
	"net"
	stdHttp "net/http"
	"os"
	"time"

	"github.com/minio/cli"
	"gitlab.com/go-extension/http"
	"gitlab.com/go-extension/tls"
)

func clientTransportKTLS(ctx *cli.Context, localIP, resolvedHost, originalHost string) stdHttp.RoundTripper {
	var sni string
	if originalHost != "" {
		if h, _, err := net.SplitHostPort(originalHost); err == nil {
			sni = h
		} else {
			sni = originalHost
		}
	}
	// Keep TLS config.
	tlsConfig := &tls.Config{
		RootCAs: mustGetSystemCertPool(),
		// Can't use SSLv3 because of POODLE and BEAST
		// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
		// Can't use TLSv1.1 because of RC4 cipher usage
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: ctx.Bool("insecure"),
		ServerName:         sni,
		ClientSessionCache: tls.NewLRUClientSessionCache(1024), // up to 1024 nodes

		// Extra configs
		KernelTX: true,
		// Disable RX offload by default due to severe performance regressions and issues
		// https://github.com/golang/go/issues/44506#issuecomment-2387977030
		// https://github.com/golang/go/issues/44506#issuecomment-2765047544
		KernelRX: false,
		// We don't care about the size.
		CertificateCompressionDisabled: true,
	}

	if ctx.Bool("debug") {
		tlsConfig.KeyLogWriter = os.Stdout
	}

	netD := makeDialer(localIP)

	getDialAddr := func(addr string) string {
		if originalHost == "" || resolvedHost == "" {
			return addr
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			host = addr
			port = "443"
		}
		targetHost, _, err := net.SplitHostPort(resolvedHost)
		if err != nil {
			targetHost = resolvedHost
		}
		if host != targetHost {
			return net.JoinHostPort(targetHost, port)
		}
		return addr
	}

	// If we don't enable http/2, then using a custom DialTLSConext is the best choice.
	// It can improve performance by not using a compatibility layer.
	if !ctx.Bool("http2") {
		tlsDialer := &tls.Dialer{NetDialer: netD, Config: tlsConfig}
		h1Dialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialAddr := getDialAddr(addr)
			return tlsDialer.DialContext(ctx, network, dialAddr)
		}
		return newClientTransport(ctx, withDialTLSContext(h1Dialer))
	}

	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialAddr := getDialAddr(addr)
			return netD.DialContext(ctx, network, dialAddr)
		},
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
		ForceAttemptHTTP2: true,

		// Extra config
		TLSClientConfig: tlsConfig,
	}

	return &http.CompatableTransport{Transport: tr}
}
