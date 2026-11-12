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
	// Keep TLS config.
	// ServerName is left empty here; see the dialers below for how SNI is set
	// when --resolve-host dials a resolved IP.
	tlsConfig := &tls.Config{
		RootCAs: mustGetSystemCertPool(),
		// Can't use SSLv3 because of POODLE and BEAST
		// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
		// Can't use TLSv1.1 because of RC4 cipher usage
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: ctx.Bool("insecure"),
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

	// pinner is nil (never rewrites) unless --resolve-host is active.
	// Only addresses for the original hostname (or bucket.<hostname>) are
	// rewritten; proxy addresses are left untouched.
	pinner := newResolvePinner(resolvedHost, originalHost)

	// If we don't enable http/2, then using a custom DialTLSConext is the best choice.
	// It can improve performance by not using a compatibility layer.
	if !ctx.Bool("http2") {
		tlsDialer := &tls.Dialer{NetDialer: netD, Config: tlsConfig}
		h1Dialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialAddr, pinned := pinner.dialAddr(addr, "443")
			if !pinned {
				// The TLS dialer derives ServerName from addr.
				return tlsDialer.DialContext(ctx, network, addr)
			}
			// We dial an IP, so the TLS dialer can no longer derive ServerName
			// from the address. Set it from the requested hostname instead, so
			// SNI and certificate checks use it (bucket.<hostname> included).
			cfg := tlsConfig.Clone()
			cfg.ServerName = hostOnly(addr)
			pinnedDialer := &tls.Dialer{NetDialer: netD, Config: cfg}
			return pinnedDialer.DialContext(ctx, network, dialAddr)
		}
		return newClientTransport(ctx, withDialTLSContext(h1Dialer))
	}

	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		// With http2 the transport does the TLS handshake itself and takes
		// ServerName from the request URL host, so only the dial address
		// needs rewriting.
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialAddr, _ := pinner.dialAddr(addr, "443")
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
