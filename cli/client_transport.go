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
	"strings"
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

// hostOnly returns the host part of a "host:port" string, without brackets for
// IPv6. Input without a port (including a bare IPv6 address) is returned as is.
func hostOnly(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return h
	}
	return hostport
}

// shouldPin reports whether a connection to host must be pinned to the
// resolved IP of origHostname. It matches the original hostname itself and
// any subdomain of it, so virtual-host style requests (bucket.<host>, used
// with --lookup=host) are pinned too. Hostnames are case-insensitive.
// Anything else, such as a proxy address, is not pinned.
func shouldPin(host, origHostname string) bool {
	if host == "" || origHostname == "" {
		return false
	}
	if strings.EqualFold(host, origHostname) {
		return true
	}
	n := len(host) - len(origHostname)
	return n > 1 && host[n-1] == '.' && strings.EqualFold(host[n:], origHostname)
}

// resolvePinner rewrites dial addresses that target the original hostname to
// the IP it was resolved to at startup (--resolve-host).
// A nil *resolvePinner never rewrites.
type resolvePinner struct {
	targetIP     string // resolved IP to dial, without port
	origHostname string // original hostname, without port
}

// newResolvePinner returns a pinner, or nil when --resolve-host is not active
// for this host (originalHost is empty).
func newResolvePinner(resolvedHost, originalHost string) *resolvePinner {
	if resolvedHost == "" || originalHost == "" {
		return nil
	}
	return &resolvePinner{
		targetIP:     hostOnly(resolvedHost),
		origHostname: hostOnly(originalHost),
	}
}

// dialAddr returns the address to dial for addr, and whether it was rewritten
// to the pinned IP. defaultPort is used when addr has no port.
func (p *resolvePinner) dialAddr(addr, defaultPort string) (string, bool) {
	if p == nil {
		return addr, false
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		host, port = addr, defaultPort
	}
	if !shouldPin(host, p.origHostname) {
		return addr, false
	}
	return net.JoinHostPort(p.targetIP, port), true
}

// withResolveHost rewrites the dial address from the logical hostname to the
// resolved IP when --resolve-host is active. Only activates when originalHost != "".
// Only connections to the original hostname (or a subdomain of it) are rewritten;
// proxy addresses are left unchanged, so proxied connections are not pinned.
//
// TLS needs no extra handling here: http.Transport takes the TLS ServerName from
// the request URL host, which is still the hostname, not from the dialed address.
func withResolveHost(resolvedHost, originalHost string, dialer *net.Dialer, isTLS bool) transportOption {
	return func(transport *http.Transport) {
		pinner := newResolvePinner(resolvedHost, originalHost)
		if pinner == nil {
			return
		}
		defaultPort := "80"
		if isTLS {
			defaultPort = "443"
		}
		transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialAddr, _ := pinner.dialAddr(addr, defaultPort)
			return dialer.DialContext(ctx, network, dialAddr)
		}
	}
}

func newClientTransport(ctx *cli.Context, options ...transportOption) http.RoundTripper {
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           netDialer.DialContext,
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

	for _, option := range options {
		option(tr)
	}

	return tr
}
