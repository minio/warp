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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/pkg/v3/console"
)

// dnsCache is a process-wide, TTL-bounded DNS cache shared by all transports.
//
// Go's pure resolver (forced when the binary is built with CGO_ENABLED=0, as
// warp's release binaries are) performs a fresh lookup on every dial and does
// no in-process caching. Under high --concurrent that produces a burst of DNS
// queries ("DNS storm") for a single FQDN host. This cache resolves a hostname
// at most once per TTL regardless of concurrency, while leaving the FQDN itself
// untouched so the client keeps using it for TLS SNI, certificate validation
// and the Host header.
type dnsCache struct {
	ttl time.Duration

	mu      sync.Mutex
	entries map[string]*dnsEntry
}

type dnsEntry struct {
	ips     []string  // resolved IPs (host part only, no port)
	expires time.Time // when the entry must be re-resolved
	next    uint64    // round-robin cursor across ips
	// resolving guards a single in-flight re-resolution so that only one
	// dial re-resolves on expiry while the others keep using the stale set.
	resolving bool
	logged    bool // whether the initial resolution was logged
}

// newDNSCache returns a cache with the given TTL. A ttl <= 0 disables caching
// (the returned cache passes every dial straight through to the OS resolver).
func newDNSCache(ttl time.Duration) *dnsCache {
	return &dnsCache{
		ttl:     ttl,
		entries: make(map[string]*dnsEntry),
	}
}

// enabled reports whether caching is active.
func (c *dnsCache) enabled() bool {
	return c != nil && c.ttl > 0
}

// dialContext returns a DialContext that resolves the host part of addr through
// the cache (when enabled and the host is not a literal IP) and dials the
// selected cached IP. base performs the actual TCP dial and must accept a
// "host:port" address.
func (c *dnsCache) dialContext(base func(ctx context.Context, network, addr string) (net.Conn, error)) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if !c.enabled() {
			return base(ctx, network, addr)
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			// No port; treat the whole thing as the host.
			host, port = addr, ""
		}
		// Literal IPs need no resolution.
		if host == "" || net.ParseIP(host) != nil {
			return base(ctx, network, addr)
		}

		ip, err := c.lookup(ctx, host)
		if err != nil {
			return nil, err
		}
		dialAddr := ip
		if port != "" {
			dialAddr = net.JoinHostPort(ip, port)
		}
		return base(ctx, network, dialAddr)
	}
}

// lookup returns one cached IP for host, resolving (once per TTL) as needed.
func (c *dnsCache) lookup(ctx context.Context, host string) (string, error) {
	c.mu.Lock()
	e := c.entries[host]
	now := time.Now()

	// Fresh entry: hand out the next IP round-robin.
	if e != nil && len(e.ips) > 0 && now.Before(e.expires) {
		ip := e.pick()
		c.mu.Unlock()
		return ip, nil
	}

	// Expired (or new) but another dial is already re-resolving: serve the
	// stale set if we have one, otherwise fall through to resolve inline.
	if e != nil && len(e.ips) > 0 && e.resolving {
		ip := e.pick()
		c.mu.Unlock()
		return ip, nil
	}

	// We are the resolver for this host.
	if e == nil {
		e = &dnsEntry{}
		c.entries[host] = e
	}
	e.resolving = true
	stale := append([]string(nil), e.ips...)
	logged := e.logged
	c.mu.Unlock()

	ips, err := resolveHostIPs(ctx, host)

	c.mu.Lock()
	e.resolving = false
	if err != nil || len(ips) == 0 {
		// Keep serving the last successful set; never stop the benchmark
		// on a transient DNS failure.
		if len(stale) > 0 {
			ip := e.pick()
			c.mu.Unlock()
			return ip, nil
		}
		c.mu.Unlock()
		if err == nil {
			err = &net.DNSError{Err: "no addresses found", Name: host}
		}
		return "", err
	}
	e.ips = ips
	e.expires = time.Now().Add(c.ttl)
	if !logged {
		e.logged = true
		console.Infoln("DNS cache: resolved", host, "->", strings.Join(ips, ", "), "(ttl", c.ttl.String()+")")
	}
	ip := e.pick()
	c.mu.Unlock()
	return ip, nil
}

// pick returns the next IP round-robin. Caller must hold c.mu.
func (e *dnsEntry) pick() string {
	if len(e.ips) == 1 {
		return e.ips[0]
	}
	n := atomic.AddUint64(&e.next, 1)
	return e.ips[int(n-1)%len(e.ips)]
}

// resolveHostIPs resolves host to its IP addresses (host part only).
func resolveHostIPs(ctx context.Context, host string) ([]string, error) {
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	ips := make([]string, 0, len(addrs))
	for _, a := range addrs {
		ips = append(ips, a.IP.String())
	}
	return ips, nil
}
