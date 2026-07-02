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
	"sync"
	"testing"
	"time"
)

func TestDNSCacheDisabled(t *testing.T) {
	c := newDNSCache(0)
	if c.enabled() {
		t.Fatal("cache with ttl 0 must be disabled")
	}
	var nilCache *dnsCache
	if nilCache.enabled() {
		t.Fatal("nil cache must be disabled")
	}

	// A disabled cache must dial through untouched.
	var gotAddr string
	base := func(_ context.Context, _, addr string) (net.Conn, error) {
		gotAddr = addr
		return nil, nil
	}
	_, _ = c.dialContext(base)(context.Background(), "tcp", "example.com:9000")
	if gotAddr != "example.com:9000" {
		t.Fatalf("disabled cache altered dial address: got %q", gotAddr)
	}
}

func TestDNSCacheLiteralIPPassthrough(t *testing.T) {
	c := newDNSCache(time.Minute)
	var gotAddr string
	base := func(_ context.Context, _, addr string) (net.Conn, error) {
		gotAddr = addr
		return nil, nil
	}
	// Literal IPs must not be resolved/cached.
	_, _ = c.dialContext(base)(context.Background(), "tcp", "10.0.0.1:9000")
	if gotAddr != "10.0.0.1:9000" {
		t.Fatalf("literal IP was altered: got %q", gotAddr)
	}
	if len(c.entries) != 0 {
		t.Fatalf("literal IP should not create a cache entry, got %d", len(c.entries))
	}
}

func TestDNSCacheResolvesOncePerTTL(t *testing.T) {
	c := newDNSCache(time.Hour)
	// Prime with a fresh entry directly to avoid depending on real DNS.
	c.mu.Lock()
	c.entries["host.example"] = &dnsEntry{
		ips:     []string{"192.0.2.1", "192.0.2.2"},
		expires: time.Now().Add(time.Hour),
	}
	c.mu.Unlock()

	// Many lookups within TTL must not re-resolve and must round-robin.
	seen := map[string]int{}
	for i := 0; i < 100; i++ {
		ip, err := c.lookup(context.Background(), "host.example")
		if err != nil {
			t.Fatal(err)
		}
		seen[ip]++
	}
	if seen["192.0.2.1"] == 0 || seen["192.0.2.2"] == 0 {
		t.Fatalf("expected round-robin across both IPs, got %v", seen)
	}
}

func TestDNSCacheKeepsStaleOnFailure(t *testing.T) {
	c := newDNSCache(time.Millisecond)
	// An already-expired entry with a stale set for a name that will fail to
	// resolve (reserved .invalid TLD). The stale value must still be served
	// rather than propagating the DNS error.
	c.mu.Lock()
	c.entries["nonexistent.invalid"] = &dnsEntry{
		ips:     []string{"203.0.113.5"},
		expires: time.Now().Add(-time.Hour),
	}
	c.mu.Unlock()

	ip, err := c.lookup(context.Background(), "nonexistent.invalid")
	if err != nil {
		t.Fatalf("expected stale value to be served, got error: %v", err)
	}
	if ip != "203.0.113.5" {
		t.Fatalf("expected stale IP 203.0.113.5, got %q", ip)
	}
}

func TestDNSCacheConcurrentSingleFlight(t *testing.T) {
	c := newDNSCache(time.Hour)
	c.mu.Lock()
	c.entries["cc.example"] = &dnsEntry{
		ips:     []string{"198.51.100.7"},
		expires: time.Now().Add(time.Hour),
	}
	c.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.lookup(context.Background(), "cc.example"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}
