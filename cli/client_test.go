/*
 * Warp (C) 2026 MinIO, Inc.
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
	"flag"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/minio/cli"
	"github.com/minio/warp/pkg/iceberg"
)

// The tests below only use IP literals and *.invalid hostnames, so they need
// no DNS: net.LookupIP returns IP literals as is, and *.invalid names are
// rewritten to a local IP before anything is dialed.

func TestParseHostPairs(t *testing.T) {
	tests := []struct {
		name    string
		hosts   string
		resolve bool
		want    []hostPair
	}{
		{
			name:  "no resolve keeps hosts and leaves originalHost empty",
			hosts: "s3-a.example.invalid:9000,s3-b.example.invalid:9000",
			want: []hostPair{
				{resolved: "s3-a.example.invalid:9000"},
				{resolved: "s3-b.example.invalid:9000"},
			},
		},
		{
			name:    "ipv4 with port",
			hosts:   "127.0.0.1:9000",
			resolve: true,
			want:    []hostPair{{resolved: "127.0.0.1:9000", originalHost: "127.0.0.1:9000"}},
		},
		{
			name:    "ipv4 without port",
			hosts:   "127.0.0.1",
			resolve: true,
			want:    []hostPair{{resolved: "127.0.0.1", originalHost: "127.0.0.1"}},
		},
		{
			name:    "ipv6 with port is bracketed",
			hosts:   "[::1]:9000",
			resolve: true,
			want:    []hostPair{{resolved: "[::1]:9000", originalHost: "[::1]:9000"}},
		},
		{
			name:    "ipv6 without port",
			hosts:   "::1",
			resolve: true,
			want:    []hostPair{{resolved: "::1", originalHost: "::1"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseHostPairs(tt.hosts, tt.resolve)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseHostPairs(%q, %v) = %+v, want %+v", tt.hosts, tt.resolve, got, tt.want)
			}
		})
	}
}

func TestParseHostsResolveIPv6(t *testing.T) {
	got := parseHosts("[::1]:9000,127.0.0.1:9000", true)
	want := []string{"[::1]:9000", "127.0.0.1:9000"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("parseHosts = %v, want %v", got, want)
	}
}

func TestHostOnly(t *testing.T) {
	tests := map[string]string{
		"":                       "",
		"s3.example.com":         "s3.example.com",
		"s3.example.com:9000":    "s3.example.com",
		"10.0.0.1:443":           "10.0.0.1",
		"[2001:db8::1]:443":      "2001:db8::1",
		"2001:db8::1":            "2001:db8::1",
		"bucket.s3.example.com:": "bucket.s3.example.com",
	}
	for in, want := range tests {
		if got := hostOnly(in); got != want {
			t.Errorf("hostOnly(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestShouldPin(t *testing.T) {
	const orig = "s3.example.com"
	tests := []struct {
		host string
		orig string
		want bool
	}{
		{host: "s3.example.com", orig: orig, want: true},
		{host: "S3.Example.COM", orig: orig, want: true},
		{host: "bucket.s3.example.com", orig: orig, want: true},
		{host: "a.b.s3.example.com", orig: orig, want: true},
		{host: "BUCKET.S3.EXAMPLE.COM", orig: orig, want: true},
		// Not a subdomain: no dot before the suffix.
		{host: "evils3.example.com", orig: orig, want: false},
		// Proxies and unrelated hosts are never pinned.
		{host: "proxy.corp.example", orig: orig, want: false},
		{host: "example.com", orig: orig, want: false},
		{host: ".s3.example.com", orig: orig, want: false},
		{host: "", orig: orig, want: false},
		{host: "s3.example.com", orig: "", want: false},
		{host: "10.0.0.1", orig: "10.0.0.1", want: true},
	}
	for _, tt := range tests {
		if got := shouldPin(tt.host, tt.orig); got != tt.want {
			t.Errorf("shouldPin(%q, %q) = %v, want %v", tt.host, tt.orig, got, tt.want)
		}
	}
}

func TestResolvePinnerDialAddr(t *testing.T) {
	if p := newResolvePinner("10.0.0.1:9000", ""); p != nil {
		t.Fatalf("pinner without originalHost = %+v, want nil", p)
	}
	// A nil pinner must be safe to use and never rewrite.
	var nilPinner *resolvePinner
	if got, pinned := nilPinner.dialAddr("s3.example.com:9000", "443"); got != "s3.example.com:9000" || pinned {
		t.Errorf("nil pinner rewrote address: %q, %v", got, pinned)
	}

	v4 := newResolvePinner("10.0.0.1:9000", "s3.example.com:9000")
	v6 := newResolvePinner("[2001:db8::1]:9000", "s3.example.com:9000")
	tests := []struct {
		name       string
		p          *resolvePinner
		addr       string
		want       string
		wantPinned bool
	}{
		{name: "original host", p: v4, addr: "s3.example.com:9000", want: "10.0.0.1:9000", wantPinned: true},
		{name: "virtual-host bucket", p: v4, addr: "bucket.s3.example.com:9000", want: "10.0.0.1:9000", wantPinned: true},
		{name: "keeps requested port", p: v4, addr: "s3.example.com:443", want: "10.0.0.1:443", wantPinned: true},
		{name: "default port when missing", p: v4, addr: "s3.example.com", want: "10.0.0.1:443", wantPinned: true},
		{name: "proxy untouched", p: v4, addr: "proxy.corp.example:3128", want: "proxy.corp.example:3128"},
		{name: "ipv6 target bracketed", p: v6, addr: "s3.example.com:9000", want: "[2001:db8::1]:9000", wantPinned: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, pinned := tt.p.dialAddr(tt.addr, "443")
			if got != tt.want || pinned != tt.wantPinned {
				t.Errorf("dialAddr(%q) = %q, %v; want %q, %v", tt.addr, got, pinned, tt.want, tt.wantPinned)
			}
		})
	}
}

// hostRecorder returns a handler that records the Host header of each request.
func hostRecorder(hosts chan<- string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hosts <- r.Host
		io.WriteString(w, "ok")
	})
}

// TestWithResolveHostHTTP checks that requests for the original hostname and
// for bucket.<hostname> are dialed to the pinned IP, while the Host header
// keeps the hostname.
func TestWithResolveHostHTTP(t *testing.T) {
	hosts := make(chan string, 2)
	srv := httptest.NewServer(hostRecorder(hosts))
	defer srv.Close()
	_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())

	origHost := net.JoinHostPort("s3.example.invalid", port)
	tr := &http.Transport{}
	withResolveHost(srv.Listener.Addr().String(), origHost, &net.Dialer{}, false)(tr)
	client := &http.Client{Transport: tr}
	defer tr.CloseIdleConnections()

	for _, host := range []string{origHost, "bucket." + origHost} {
		resp, err := client.Get("http://" + host + "/")
		if err != nil {
			t.Fatalf("GET %s: %v", host, err)
		}
		resp.Body.Close()
		if got := <-hosts; got != host {
			t.Errorf("Host header = %q, want %q", got, host)
		}
	}
}

// TestWithResolveHostTLS checks that, when dialing the pinned IP with an empty
// TLSClientConfig.ServerName, the SNI sent is the hostname from the request URL
// (including bucket.<hostname>), not the dialed IP. The httptest certificate
// covers example.com and *.example.com, so verification passes as well.
func TestWithResolveHostTLS(t *testing.T) {
	type seen struct{ host, sni string }
	reqs := make(chan seen, 2)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs <- seen{host: r.Host, sni: r.TLS.ServerName}
		io.WriteString(w, "ok")
	}))
	defer srv.Close()
	_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())

	tr := &http.Transport{
		TLSClientConfig: srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone(),
	}
	if tr.TLSClientConfig.ServerName != "" {
		t.Fatalf("test setup: ServerName should be empty, got %q", tr.TLSClientConfig.ServerName)
	}
	origHost := net.JoinHostPort("example.com", port)
	withResolveHost(srv.Listener.Addr().String(), origHost, &net.Dialer{}, true)(tr)
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr}

	for _, name := range []string{"example.com", "bucket.example.com"} {
		host := net.JoinHostPort(name, port)
		resp, err := client.Get("https://" + host + "/")
		if err != nil {
			t.Fatalf("GET %s: %v", host, err)
		}
		resp.Body.Close()
		got := <-reqs
		if got.host != host {
			t.Errorf("Host header = %q, want %q", got.host, host)
		}
		if got.sni != name {
			t.Errorf("SNI = %q, want %q", got.sni, name)
		}
	}
}

func testCLIContext(t *testing.T) *cli.Context {
	t.Helper()
	return cli.NewContext(cli.NewApp(), flag.NewFlagSet(t.Name(), flag.ContinueOnError), nil)
}

// TestClientTransportDefaultKeepsLocalAddr is a regression test: without
// --resolve-host, the plain HTTP transport must still bind to localIP.
// 192.0.2.1 (TEST-NET-1) is not assigned to the test machine, so a transport
// that binds to it fails to dial, while one that ignores it succeeds.
func TestClientTransportDefaultKeepsLocalAddr(t *testing.T) {
	hosts := make(chan string, 1)
	srv := httptest.NewServer(hostRecorder(hosts))
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	tr := clientTransportDefault(testCLIContext(t), "192.0.2.1", addr, "")
	resp, err := (&http.Client{Transport: tr}).Get("http://" + addr + "/")
	if err == nil {
		resp.Body.Close()
		t.Fatal("request succeeded, so the transport did not bind to localIP 192.0.2.1")
	}

	// With --resolve-host (originalHost set) the pinned dial is used, and it
	// is bound to localIP too.
	_, port, _ := net.SplitHostPort(addr)
	origHost := net.JoinHostPort("s3.example.invalid", port)
	tr = clientTransportDefault(testCLIContext(t), "127.0.0.1", addr, origHost)
	resp, err = (&http.Client{Transport: tr}).Get("http://" + origHost + "/")
	if err != nil {
		t.Fatalf("pinned GET: %v", err)
	}
	resp.Body.Close()
	<-hosts
}

func TestBuildCatalogURLs(t *testing.T) {
	hosts := []string{"127.0.0.1:9000", "[::1]:9000", "::1", "2001:db8::1", "s3.example.com"}
	want := []string{
		"https://127.0.0.1:9000/_iceberg",
		"https://[::1]:9000/_iceberg",
		"https://[::1]/_iceberg",
		"https://[2001:db8::1]/_iceberg",
		"https://s3.example.com/_iceberg",
	}
	got := buildCatalogURLs(hosts, true, iceberg.ExternalCatalogNone)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("buildCatalogURLs = %v, want %v", got, want)
	}
	for _, u := range got {
		if _, err := url.Parse(u); err != nil {
			t.Errorf("invalid catalog URL %q: %v", u, err)
		}
	}
}

func TestClientEndpoint(t *testing.T) {
	tests := []struct {
		name         string
		resolved     string
		originalHost string
		rdma         bool
		want         string
	}{
		{name: "no resolve-host", resolved: "s3.example.com:9000", want: "s3.example.com:9000"},
		{name: "no resolve-host with rdma", resolved: "s3.example.com:9000", rdma: true, want: "s3.example.com:9000"},
		{name: "resolve-host uses hostname", resolved: "10.0.0.1:9000", originalHost: "s3.example.com:9000", want: "s3.example.com:9000"},
		// libminiocpp dials the endpoint itself, so RDMA must keep the pinned IP.
		{name: "resolve-host with rdma keeps IP", resolved: "10.0.0.1:9000", originalHost: "s3.example.com:9000", rdma: true, want: "10.0.0.1:9000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := clientEndpoint(tt.resolved, tt.originalHost, tt.rdma); got != tt.want {
				t.Errorf("clientEndpoint(%q, %q, %v) = %q, want %q", tt.resolved, tt.originalHost, tt.rdma, got, tt.want)
			}
		})
	}
}

func TestResolveHostProxy(t *testing.T) {
	pairs := []hostPair{
		{resolved: "10.0.0.1:9000", originalHost: "s3.example.com:9000"},
		{resolved: "10.0.0.2:9000", originalHost: "s3.example.com:9000"},
	}
	proxy := func(*http.Request) (*url.URL, error) {
		return &url.URL{Scheme: "http", Host: "proxy.corp.example:3128"}, nil
	}
	noProxy := func(*http.Request) (*url.URL, error) { return nil, nil }

	var gotScheme string
	recordScheme := func(r *http.Request) (*url.URL, error) {
		gotScheme = r.URL.Scheme
		return nil, nil
	}

	if got := resolveHostProxy(pairs, true, proxy); got != "proxy.corp.example:3128" {
		t.Errorf("with proxy = %q, want proxy.corp.example:3128", got)
	}
	if got := resolveHostProxy(pairs, true, noProxy); got != "" {
		t.Errorf("without proxy = %q, want empty", got)
	}
	// Pairs without originalHost (--resolve-host off) are never checked.
	if got := resolveHostProxy([]hostPair{{resolved: "s3.example.com:9000"}}, false, proxy); got != "" {
		t.Errorf("without originalHost = %q, want empty", got)
	}
	resolveHostProxy(pairs, false, recordScheme)
	if gotScheme != "http" {
		t.Errorf("scheme for insecure = %q, want http", gotScheme)
	}
	resolveHostProxy(pairs, true, recordScheme)
	if gotScheme != "https" {
		t.Errorf("scheme for TLS = %q, want https", gotScheme)
	}
}
