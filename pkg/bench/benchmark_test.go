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

package bench

import (
	"net/http"
	"testing"

	"github.com/minio/minio-go/v7"
)

func testMinioClient(t *testing.T) *minio.Client {
	t.Helper()
	cl, err := minio.New("s3.example.com:9000", &minio.Options{Secure: true})
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

func TestCommonEndpointOf(t *testing.T) {
	cl := testMinioClient(t)
	const fallback = "https://s3.example.com:9000"

	if got := (&Common{}).endpointOf(cl); got != fallback {
		t.Errorf("no ClientEndpoint: got %q, want %q", got, fallback)
	}
	empty := &Common{ClientEndpoint: func(*minio.Client) string { return "" }}
	if got := empty.endpointOf(cl); got != fallback {
		t.Errorf("empty ClientEndpoint: got %q, want %q", got, fallback)
	}
	pinned := &Common{ClientEndpoint: func(*minio.Client) string { return "https://10.0.0.1:9000" }}
	if got := pinned.endpointOf(cl); got != "https://10.0.0.1:9000" {
		t.Errorf("pinned ClientEndpoint: got %q, want the pinned IP", got)
	}
}

func TestCommonTransportOf(t *testing.T) {
	cl := testMinioClient(t)
	shared, own := &http.Transport{}, &http.Transport{}

	if got := (&Common{Transport: shared}).transportOf(cl); got != shared {
		t.Error("no ClientTransport: want the shared Transport")
	}
	none := &Common{Transport: shared, ClientTransport: func(*minio.Client) http.RoundTripper { return nil }}
	if got := none.transportOf(cl); got != shared {
		t.Error("nil ClientTransport: want the shared Transport")
	}
	pinned := &Common{Transport: shared, ClientTransport: func(*minio.Client) http.RoundTripper { return own }}
	if got := pinned.transportOf(cl); got != own {
		t.Error("pinned ClientTransport: want the client's own transport")
	}
}

// TestPutPostClient checks that presigned POST uploads use the transport of
// the client they are made for (--resolve-host), and the shared client otherwise.
func TestPutPostClient(t *testing.T) {
	cl := testMinioClient(t)
	shared, own := &http.Transport{}, &http.Transport{}
	sharedClient := &http.Client{Transport: shared}

	u := &Put{Common: Common{Transport: shared}, cl: sharedClient}
	if got := u.postClient(cl); got != sharedClient {
		t.Error("without a pinned transport, want the shared http.Client")
	}

	u.ClientTransport = func(*minio.Client) http.RoundTripper { return own }
	if got := u.postClient(cl); got.Transport != own {
		t.Error("with a pinned transport, want an http.Client using it")
	}
}
