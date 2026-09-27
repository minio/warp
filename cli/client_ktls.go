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
	"net/http"
	"os"

	"github.com/minio/cli"
	"gitlab.com/go-extension/tls"
)

func clientTransportKTLS(ctx *cli.Context, localIP string) http.RoundTripper {
	// Keep TLS config.
	tlsConfig := &tls.Config{
		RootCAs: mustGetSystemCertPool(),
		// Can't use SSLv3 because of POODLE and BEAST
		// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
		// Can't use TLSv1.1 because of RC4 cipher usage
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: ctx.Bool("insecure"),
		ClientSessionCache: tls.NewLRUClientSessionCache(1024), // up to 1024 nodes

		// We don't care about the size.
		CertificateCompressionDisabled: true,
	}
	setupKTLS(tlsConfig)

	if ctx.Bool("debug") {
		tlsConfig.KeyLogWriter = os.Stdout
	}

	dialer := makeDialer(localIP)
	dialTLSContext := func(ctx context.Context, network, addr string) (net.Conn, error) {
		serverName, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}

		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		tlsConfig := tlsConfig.Clone()
		tlsConfig.ServerName = serverName
		return tls.Client(conn, tlsConfig).Compatible(), nil
	}

	return newClientTransport(ctx, withDialTLSContext(dialTLSContext), withLocalAddr(localIP))
}
