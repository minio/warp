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
	"crypto/tls"
	"net"
	"net/http"
	"os"

	"github.com/minio/cli"
)

func clientTransportTLS(ctx *cli.Context, localIP, resolvedHost, originalHost string) http.RoundTripper {
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
	}

	if ctx.Bool("debug") {
		tlsConfig.KeyLogWriter = os.Stdout
	}

	dialer := makeDialer(localIP)
	opts := []transportOption{withTLSConfig(tlsConfig)}
	if originalHost != "" {
		opts = append(opts, withResolveHost(resolvedHost, originalHost, dialer, true))
	} else {
		opts = append(opts, withLocalAddr(localIP))
	}
	return newClientTransport(ctx, opts...)
}
