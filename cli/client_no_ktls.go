//go:build !ktls

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
	"errors"
	"net/http"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
)

func clientTransportKernelTLS(ctx *cli.Context) http.RoundTripper {
	fatal(probe.NewError(errors.New("kernel tls is experimental and not built-in, use `go build -tags ktls` to enable it")), "ktls")
	return nil
}
