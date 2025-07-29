/*
 * Warp (C) 2019-2020 MinIO, Inc.
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
	"github.com/minio/cli"
	"github.com/minio/minio-go/v7/pkg/encrypt"
)

var sseKey encrypt.ServerSide

// newSSE returns a randomly generated key if SSE is requested.
// Only one key will be generated.
func newSSE(ctx *cli.Context) encrypt.ServerSide {
	if !ctx.Bool("encrypt") && !ctx.Bool("sse-s3-encrypt") {
		return nil
	}
	if sseKey != nil {
		return sseKey
	}

	if ctx.Bool("sse-s3-encrypt") {
		sseKey = encrypt.NewSSE()
		return sseKey
	}

	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	sseKey, err := encrypt.NewSSEC(key[:])
	if err != nil {
		panic(err)
	}
	return sseKey
}
