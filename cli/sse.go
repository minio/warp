/*
 * Warp (C) 2019- MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cli

import (
	"crypto/rand"

	"github.com/minio/cli"
	"github.com/minio/minio-go/v6/pkg/encrypt"
)

var sseKey encrypt.ServerSide

// newSSE returns a randomly generated key if SSE is requested.
// Only one key will be generated.
func newSSE(ctx *cli.Context) encrypt.ServerSide {
	if !ctx.Bool("encrypt") {
		return nil
	}
	if sseKey != nil {
		return sseKey
	}
	var key [32]byte
	_, err := rand.Read(key[:])
	if err != nil {
		panic(err)
	}
	sseKey, err = encrypt.NewSSEC(key[:])
	if err != nil {
		panic(err)
	}
	return sseKey
}
