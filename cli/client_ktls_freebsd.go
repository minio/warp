/*
 * Warp (C) 2019-2026 MinIO, Inc.
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

import "gitlab.com/go-extension/tls"

func setupKTLS(tlsConfig *tls.Config) {
	tlsConfig.KernelOptions.TX = true

	// Disable RX offload by default due to severe performance regressions and issues
	// - https://github.com/golang/go/issues/44506#issuecomment-2387977030
	// - https://github.com/golang/go/issues/44506#issuecomment-2765047544
	tlsConfig.KernelOptions.RX = false
}
