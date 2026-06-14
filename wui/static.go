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

package wui

import (
	"embed"
	"io/fs"
)

//go:embed static/*
var staticFiles embed.FS

// StaticFS returns the dashboard's static assets (index.html, app.js,
// compare.html, etc.) rooted at the asset directory. The control plane uses this
// to serve the dashboard and comparison views through its own HTTP server.
func StaticFS() fs.FS {
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic("wui: static assets: " + err.Error())
	}
	return sub
}
