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

import (
	"net/http"
	"path/filepath"
	"time"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/v3/console"

	"github.com/minio/warp/pkg/control"
)

var controlFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "addr",
		Value: "127.0.0.1:7762",
		Usage: "Address for the control-plane UI to listen on. Use 0.0.0.0:7762 to expose beyond loopback (e.g. behind a reverse proxy).",
	},
	cli.StringFlag{
		Name:  "dir",
		Value: "warp-control",
		Usage: "Directory for the control-plane store and per-run artifacts.",
	},
}

var controlCmd = cli.Command{
	Name:   "control",
	Usage:  "run the benchmark control-plane web UI",
	Action: mainControl,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, controlFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]

  The control plane stores benchmark scenarios, manages a pool of warp clients,
  runs scenarios across them in distributed mode and serves a web UI.

  Start warp clients elsewhere with: warp client 0.0.0.0:7761
  then register their addresses in the UI.

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

func mainControl(ctx *cli.Context) error {
	dir := ctx.String("dir")
	storePath := filepath.Join(dir, "store.json")

	store, err := control.NewStore(storePath)
	fatalIf(probe.NewError(err), "Unable to open control-plane store")

	addr := ctx.String("addr")
	exec := &control.DistributedExecutor{}
	svc := control.NewService(store, exec, dir)

	server := &http.Server{
		Addr:              addr,
		Handler:           svc.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	console.Infoln("Warp control plane listening on http://" + addr)
	console.Infoln("Store:", storePath)
	err = server.ListenAndServe()
	fatalIf(probe.NewError(err), "Control-plane server stopped")
	return nil
}
