/*
 * Warp (C) 2019-2024 MinIO, Inc.
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

import "github.com/minio/cli"

var icebergSubcommands = []cli.Command{
	icebergWriteCmd,
	catalogReadCmd,
	catalogCommitsCmd,
	catalogMixedCmd,
}

var icebergCmd = cli.Command{
	Name:            "iceberg",
	Usage:           "benchmark Iceberg catalog operations",
	Action:          icebergCmdNotFound,
	Before:          setGlobalsFromContext,
	Subcommands:     icebergSubcommands,
	HideHelpCommand: true,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} COMMAND [FLAGS]

COMMANDS:
  {{range .VisibleCommands}}{{.Name}}{{"\t"}}{{.Usage}}
  {{end}}
EXAMPLES:
  # Benchmark Iceberg table write performance (parquet upload + commit)
  {{.HelpName}} write --host=minio:9000 --access-key=minioadmin --secret-key=minioadmin \
    --warehouse=my-warehouse --duration=1m

  # Benchmark catalog read operations
  {{.HelpName}} catalog-read --host=minio:9000 --access-key=minioadmin --secret-key=minioadmin

  # Benchmark catalog commit operations
  {{.HelpName}} catalog-commits --host=minio:9000 --access-key=minioadmin --secret-key=minioadmin

  # Multiple hosts with round-robin
  {{.HelpName}} catalog-read --host=minio1:9000,minio2:9000 --access-key=minioadmin --secret-key=minioadmin

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

func icebergCmdNotFound(ctx *cli.Context) error {
	if ctx.Args().First() != "" {
		return cli.ShowCommandHelp(ctx, ctx.Args().First())
	}
	return cli.ShowSubcommandHelp(ctx)
}
