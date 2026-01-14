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

import (
	"time"

	"github.com/minio/cli"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
)

var icebergFlags = []cli.Flag{
	cli.StringFlag{
		Name:   "catalog-uri",
		Usage:  "Iceberg REST catalog URI",
		EnvVar: "ICEBERG_CATALOG_URI",
		Value:  "",
	},
	cli.StringFlag{
		Name:   "warehouse",
		Usage:  "Iceberg warehouse name or location",
		EnvVar: "ICEBERG_WAREHOUSE",
		Value:  "",
	},
	cli.StringFlag{
		Name:  "namespace",
		Usage: "Iceberg namespace",
		Value: "benchmark",
	},
	cli.StringFlag{
		Name:  "table",
		Usage: "Iceberg table name",
		Value: "warp_benchmark",
	},
	cli.IntFlag{
		Name:  "num-files",
		Usage: "Number of parquet files to generate per worker",
		Value: 10,
	},
	cli.IntFlag{
		Name:  "rows-per-file",
		Usage: "Number of rows per parquet file",
		Value: 10000,
	},
	cli.StringFlag{
		Name:  "cache-dir",
		Usage: "Local cache directory for generated data",
		Value: "/tmp/warp-iceberg-cache",
	},
	cli.IntFlag{
		Name:  "iterations",
		Usage: "Number of upload+commit iterations per worker",
		Value: 10,
	},
	cli.IntFlag{
		Name:  "max-retries",
		Usage: "Maximum commit retries on conflict",
		Value: 10,
	},
	cli.StringFlag{
		Name:  "backoff-base",
		Usage: "Base backoff duration for commit retries",
		Value: "100ms",
	},
}

var icebergCombinedFlags = combineFlags(globalFlags, ioFlags, icebergFlags, benchFlags, analyzeFlags)

var icebergCmd = cli.Command{
	Name:   "iceberg",
	Usage:  "benchmark Iceberg table write performance",
	Action: mainIceberg,
	Before: setGlobalsFromContext,
	Flags:  icebergCombinedFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]

DESCRIPTION:
  Benchmark Iceberg table write performance by generating parquet files,
  uploading them to S3 storage, and committing them to an Iceberg table.
  Tests commit conflict handling under concurrent load.

  The benchmark:
  1. Generates parquet files locally with configurable size
  2. Uploads parquet files to S3 storage
  3. Commits file references to an Iceberg table via REST catalog
  4. Handles commit conflicts with exponential backoff retry

  Requires an Iceberg REST catalog (e.g., MinIO AIStor with Iceberg support).

EXAMPLES:
  # Single node benchmark
  {{.HelpName}} --host=minio:9000 --access-key=minioadmin --secret-key=minioadmin \
    --catalog-uri=http://minio:9000/_iceberg --warehouse=my-warehouse \
    --bucket=my-warehouse --num-files=10 --iterations=5

  # Distributed benchmark with more data
  {{.HelpName}} --host=minio:9000 --access-key=minioadmin --secret-key=minioadmin \
    --catalog-uri=http://minio:9000/_iceberg --warehouse=my-warehouse \
    --bucket=my-warehouse --warp-client=node1:7761,node2:7761 \
    --num-files=50 --rows-per-file=100000

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

func mainIceberg(ctx *cli.Context) error {
	checkIcebergSyntax(ctx)

	// Parse backoff duration
	backoffBase, err := time.ParseDuration(ctx.String("backoff-base"))
	if err != nil {
		backoffBase = 100 * time.Millisecond
	}

	// Get catalog URI - default to host if not specified
	catalogURI := ctx.String("catalog-uri")
	if catalogURI == "" {
		host := ctx.String("host")
		scheme := "http"
		if ctx.Bool("tls") {
			scheme = "https"
		}
		catalogURI = scheme + "://" + host + "/_iceberg"
	}

	// Get warehouse - default to bucket if not specified
	warehouse := ctx.String("warehouse")
	if warehouse == "" {
		warehouse = ctx.String("bucket")
	}

	b := bench.Iceberg{
		Common:      getCommon(ctx, nil),
		CatalogURI:  catalogURI,
		Warehouse:   warehouse,
		Namespace:   ctx.String("namespace"),
		TableName:   ctx.String("table"),
		NumFiles:    ctx.Int("num-files"),
		RowsPerFile: ctx.Int("rows-per-file"),
		CacheDir:    ctx.String("cache-dir"),
		Iterations:  ctx.Int("iterations"),
		MaxRetries:  ctx.Int("max-retries"),
		BackoffBase: backoffBase,
	}

	// Store credentials in ExtraFlags for the benchmark to access
	if b.ExtraFlags == nil {
		b.ExtraFlags = make(map[string]string)
	}
	b.ExtraFlags["access-key"] = ctx.String("access-key")
	b.ExtraFlags["secret-key"] = ctx.String("secret-key")

	return runBench(ctx, &b)
}

func checkIcebergSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
