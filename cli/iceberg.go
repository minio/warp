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
		Usage: "Local cache directory for generated/downloaded data",
		Value: "/tmp/warp-iceberg-cache",
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
	cli.BoolFlag{
		Name:  "tpcds",
		Usage: "Use TPC-DS data from GCS (auto-downloads if not cached)",
	},
	cli.StringFlag{
		Name:  "scale-factor",
		Usage: "TPC-DS scale factor (sf1, sf10, sf100, sf1000)",
		Value: "sf100",
	},
	cli.StringFlag{
		Name:  "tpcds-table",
		Usage: "TPC-DS table name to use",
		Value: "store_sales",
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
  Benchmark Iceberg table write performance by uploading parquet files
  to S3 storage and committing them to an Iceberg table.
  Tests commit conflict handling under concurrent load.

  The benchmark can operate in two modes:
  1. Generated data (default): Creates synthetic parquet files
  2. TPC-DS data (--tpcds): Uses real TPC-DS benchmark data from GCS

  The benchmark automatically:
  - Creates bucket, namespace, and table if they don't exist
  - Downloads TPC-DS data from GCS if --tpcds is used and data isn't cached

  Workflow:
  1. Creates bucket (if needed)
  2. Downloads/generates data files
  3. Creates Iceberg namespace and table (if needed)
  4. Uploads parquet files to S3 storage
  5. Commits file references to Iceberg table via REST catalog
  6. Handles commit conflicts with exponential backoff retry

  Requires an Iceberg REST catalog (e.g., MinIO AIStor with Iceberg support).
  The warehouse must be created beforehand using mc or MinIO Console.

EXAMPLES:
  # Benchmark with generated data
  {{.HelpName}} --host=minio:9000 --access-key=minioadmin --secret-key=minioadmin \
    --catalog-uri=http://minio:9000/_iceberg --warehouse=my-warehouse \
    --num-files=10 --duration=1m

  # Benchmark with TPC-DS data (auto-downloads if not cached)
  {{.HelpName}} --host=minio:9000 --access-key=minioadmin --secret-key=minioadmin \
    --catalog-uri=http://minio:9000/_iceberg --warehouse=my-warehouse \
    --tpcds --scale-factor=sf100

  # Distributed benchmark
  {{.HelpName}} --host=minio:9000 --access-key=minioadmin --secret-key=minioadmin \
    --catalog-uri=http://minio:9000/_iceberg --warehouse=my-warehouse \
    --warp-client=node1:7761,node2:7761 --tpcds --scale-factor=sf100

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

func mainIceberg(ctx *cli.Context) error {
	checkIcebergSyntax(ctx)

	backoffBase, err := time.ParseDuration(ctx.String("backoff-base"))
	if err != nil {
		backoffBase = 100 * time.Millisecond
	}

	host := ctx.String("host")
	useTLS := ctx.Bool("tls")

	catalogURI := ctx.String("catalog-uri")
	if catalogURI == "" {
		scheme := "http"
		if useTLS {
			scheme = "https"
		}
		catalogURI = scheme + "://" + host + "/_iceberg"
	}

	warehouse := ctx.String("warehouse")
	if warehouse == "" {
		console.Fatal("--warehouse is required")
	}

	common := getCommon(ctx, nil)
	common.Bucket = warehouse

	b := bench.Iceberg{
		Common:      common,
		CatalogURI:  catalogURI,
		Warehouse:   warehouse,
		Namespace:   ctx.String("namespace"),
		TableName:   ctx.String("table"),
		NumFiles:    ctx.Int("num-files"),
		RowsPerFile: ctx.Int("rows-per-file"),
		CacheDir:    ctx.String("cache-dir"),
		MaxRetries:  ctx.Int("max-retries"),
		BackoffBase: backoffBase,
		UseTPCDS:    ctx.Bool("tpcds"),
		ScaleFactor: ctx.String("scale-factor"),
		TPCDSTable:  ctx.String("tpcds-table"),
	}

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
