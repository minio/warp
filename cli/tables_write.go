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
	"context"
	"time"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/pkg/iceberg"
)

var tablesWriteFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "catalog-name",
		Usage: "Catalog name to use",
		Value: "benchmarkcatalog",
	},
	cli.IntFlag{
		Name:  "namespace-width",
		Usage: "Width of the N-ary namespace tree (children per namespace)",
		Value: 1,
	},
	cli.IntFlag{
		Name:  "namespace-depth",
		Usage: "Depth of the N-ary namespace tree",
		Value: 1,
	},
	cli.IntFlag{
		Name:  "tables-per-ns",
		Usage: "Number of tables per leaf namespace",
		Value: 3,
	},
	cli.IntFlag{
		Name:  "columns",
		Usage: "Number of columns per table schema",
		Value: 10,
	},
	cli.IntFlag{
		Name:  "properties",
		Usage: "Number of properties per entity",
		Value: 5,
	},
	cli.StringFlag{
		Name:  "base-location",
		Usage: "Base storage location for tables",
		Value: "s3://benchmark",
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

var tablesWriteCombinedFlags = combineFlags(globalFlags, ioFlags, tablesWriteFlags, benchFlags, analyzeFlags)

var tablesWriteCmd = cli.Command{
	Name:   "write",
	Usage:  "benchmark Iceberg table write performance",
	Action: mainTablesWrite,
	Before: setGlobalsFromContext,
	Flags:  tablesWriteCombinedFlags,
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
  - Creates warehouse, namespaces, and tables using the tree structure
  - Downloads TPC-DS data from GCS if --tpcds is used and data isn't cached

  Workers round-robin across tables in the tree, creating commit conflicts
  when multiple workers target the same table.

  Workflow:
  1. Creates warehouse and dataset tree (namespaces + tables)
  2. Downloads/generates data files
  3. Uploads parquet files to S3 storage
  4. Commits file references to Iceberg tables via REST catalog
  5. Handles commit conflicts with exponential backoff retry

EXAMPLES:
  # Benchmark with generated data (default: 1 namespace, 3 tables)
  {{.HelpName}} --host=localhost:9000 --access-key=minioadmin --secret-key=minioadmin

  # More tables for higher concurrency
  {{.HelpName}} --host=localhost:9000 --access-key=minioadmin --secret-key=minioadmin \
    --tables-per-ns=10 --concurrent=20

  # Benchmark with TPC-DS data
  {{.HelpName}} --host=localhost:9000 --access-key=minioadmin --secret-key=minioadmin \
    --tpcds --scale-factor=sf100

  # Hierarchical namespace structure
  {{.HelpName}} --host=localhost:9000 --access-key=minioadmin --secret-key=minioadmin \
    --namespace-width=2 --namespace-depth=2 --tables-per-ns=5

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

func mainTablesWrite(ctx *cli.Context) error {
	checkTablesWriteSyntax(ctx)

	backoffBase, err := time.ParseDuration(ctx.String("backoff-base"))
	if err != nil {
		backoffBase = 100 * time.Millisecond
	}

	hosts := parseHosts(ctx.String("host"), ctx.Bool("resolve-host"))
	useTLS := ctx.Bool("tls") || ctx.Bool("ktls")
	catalogURLs := buildCatalogURLs(hosts, useTLS)

	catalogCfg := iceberg.CatalogConfig{
		CatalogURI: catalogURLs[0],
		Warehouse:  ctx.String("catalog-name"),
		AccessKey:  ctx.String("access-key"),
		SecretKey:  ctx.String("secret-key"),
		Region:     ctx.String("region"),
	}

	err = iceberg.EnsureWarehouse(context.Background(), catalogCfg)
	fatalIf(probe.NewError(err), "Failed to ensure warehouse")

	cat, err := iceberg.NewCatalog(context.Background(), catalogCfg)
	fatalIf(probe.NewError(err), "Failed to create catalog")

	catalogPool, err := iceberg.NewCatalogPool(context.Background(), catalogURLs, catalogCfg)
	fatalIf(probe.NewError(err), "Failed to create catalog pool")

	treeCfg := iceberg.TreeConfig{
		NamespaceWidth:   ctx.Int("namespace-width"),
		NamespaceDepth:   ctx.Int("namespace-depth"),
		TablesPerNS:      ctx.Int("tables-per-ns"),
		ViewsPerNS:       0,
		ColumnsPerTable:  ctx.Int("columns"),
		ColumnsPerView:   0,
		PropertiesPerNS:  ctx.Int("properties"),
		PropertiesPerTbl: ctx.Int("properties"),
		PropertiesPerVw:  0,
		BaseLocation:     ctx.String("base-location"),
		CatalogName:      ctx.String("catalog-name"),
	}

	b := bench.Iceberg{
		Common:      getCommon(ctx, nil),
		Catalog:     cat,
		CatalogPool: catalogPool,
		TreeConfig:  treeCfg,
		CatalogURI:  catalogURLs[0],
		AccessKey:   ctx.String("access-key"),
		SecretKey:   ctx.String("secret-key"),
		NumFiles:    ctx.Int("num-files"),
		RowsPerFile: ctx.Int("rows-per-file"),
		CacheDir:    ctx.String("cache-dir"),
		MaxRetries:  ctx.Int("max-retries"),
		BackoffBase: backoffBase,
		UseTPCDS:    ctx.Bool("tpcds"),
		ScaleFactor: ctx.String("scale-factor"),
		TPCDSTable:  ctx.String("tpcds-table"),
	}

	return runBench(ctx, &b)
}

func checkTablesWriteSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}
	if ctx.String("host") == "" {
		console.Fatal("--host is required")
	}
	if ctx.String("access-key") == "" {
		console.Fatal("--access-key is required")
	}
	if ctx.String("secret-key") == "" {
		console.Fatal("--secret-key is required")
	}
	if ctx.Int("namespace-width") < 1 {
		console.Fatal("--namespace-width must be at least 1")
	}
	if ctx.Int("namespace-depth") < 1 {
		console.Fatal("--namespace-depth must be at least 1")
	}
	if ctx.Int("tables-per-ns") < 1 {
		console.Fatal("--tables-per-ns must be at least 1")
	}
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
