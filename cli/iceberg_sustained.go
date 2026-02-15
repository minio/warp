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

var icebergSustainedFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "external-catalog",
		Usage: "External catalog type (polaris)",
		Value: "",
	},
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
		Value: 1,
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
		Usage: "Base storage location for tables (default: s3://{catalog-name})",
		Value: "",
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
		Name:  "files-per-commit",
		Usage: "Number of files to upload before each commit",
		Value: 1,
	},
	cli.IntFlag{
		Name:  "max-retries",
		Usage: "Maximum commit retries on conflict",
		Value: 4,
	},
	cli.StringFlag{
		Name:  "backoff-base",
		Usage: "Base backoff duration for commit retries",
		Value: "100ms",
	},
	cli.StringFlag{
		Name:  "backoff-max",
		Usage: "Maximum backoff duration for commit retries",
		Value: "60s",
	},
	cli.BoolTFlag{
		Name:  "skip-upload",
		Usage: "Upload files once in prepare, then only benchmark commits (no upload during benchmark)",
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
	cli.StringFlag{
		Name:  "s3-host",
		Usage: "S3 host for file uploads when using external catalog (e.g., localhost:9000)",
		Value: "",
	},
	cli.StringFlag{
		Name:  "s3-access-key",
		Usage: "S3 access key for file uploads when using external catalog",
		Value: "",
	},
	cli.StringFlag{
		Name:  "s3-secret-key",
		Usage: "S3 secret key for file uploads when using external catalog",
		Value: "",
	},
	cli.BoolFlag{
		Name:  "s3-tls",
		Usage: "Use TLS for S3 connection when using external catalog",
	},
	cli.BoolFlag{
		Name:  "simulate-read",
		Usage: "Enable parallel LoadTable reads during write benchmark",
	},
	cli.IntFlag{
		Name:  "read-concurrent",
		Usage: "Number of parallel read workers doing LoadTable operations",
		Value: 20,
	},
	cli.Float64Flag{
		Name:  "read-rps-limit",
		Usage: "RPS limit for read workers (0 to disable)",
		Value: 400,
	},
}

var icebergSustainedCombinedFlags = combineFlags(globalFlags, ioFlags, icebergSustainedFlags, benchFlags, analyzeFlags)

var icebergSustainedCmd = cli.Command{
	Name:   "sustained",
	Usage:  "sustained Iceberg workload with controlled RPS (commits + reads)",
	Action: mainIcebergSustained,
	Before: setGlobalsFromContext,
	Flags:  icebergSustainedCombinedFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]

DESCRIPTION:
  Run a sustained Iceberg workload with controlled request rates.
  Designed for long-running tests with specific RPS limits for commits and reads.

  The benchmark supports two concurrent workloads:
  1. Commits: Upload parquet files and commit to Iceberg tables (controlled by --rps-limit)
  2. Reads: LoadTable operations (enabled with --simulate-read, controlled by --read-rps-limit)

  Data modes:
  1. Generated data (default): Creates synthetic parquet files
  2. TPC-DS data (--tpcds): Uses real TPC-DS benchmark data from GCS

  By default, files are uploaded once during prepare (--skip-upload=true).
  Use --skip-upload=false to upload files during benchmark.

EXAMPLES:
  # Sustained commits (1 every 2 sec) with 400 reads/sec
  {{.HelpName}} --host=localhost:9000 --access-key=minioadmin --secret-key=minioadmin \
    --duration=1h --concurrent=1 --rps-limit=0.5 \
    --simulate-read --read-concurrent=20 --read-rps-limit=400

  # Commit-only benchmark
  {{.HelpName}} --host=localhost:9000 --access-key=minioadmin --secret-key=minioadmin \
    --rps-limit=1

  # With uploads during benchmark
  {{.HelpName}} --host=localhost:9000 --access-key=minioadmin --secret-key=minioadmin \
    --skip-upload=false --duration=1h

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

func mainIcebergSustained(ctx *cli.Context) error {
	checkIcebergSustainedSyntax(ctx)

	backoffBase, err := time.ParseDuration(ctx.String("backoff-base"))
	if err != nil {
		backoffBase = 100 * time.Millisecond
	}

	backoffMax, err := time.ParseDuration(ctx.String("backoff-max"))
	if err != nil {
		backoffMax = 60 * time.Second
	}

	hosts := parseHosts(ctx.String("host"), ctx.Bool("resolve-host"))
	useTLS := ctx.Bool("tls") || ctx.Bool("ktls")
	externalCatalog := iceberg.ExternalCatalogType(ctx.String("external-catalog"))
	catalogURLs := buildCatalogURLs(hosts, useTLS, externalCatalog)

	catalogCfg := iceberg.CatalogConfig{
		CatalogURI:      catalogURLs[0],
		Warehouse:       ctx.String("catalog-name"),
		AccessKey:       ctx.String("access-key"),
		SecretKey:       ctx.String("secret-key"),
		Region:          ctx.String("region"),
		ExternalCatalog: externalCatalog,
	}

	if externalCatalog == iceberg.ExternalCatalogNone {
		_ = iceberg.EnsureWarehouse(context.Background(), catalogCfg)
	}

	cat, err := iceberg.NewCatalog(context.Background(), catalogCfg)
	fatalIf(probe.NewError(err), "Failed to create catalog")

	catalogPool, err := iceberg.NewCatalogPool(context.Background(), catalogURLs, catalogCfg)
	fatalIf(probe.NewError(err), "Failed to create catalog pool")

	baseLocation := ctx.String("base-location")
	if baseLocation == "" {
		baseLocation = "s3://" + ctx.String("catalog-name")
	}

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
		BaseLocation:     baseLocation,
		CatalogName:      ctx.String("catalog-name"),
	}

	b := bench.Iceberg{
		Common:          getCommon(ctx, nil),
		Catalog:         cat,
		CatalogPool:     catalogPool,
		TreeConfig:      treeCfg,
		CatalogURI:      catalogURLs[0],
		AccessKey:       ctx.String("access-key"),
		SecretKey:       ctx.String("secret-key"),
		ExternalCatalog: externalCatalog,
		NumFiles:        ctx.Int("num-files"),
		RowsPerFile:     ctx.Int("rows-per-file"),
		CacheDir:        ctx.String("cache-dir"),
		FilesPerCommit:  ctx.Int("files-per-commit"),
		MaxRetries:      ctx.Int("max-retries"),
		BackoffBase:     backoffBase,
		BackoffMax:      backoffMax,
		SkipUpload:      ctx.Bool("skip-upload"),
		UseTPCDS:        ctx.Bool("tpcds"),
		ScaleFactor:     ctx.String("scale-factor"),
		TPCDSTable:      ctx.String("tpcds-table"),
		S3Hosts:         parseS3Hosts(ctx.String("s3-host")),
		S3AccessKey:     ctx.String("s3-access-key"),
		S3SecretKey:     ctx.String("s3-secret-key"),
		S3TLS:           ctx.Bool("s3-tls"),
		SimulateRead:    ctx.Bool("simulate-read"),
		ReadConcurrent:  ctx.Int("read-concurrent"),
		ReadRpsLimit:    ctx.Float64("read-rps-limit"),
	}

	return runBench(ctx, &b)
}

func checkIcebergSustainedSyntax(ctx *cli.Context) {
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

func parseS3Hosts(h string) []string {
	if h == "" {
		return nil
	}
	return parseHosts(h, false)
}
