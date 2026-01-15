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

var catalogCommitsFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "catalog-name",
		Usage: "Catalog name to use",
		Value: "benchmarkcatalog",
	},
	cli.IntFlag{
		Name:  "namespace-width",
		Usage: "Width of the N-ary namespace tree (children per namespace)",
		Value: 2,
	},
	cli.IntFlag{
		Name:  "namespace-depth",
		Usage: "Depth of the N-ary namespace tree",
		Value: 3,
	},
	cli.IntFlag{
		Name:  "tables-per-ns",
		Usage: "Number of tables per leaf namespace",
		Value: 5,
	},
	cli.IntFlag{
		Name:  "views-per-ns",
		Usage: "Number of views per leaf namespace",
		Value: 5,
	},
	cli.StringFlag{
		Name:  "base-location",
		Usage: "Base storage location for tables",
		Value: "s3://benchmark",
	},
	cli.IntFlag{
		Name:  "table-commits-throughput",
		Usage: "Number of concurrent table commit workers (0 = half of --concurrent)",
		Value: 0,
	},
	cli.IntFlag{
		Name:  "view-commits-throughput",
		Usage: "Number of concurrent view commit workers (0 = half of --concurrent)",
		Value: 0,
	},
	cli.IntFlag{
		Name:  "max-retries",
		Usage: "Maximum number of retries on 409/500 errors",
		Value: 10,
	},
	cli.DurationFlag{
		Name:  "retry-backoff",
		Usage: "Backoff duration between retries",
		Value: 100 * time.Millisecond,
	},
}

var catalogCommitsCombinedFlags = combineFlags(globalFlags, ioFlags, catalogCommitsFlags, benchFlags, analyzeFlags)

var catalogCommitsCmd = cli.Command{
	Name:   "catalog-commits",
	Usage:  "benchmark Iceberg REST catalog commit generation (updates table/view properties to create commits)",
	Action: mainCatalogCommits,
	Before: setGlobalsFromContext,
	Flags:  catalogCommitsCombinedFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]

DESCRIPTION:
  Benchmarks Iceberg REST catalog commit generation by updating table/view properties.

  Prepare phase:
  1. Creates N-ary tree of namespaces (--namespace-width, --namespace-depth)
  2. Creates tables in leaf namespaces (--tables-per-ns)
  3. Creates views in leaf namespaces (--views-per-ns)

  Benchmark phase:
  - Two separate worker pools:
    - Table workers: --table-commits-throughput (default: --concurrent/2)
    - View workers: --view-commits-throughput (default: --concurrent/2)
  - Each worker round-robins through tables/views
  - Updates properties with incrementing attribute to create commits
  - Retries on 409 Conflict or 500 errors

  Operations recorded:
  - TABLE_UPDATE: UpdateTable (sets new property)
  - VIEW_UPDATE: UpdateView (sets new property)

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  # Basic commit benchmark
  {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin

  # More table commits than view commits
  {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin \
    --table-commits-throughput 15 --view-commits-throughput 5

  # Tables only (no views)
  {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin \
    --views-per-ns 0
`,
}

func mainCatalogCommits(ctx *cli.Context) error {
	checkCatalogCommitsSyntax(ctx)

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

	err := iceberg.EnsureWarehouse(context.Background(), catalogCfg)
	fatalIf(probe.NewError(err), "Failed to ensure warehouse")

	cat, err := iceberg.NewCatalog(context.Background(), catalogCfg)
	fatalIf(probe.NewError(err), "Failed to create catalog")

	treeCfg := iceberg.TreeConfig{
		NamespaceWidth: ctx.Int("namespace-width"),
		NamespaceDepth: ctx.Int("namespace-depth"),
		TablesPerNS:    ctx.Int("tables-per-ns"),
		ViewsPerNS:     ctx.Int("views-per-ns"),
		BaseLocation:   ctx.String("base-location"),
		CatalogName:    ctx.String("catalog-name"),
	}

	b := bench.IcebergCommits{
		Common:                 getIcebergCommon(ctx),
		Catalog:                cat,
		TreeConfig:             treeCfg,
		CatalogURI:             catalogURLs[0],
		AccessKey:              ctx.String("access-key"),
		SecretKey:              ctx.String("secret-key"),
		TableCommitsThroughput: ctx.Int("table-commits-throughput"),
		ViewCommitsThroughput:  ctx.Int("view-commits-throughput"),
		MaxRetries:             ctx.Int("max-retries"),
		RetryBackoff:           ctx.Duration("retry-backoff"),
	}

	return runBench(ctx, &b)
}

func checkCatalogCommitsSyntax(ctx *cli.Context) {
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
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
