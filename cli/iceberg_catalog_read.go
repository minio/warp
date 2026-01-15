package cli

import (
	"context"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/pkg/iceberg"
)

var catalogReadFlags = []cli.Flag{
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
	cli.IntFlag{
		Name:  "columns",
		Usage: "Number of columns per table/view schema",
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
}

var catalogReadCombinedFlags = combineFlags(globalFlags, ioFlags, catalogReadFlags, benchFlags, analyzeFlags)

var catalogReadCmd = cli.Command{
	Name:   "catalog-read",
	Usage:  "benchmark Iceberg REST catalog read operations (creates dataset in prepare, then benchmarks reads)",
	Action: mainCatalogRead,
	Before: setGlobalsFromContext,
	Flags:  catalogReadCombinedFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]

DESCRIPTION:
  Benchmarks Iceberg REST catalog read operations.

  Prepare phase:
  1. Creates N-ary tree of namespaces (--namespace-width, --namespace-depth)
  2. Creates tables in leaf namespaces (--tables-per-ns)
  3. Creates views in leaf namespaces (--views-per-ns)

  Benchmark phase:
  - Spawns --concurrent workers (default 20)
  - Each worker loops through all namespaces/tables/views
  - Performs read operations: GET, HEAD, LIST

  Operations recorded:
  - NS_GET: LoadNamespaceProperties
  - NS_HEAD: CheckNamespaceExists
  - NS_LIST: ListNamespaces (on non-leaf namespaces)
  - TABLE_GET: LoadTable
  - TABLE_HEAD: CheckTableExists
  - TABLE_LIST: ListTables
  - VIEW_GET: LoadView
  - VIEW_HEAD: CheckViewExists
  - VIEW_LIST: ListViews

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  # Basic read benchmark
  {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin

  # Larger dataset
  {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin \
    --namespace-width 3 --namespace-depth 4 --tables-per-ns 10

  # Multiple hosts
  {{.HelpName}} --host localhost:9001,localhost:9002 --access-key minioadmin --secret-key minioadmin
`,
}

func mainCatalogRead(ctx *cli.Context) error {
	checkCatalogReadSyntax(ctx)

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
		NamespaceWidth:   ctx.Int("namespace-width"),
		NamespaceDepth:   ctx.Int("namespace-depth"),
		TablesPerNS:      ctx.Int("tables-per-ns"),
		ViewsPerNS:       ctx.Int("views-per-ns"),
		ColumnsPerTable:  ctx.Int("columns"),
		ColumnsPerView:   ctx.Int("columns"),
		PropertiesPerNS:  ctx.Int("properties"),
		PropertiesPerTbl: ctx.Int("properties"),
		PropertiesPerVw:  ctx.Int("properties"),
		BaseLocation:     ctx.String("base-location"),
		CatalogName:      ctx.String("catalog-name"),
	}

	b := bench.IcebergRead{
		Common:     getIcebergCommon(ctx),
		Catalog:    cat,
		TreeConfig: treeCfg,
		CatalogURI: catalogURLs[0],
		AccessKey:  ctx.String("access-key"),
		SecretKey:  ctx.String("secret-key"),
	}

	return runBench(ctx, &b)
}

func checkCatalogReadSyntax(ctx *cli.Context) {
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
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}

func getIcebergCommon(ctx *cli.Context) bench.Common {
	statusln := func(s string) {
		console.Eraseline()
		console.Print(s)
	}
	if globalQuiet {
		statusln = func(_ string) {}
	}

	return bench.Common{
		Concurrency:  ctx.Int("concurrent"),
		UpdateStatus: statusln,
		TotalClients: 1,
		Error: func(data ...any) {
			console.Errorln(data...)
		},
	}
}
