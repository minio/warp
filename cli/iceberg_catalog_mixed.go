package cli

import (
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/pkg/iceberg"
	"github.com/minio/warp/pkg/iceberg/rest"
)

var catalogMixedFlags = []cli.Flag{
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
	cli.Float64Flag{
		Name:  "ns-list-distrib",
		Usage: "Weight of namespace list operations",
		Value: 10,
	},
	cli.Float64Flag{
		Name:  "ns-head-distrib",
		Usage: "Weight of namespace exists operations",
		Value: 10,
	},
	cli.Float64Flag{
		Name:  "ns-get-distrib",
		Usage: "Weight of namespace get operations",
		Value: 10,
	},
	cli.Float64Flag{
		Name:  "ns-update-distrib",
		Usage: "Weight of namespace update operations",
		Value: 5,
	},
	cli.Float64Flag{
		Name:  "table-list-distrib",
		Usage: "Weight of table list operations",
		Value: 10,
	},
	cli.Float64Flag{
		Name:  "table-head-distrib",
		Usage: "Weight of table exists operations",
		Value: 10,
	},
	cli.Float64Flag{
		Name:  "table-get-distrib",
		Usage: "Weight of table get operations",
		Value: 10,
	},
	cli.Float64Flag{
		Name:  "table-update-distrib",
		Usage: "Weight of table update operations",
		Value: 5,
	},
	cli.Float64Flag{
		Name:  "view-list-distrib",
		Usage: "Weight of view list operations",
		Value: 10,
	},
	cli.Float64Flag{
		Name:  "view-head-distrib",
		Usage: "Weight of view exists operations",
		Value: 10,
	},
	cli.Float64Flag{
		Name:  "view-get-distrib",
		Usage: "Weight of view get operations",
		Value: 10,
	},
	cli.Float64Flag{
		Name:  "view-update-distrib",
		Usage: "Weight of view update operations",
		Value: 5,
	},
}

var catalogMixedCombinedFlags = combineFlags(globalFlags, ioFlags, catalogMixedFlags, benchFlags, analyzeFlags)

var catalogMixedCmd = cli.Command{
	Name:   "catalog-mixed",
	Usage:  "benchmark mixed read/update workload on existing Iceberg REST catalog dataset",
	Action: mainCatalogMixed,
	Before: setGlobalsFromContext,
	Flags:  catalogMixedCombinedFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  1. Run mixed benchmark with default distribution:
     {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin

  2. Run read-heavy workload (disable updates):
     {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin \
       --ns-update-distrib 0 --table-update-distrib 0 --view-update-distrib 0

  3. Run with multiple hosts (round-robin):
     {{.HelpName}} --host localhost:9001,localhost:9002 --access-key minioadmin --secret-key minioadmin
`,
}

func mainCatalogMixed(ctx *cli.Context) error {
	checkCatalogMixedSyntax(ctx)

	hosts := parseHosts(ctx.String("host"), ctx.Bool("resolve-host"))
	useTLS := ctx.Bool("tls") || ctx.Bool("ktls")
	catalogURLs := buildCatalogURLs(hosts, useTLS)

	restClient := rest.NewClient(rest.ClientConfig{
		BaseURLs:  catalogURLs,
		APIPrefix: "/v1",
		AccessKey: ctx.String("access-key"),
		SecretKey: ctx.String("secret-key"),
		Region:    ctx.String("region"),
	})

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

	dist := bench.IcebergMixedDistribution{
		Distribution: map[string]float64{
			bench.OpNSList:      ctx.Float64("ns-list-distrib"),
			bench.OpNSHead:      ctx.Float64("ns-head-distrib"),
			bench.OpNSGet:       ctx.Float64("ns-get-distrib"),
			bench.OpNSUpdate:    ctx.Float64("ns-update-distrib"),
			bench.OpTableList:   ctx.Float64("table-list-distrib"),
			bench.OpTableHead:   ctx.Float64("table-head-distrib"),
			bench.OpTableGet:    ctx.Float64("table-get-distrib"),
			bench.OpTableUpdate: ctx.Float64("table-update-distrib"),
			bench.OpViewList:    ctx.Float64("view-list-distrib"),
			bench.OpViewHead:    ctx.Float64("view-head-distrib"),
			bench.OpViewGet:     ctx.Float64("view-get-distrib"),
			bench.OpViewUpdate:  ctx.Float64("view-update-distrib"),
		},
	}
	err := dist.Generate()
	fatalIf(probe.NewError(err), "Invalid distribution")

	b := bench.IcebergMixed{
		Common:     getIcebergCommon(ctx),
		RestClient: restClient,
		TreeConfig: treeCfg,
		CatalogURI: catalogURLs[0],
		AccessKey:  ctx.String("access-key"),
		SecretKey:  ctx.String("secret-key"),
		Dist:       &dist,
	}

	return runBench(ctx, &b)
}

func checkCatalogMixedSyntax(ctx *cli.Context) {
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
