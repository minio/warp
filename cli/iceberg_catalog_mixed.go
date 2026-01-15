package cli

import (
	"github.com/minio/cli"
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
		Value: 0,
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
		Name:  "read-ratio",
		Usage: "Ratio of read operations (0.0-1.0). Rest are updates.",
		Value: 0.5,
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
  1. Run mixed read/update benchmark with 50% reads:
     {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin

  2. Run read-heavy workload (80% reads):
     {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin --read-ratio 0.8

  3. Run write-heavy workload (20% reads):
     {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin --read-ratio 0.2

  4. Run with multiple hosts (round-robin):
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

	b := bench.IcebergMixed{
		Common:     getIcebergCommon(ctx),
		RestClient: restClient,
		TreeConfig: treeCfg,
		CatalogURI: catalogURLs[0],
		AccessKey:  ctx.String("access-key"),
		SecretKey:  ctx.String("secret-key"),
		ReadRatio:  ctx.Float64("read-ratio"),
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
	readRatio := ctx.Float64("read-ratio")
	if readRatio < 0.0 || readRatio > 1.0 {
		console.Fatal("--read-ratio must be between 0.0 and 1.0")
	}
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
