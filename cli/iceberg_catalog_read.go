package cli

import (
	"github.com/minio/cli"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/pkg/iceberg"
	"github.com/minio/warp/pkg/iceberg/rest"
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

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  1. Run Iceberg read benchmark against MinIO S3 Tables:
     {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin

  2. Create a larger dataset with more tables:
     {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin \
       --namespace-width 3 --namespace-depth 4 --tables-per-ns 10

  3. Run with multiple hosts (round-robin):
     {{.HelpName}} --host localhost:9001,localhost:9002 --access-key minioadmin --secret-key minioadmin
`,
}

func mainCatalogRead(ctx *cli.Context) error {
	checkCatalogReadSyntax(ctx)

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

	b := bench.IcebergRead{
		Common:     getIcebergCommon(ctx),
		RestClient: restClient,
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
