package cli

import (
	"github.com/minio/cli"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/pkg/iceberg"
	"github.com/minio/warp/pkg/iceberg/rest"
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
		Value: 0,
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

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  1. Generate commits on existing dataset:
     {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin

  2. Generate commits with specific throughput:
     {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin \
       --table-commits-throughput 10 --view-commits-throughput 5

  3. Run with multiple hosts (round-robin):
     {{.HelpName}} --host localhost:9001,localhost:9002 --access-key minioadmin --secret-key minioadmin
`,
}

func mainCatalogCommits(ctx *cli.Context) error {
	checkCatalogCommitsSyntax(ctx)

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
		NamespaceWidth: ctx.Int("namespace-width"),
		NamespaceDepth: ctx.Int("namespace-depth"),
		TablesPerNS:    ctx.Int("tables-per-ns"),
		ViewsPerNS:     ctx.Int("views-per-ns"),
		BaseLocation:   ctx.String("base-location"),
		CatalogName:    ctx.String("catalog-name"),
	}

	b := bench.IcebergCommits{
		Common:                 getIcebergCommon(ctx),
		RestClient:             restClient,
		TreeConfig:             treeCfg,
		CatalogURI:             catalogURLs[0],
		AccessKey:              ctx.String("access-key"),
		SecretKey:              ctx.String("secret-key"),
		TableCommitsThroughput: ctx.Int("table-commits-throughput"),
		ViewCommitsThroughput:  ctx.Int("view-commits-throughput"),
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
