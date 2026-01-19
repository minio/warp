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

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/pkg/iceberg"
)

var tablesCatalogReadFlags = []cli.Flag{
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
}

var tablesCatalogReadCombinedFlags = combineFlags(globalFlags, ioFlags, tablesCatalogReadFlags, benchFlags, analyzeFlags)

var tablesCatalogReadCmd = cli.Command{
	Name:   "catalog-read",
	Usage:  "benchmark Iceberg REST catalog read operations (creates dataset in prepare, then benchmarks reads)",
	Action: mainTablesCatalogRead,
	Before: setGlobalsFromContext,
	Flags:  tablesCatalogReadCombinedFlags,
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
  - All workers share a pre-shuffled pool of 1000 operations
  - Each worker picks next operation from pool, executes it, repeats

  Operation distribution:
  - Weights are proportional, not percentages
  - Example: 10,10,5 is same ratio as 2,2,1 or 100,100,50
  - Pool is shuffled for random distribution

  Operations (default weights):
  - NS_LIST (10), NS_HEAD (10), NS_GET (10)
  - TABLE_LIST (10), TABLE_HEAD (10), TABLE_GET (10)
  - VIEW_LIST (10), VIEW_HEAD (10), VIEW_GET (10)

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  # Basic read benchmark
  {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin

  # Larger dataset
  {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin \
    --namespace-width 3 --namespace-depth 4 --tables-per-ns 10

  # Heavy table reads
  {{.HelpName}} --host localhost:9001 --access-key minioadmin --secret-key minioadmin \
    --table-get-distrib 50 --table-list-distrib 20

  # Multiple hosts
  {{.HelpName}} --host localhost:9001,localhost:9002 --access-key minioadmin --secret-key minioadmin
`,
}

func mainTablesCatalogRead(ctx *cli.Context) error {
	checkTablesCatalogReadSyntax(ctx)

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
		err := iceberg.EnsureWarehouse(context.Background(), catalogCfg)
		fatalIf(probe.NewError(err), "Failed to ensure warehouse")
	}

	cat, err := iceberg.NewCatalog(context.Background(), catalogCfg)
	fatalIf(probe.NewError(err), "Failed to create catalog")

	// Create catalog pool for round-robin access across all hosts
	catalogPool, err := iceberg.NewCatalogPool(context.Background(), catalogURLs, catalogCfg)
	fatalIf(probe.NewError(err), "Failed to create catalog pool")

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
			bench.OpNSList:    ctx.Float64("ns-list-distrib"),
			bench.OpNSHead:    ctx.Float64("ns-head-distrib"),
			bench.OpNSGet:     ctx.Float64("ns-get-distrib"),
			bench.OpTableList: ctx.Float64("table-list-distrib"),
			bench.OpTableHead: ctx.Float64("table-head-distrib"),
			bench.OpTableGet:  ctx.Float64("table-get-distrib"),
			bench.OpViewList:  ctx.Float64("view-list-distrib"),
			bench.OpViewHead:  ctx.Float64("view-head-distrib"),
			bench.OpViewGet:   ctx.Float64("view-get-distrib"),
		},
	}
	err = dist.Generate()
	fatalIf(probe.NewError(err), "Invalid distribution")

	b := bench.IcebergRead{
		Common:          getTablesCommon(ctx),
		Catalog:         cat,
		CatalogPool:     catalogPool,
		TreeConfig:      treeCfg,
		CatalogURI:      catalogURLs[0],
		AccessKey:       ctx.String("access-key"),
		SecretKey:       ctx.String("secret-key"),
		ExternalCatalog: externalCatalog,
		Dist:            &dist,
	}

	return runBench(ctx, &b)
}

func checkTablesCatalogReadSyntax(ctx *cli.Context) {
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

func getTablesCommon(ctx *cli.Context) bench.Common {
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
