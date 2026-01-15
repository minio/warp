package bench

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/minio/warp/pkg/iceberg"
	"github.com/minio/warp/pkg/iceberg/rest"
)

// Iceberg REST catalog operation types.
const (
	OpNamespaceCreate = "NAMESPACE_CREATE"
	OpNamespaceGet    = "NAMESPACE_GET"
	OpNamespaceHead   = "NAMESPACE_HEAD"
	OpNamespaceList   = "NAMESPACE_LIST"
	OpNamespaceUpdate = "NAMESPACE_UPDATE"
	OpTableCreate     = "TABLE_CREATE"
	OpTableGet        = "TABLE_GET"
	OpTableHead       = "TABLE_HEAD"
	OpTableList       = "TABLE_LIST"
	OpTableUpdate     = "TABLE_UPDATE"
	OpViewCreate      = "VIEW_CREATE"
	OpViewGet         = "VIEW_GET"
	OpViewHead        = "VIEW_HEAD"
	OpViewList        = "VIEW_LIST"
	OpViewUpdate      = "VIEW_UPDATE"
)

type IcebergRead struct {
	Common
	RestClient *rest.Client
	Tree       *iceberg.Tree
	TreeConfig iceberg.TreeConfig

	CatalogURI string
	AccessKey  string
	SecretKey  string

	namespaces []iceberg.NamespaceInfo
	tables     []iceberg.TableInfo
	views      []iceberg.ViewInfo
}

func (b *IcebergRead) Prepare(ctx context.Context) error {
	b.Tree = iceberg.NewTree(b.TreeConfig)

	b.UpdateStatus(fmt.Sprintf("Preparing dataset: %d namespaces, %d tables, %d views",
		b.Tree.TotalNamespaces(), b.Tree.TotalTables(), b.Tree.TotalViews()))

	b.namespaces = b.Tree.AllNamespaces()
	b.tables = b.Tree.AllTables()
	b.views = b.Tree.AllViews()

	totalOps := len(b.namespaces) + len(b.tables) + len(b.views)
	if totalOps == 0 {
		return fmt.Errorf("no operations to perform: check tree configuration")
	}

	// Ensure warehouse exists
	if b.CatalogURI != "" && b.AccessKey != "" {
		b.UpdateStatus("Ensuring warehouse exists...")
		err := iceberg.EnsureWarehouse(ctx, iceberg.CatalogConfig{
			CatalogURI: b.CatalogURI,
			Warehouse:  b.TreeConfig.CatalogName,
			AccessKey:  b.AccessKey,
			SecretKey:  b.SecretKey,
		})
		if err != nil {
			b.Error("Note: warehouse creation returned:", err)
		}
	}

	b.UpdateStatus("Creating namespaces...")
	if err := b.createNamespaces(ctx); err != nil {
		return fmt.Errorf("create namespaces: %w", err)
	}

	if len(b.tables) > 0 {
		b.UpdateStatus("Creating tables...")
		if err := b.createTables(ctx); err != nil {
			return fmt.Errorf("create tables: %w", err)
		}
	}

	if len(b.views) > 0 {
		b.UpdateStatus("Creating views...")
		if err := b.createViews(ctx); err != nil {
			return fmt.Errorf("create views: %w", err)
		}
	}

	b.UpdateStatus("Preparation complete")
	return nil
}

func (b *IcebergRead) createNamespaces(ctx context.Context) error {
	rcv := b.Collector.Receiver()
	catalog := b.TreeConfig.CatalogName
	cfg := b.Tree.Config()

	for i, ns := range b.namespaces {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		props := rest.BuildTableProperties(cfg.PropertiesPerNS, "ns_prop")

		op := Operation{
			OpType:   OpNamespaceCreate,
			Thread:   0,
			Size:     0,
			File:     fmt.Sprintf("%s/%v", catalog, ns.Path),
			ObjPerOp: 0,
			Endpoint: b.TreeConfig.CatalogName,
		}

		op.Start = time.Now()
		_, err := b.RestClient.CreateNamespace(ctx, catalog, ns.Path, props)
		op.End = time.Now()

		if err != nil && !rest.IsConflict(err) {
			op.Err = err.Error()
			b.Error("namespace create error:", err)
		}

		rcv <- op
		b.prepareProgress(float64(i+1) / float64(len(b.namespaces)))
	}

	return nil
}

func (b *IcebergRead) createTables(ctx context.Context) error {
	rcv := b.Collector.Receiver()
	catalog := b.TreeConfig.CatalogName
	cfg := b.Tree.Config()

	for i, tbl := range b.tables {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req := rest.CreateTableRequest{
			Name:       tbl.Name,
			Location:   tbl.Location,
			Schema:     rest.BuildTableSchema(cfg.ColumnsPerTable),
			Properties: rest.BuildTableProperties(cfg.PropertiesPerTbl, "tbl_prop"),
		}

		op := Operation{
			OpType:   OpTableCreate,
			Thread:   0,
			Size:     0,
			File:     fmt.Sprintf("%s/%v/%s", catalog, tbl.Namespace, tbl.Name),
			ObjPerOp: 0,
			Endpoint: b.TreeConfig.CatalogName,
		}

		op.Start = time.Now()
		_, err := b.RestClient.CreateTable(ctx, catalog, tbl.Namespace, req)
		op.End = time.Now()

		if err != nil && !rest.IsConflict(err) {
			op.Err = err.Error()
			b.Error("table create error:", err)
		}

		rcv <- op
		b.prepareProgress(float64(i+1) / float64(len(b.tables)))
	}

	return nil
}

func (b *IcebergRead) createViews(ctx context.Context) error {
	rcv := b.Collector.Receiver()
	catalog := b.TreeConfig.CatalogName
	cfg := b.Tree.Config()

	for i, vw := range b.views {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req := rest.CreateViewRequest{
			Name:        vw.Name,
			Location:    vw.Location,
			Schema:      rest.BuildViewSchema(cfg.ColumnsPerView),
			ViewVersion: rest.BuildViewVersion(catalog, vw.Namespace, vw.Name),
			Properties:  rest.BuildTableProperties(cfg.PropertiesPerVw, "view_prop"),
		}

		op := Operation{
			OpType:   OpViewCreate,
			Thread:   0,
			Size:     0,
			File:     fmt.Sprintf("%s/%v/%s", catalog, vw.Namespace, vw.Name),
			ObjPerOp: 0,
			Endpoint: b.TreeConfig.CatalogName,
		}

		op.Start = time.Now()
		_, err := b.RestClient.CreateView(ctx, catalog, vw.Namespace, req)
		op.End = time.Now()

		if err != nil && !rest.IsConflict(err) {
			op.Err = err.Error()
			b.Error("view create error:", err)
		}

		rcv <- op
		b.prepareProgress(float64(i+1) / float64(len(b.views)))
	}

	return nil
}

func (b *IcebergRead) Start(ctx context.Context, wait chan struct{}) error {
	var wg sync.WaitGroup
	wg.Add(b.Concurrency)
	c := b.Collector

	if b.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, OpNamespaceGet, b.AutoTermScale, autoTermCheck, autoTermSamples, b.AutoTermDur)
	}

	for i := 0; i < b.Concurrency; i++ {
		go func(thread int) {
			defer wg.Done()
			rcv := c.Receiver()
			done := ctx.Done()
			catalog := b.TreeConfig.CatalogName

			<-wait

			nsIdx := 0
			tblIdx := 0
			vwIdx := 0

			for {
				select {
				case <-done:
					return
				default:
				}

				if b.rpsLimit(ctx) != nil {
					return
				}

				b.readNamespace(ctx, rcv, thread, catalog, b.namespaces[nsIdx%len(b.namespaces)])
				nsIdx++

				if len(b.tables) > 0 {
					b.readTable(ctx, rcv, thread, catalog, b.tables[tblIdx%len(b.tables)])
					tblIdx++
				}

				if len(b.views) > 0 {
					b.readView(ctx, rcv, thread, catalog, b.views[vwIdx%len(b.views)])
					vwIdx++
				}
			}
		}(i)
	}

	wg.Wait()
	return nil
}

func (b *IcebergRead) readNamespace(ctx context.Context, rcv chan<- Operation, thread int, catalog string, ns iceberg.NamespaceInfo) {
	op := Operation{
		OpType:   OpNamespaceGet,
		Thread:   uint32(thread),
		Size:     0,
		File:     fmt.Sprintf("%s/%v", catalog, ns.Path),
		ObjPerOp: 0,
		Endpoint: catalog,
	}

	op.Start = time.Now()
	_, err := b.RestClient.GetNamespace(ctx, catalog, ns.Path)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op

	op = Operation{
		OpType:   OpNamespaceHead,
		Thread:   uint32(thread),
		Size:     0,
		File:     fmt.Sprintf("%s/%v", catalog, ns.Path),
		ObjPerOp: 0,
		Endpoint: catalog,
	}

	op.Start = time.Now()
	_, err = b.RestClient.NamespaceExists(ctx, catalog, ns.Path)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op

	if !ns.IsLeaf {
		op = Operation{
			OpType:   OpNamespaceList,
			Thread:   uint32(thread),
			Size:     0,
			File:     fmt.Sprintf("%s/%v", catalog, ns.Path),
			ObjPerOp: 0,
			Endpoint: catalog,
		}

		op.Start = time.Now()
		_, err = b.RestClient.ListNamespaces(ctx, catalog, ns.Path)
		op.End = time.Now()

		if err != nil {
			op.Err = err.Error()
		}
		rcv <- op
	}
}

func (b *IcebergRead) readTable(ctx context.Context, rcv chan<- Operation, thread int, catalog string, tbl iceberg.TableInfo) {
	op := Operation{
		OpType:   OpTableGet,
		Thread:   uint32(thread),
		Size:     0,
		File:     fmt.Sprintf("%s/%v/%s", catalog, tbl.Namespace, tbl.Name),
		ObjPerOp: 0,
		Endpoint: catalog,
	}

	op.Start = time.Now()
	_, err := b.RestClient.GetTable(ctx, catalog, tbl.Namespace, tbl.Name)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op

	op = Operation{
		OpType:   OpTableHead,
		Thread:   uint32(thread),
		Size:     0,
		File:     fmt.Sprintf("%s/%v/%s", catalog, tbl.Namespace, tbl.Name),
		ObjPerOp: 0,
		Endpoint: catalog,
	}

	op.Start = time.Now()
	_, err = b.RestClient.TableExists(ctx, catalog, tbl.Namespace, tbl.Name)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op

	op = Operation{
		OpType:   OpTableList,
		Thread:   uint32(thread),
		Size:     0,
		File:     fmt.Sprintf("%s/%v", catalog, tbl.Namespace),
		ObjPerOp: 0,
		Endpoint: catalog,
	}

	op.Start = time.Now()
	_, err = b.RestClient.ListTables(ctx, catalog, tbl.Namespace)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergRead) readView(ctx context.Context, rcv chan<- Operation, thread int, catalog string, vw iceberg.ViewInfo) {
	op := Operation{
		OpType:   OpViewGet,
		Thread:   uint32(thread),
		Size:     0,
		File:     fmt.Sprintf("%s/%v/%s", catalog, vw.Namespace, vw.Name),
		ObjPerOp: 0,
		Endpoint: catalog,
	}

	op.Start = time.Now()
	_, err := b.RestClient.GetView(ctx, catalog, vw.Namespace, vw.Name)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op

	op = Operation{
		OpType:   OpViewHead,
		Thread:   uint32(thread),
		Size:     0,
		File:     fmt.Sprintf("%s/%v/%s", catalog, vw.Namespace, vw.Name),
		ObjPerOp: 0,
		Endpoint: catalog,
	}

	op.Start = time.Now()
	_, err = b.RestClient.ViewExists(ctx, catalog, vw.Namespace, vw.Name)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op

	op = Operation{
		OpType:   OpViewList,
		Thread:   uint32(thread),
		Size:     0,
		File:     fmt.Sprintf("%s/%v", catalog, vw.Namespace),
		ObjPerOp: 0,
		Endpoint: catalog,
	}

	op.Start = time.Now()
	_, err = b.RestClient.ListViews(ctx, catalog, vw.Namespace)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergRead) Cleanup(_ context.Context) {
	b.UpdateStatus("Cleanup: skipping (use --noclear=false to delete)")
}
