package bench

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/minio/warp/pkg/iceberg"
	warprest "github.com/minio/warp/pkg/iceberg/rest"
)

// Ensure warprest import is used
var _ = warprest.BuildIcebergSchema

// Iceberg REST catalog operation types.
const (
	OpNSCreate    = "NS_CREATE"
	OpNSGet       = "NS_GET"
	OpNSHead      = "NS_HEAD"
	OpNSList      = "NS_LIST"
	OpNSUpdate    = "NS_UPDATE"
	OpTableCreate = "TABLE_CREATE"
	OpTableGet    = "TABLE_GET"
	OpTableHead   = "TABLE_HEAD"
	OpTableList   = "TABLE_LIST"
	OpTableUpdate = "TABLE_UPDATE"
	OpViewCreate  = "VIEW_CREATE"
	OpViewGet     = "VIEW_GET"
	OpViewHead    = "VIEW_HEAD"
	OpViewList    = "VIEW_LIST"
	OpViewUpdate  = "VIEW_UPDATE"
)

type IcebergRead struct {
	Common
	Catalog    *rest.Catalog
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
	catalogName := b.TreeConfig.CatalogName
	cfg := b.Tree.Config()

	for i, ns := range b.namespaces {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		props := warprest.BuildTableProperties(cfg.PropertiesPerNS, "ns_prop")

		op := Operation{
			OpType:   OpNSCreate,
			Thread:   0,
			Size:     0,
			File:     fmt.Sprintf("%s/%v", catalogName, ns.Path),
			ObjPerOp: 0,
			Endpoint: catalogName,
		}

		op.Start = time.Now()
		err := b.Catalog.CreateNamespace(ctx, ns.Path, props)
		op.End = time.Now()

		if err != nil && !isAlreadyExists(err) {
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
	catalogName := b.TreeConfig.CatalogName
	cfg := b.Tree.Config()

	schema := warprest.BuildIcebergSchema(cfg.ColumnsPerTable)
	props := warprest.BuildTableProperties(cfg.PropertiesPerTbl, "tbl_prop")

	for i, tbl := range b.tables {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		ident := toTableIdentifier(tbl.Namespace, tbl.Name)

		op := Operation{
			OpType:   OpTableCreate,
			Thread:   0,
			Size:     0,
			File:     fmt.Sprintf("%s/%v/%s", catalogName, tbl.Namespace, tbl.Name),
			ObjPerOp: 0,
			Endpoint: catalogName,
		}

		op.Start = time.Now()
		_, err := b.Catalog.CreateTable(ctx, ident, schema,
			catalog.WithLocation(tbl.Location),
			catalog.WithProperties(props),
		)
		op.End = time.Now()

		if err != nil && !isAlreadyExists(err) {
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
	catalogName := b.TreeConfig.CatalogName
	cfg := b.Tree.Config()

	schema := warprest.BuildIcebergSchema(cfg.ColumnsPerView)
	props := warprest.BuildTableProperties(cfg.PropertiesPerVw, "view_prop")

	for i, vw := range b.views {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		ident := toTableIdentifier(vw.Namespace, vw.Name)
		version := warprest.BuildIcebergViewVersion(vw.Namespace, vw.Name)

		op := Operation{
			OpType:   OpViewCreate,
			Thread:   0,
			Size:     0,
			File:     fmt.Sprintf("%s/%v/%s", catalogName, vw.Namespace, vw.Name),
			ObjPerOp: 0,
			Endpoint: catalogName,
		}

		op.Start = time.Now()
		_, err := b.Catalog.CreateView(ctx, ident, version, schema,
			catalog.WithViewLocation(vw.Location),
			catalog.WithViewProperties(props),
		)
		op.End = time.Now()

		if err != nil && !isAlreadyExists(err) {
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
		ctx = c.AutoTerm(ctx, OpNSGet, b.AutoTermScale, autoTermCheck, autoTermSamples, b.AutoTermDur)
	}

	for i := 0; i < b.Concurrency; i++ {
		go func(thread int) {
			defer wg.Done()
			rcv := c.Receiver()
			done := ctx.Done()
			catalogName := b.TreeConfig.CatalogName
			opCtx := context.Background()

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

				b.readNamespace(opCtx, rcv, thread, catalogName, b.namespaces[nsIdx%len(b.namespaces)])
				nsIdx++

				if len(b.tables) > 0 {
					b.readTable(opCtx, rcv, thread, catalogName, b.tables[tblIdx%len(b.tables)])
					tblIdx++
				}

				if len(b.views) > 0 {
					b.readView(opCtx, rcv, thread, catalogName, b.views[vwIdx%len(b.views)])
					vwIdx++
				}
			}
		}(i)
	}

	wg.Wait()
	return nil
}

func (b *IcebergRead) readNamespace(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, ns iceberg.NamespaceInfo) {
	ident := ns.Path

	op := Operation{
		OpType:   OpNSGet,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalogName, ns.Path),
		Endpoint: catalogName,
	}

	op.Start = time.Now()
	_, err := b.Catalog.LoadNamespaceProperties(ctx, ident)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op

	op = Operation{
		OpType:   OpNSHead,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalogName, ns.Path),
		Endpoint: catalogName,
	}

	op.Start = time.Now()
	_, err = b.Catalog.CheckNamespaceExists(ctx, ident)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op

	if !ns.IsLeaf {
		op = Operation{
			OpType:   OpNSList,
			Thread:   uint32(thread),
			File:     fmt.Sprintf("%s/%v", catalogName, ns.Path),
			Endpoint: catalogName,
		}

		op.Start = time.Now()
		_, err = b.Catalog.ListNamespaces(ctx, ident)
		op.End = time.Now()

		if err != nil {
			op.Err = err.Error()
		}
		rcv <- op
	}
}

func (b *IcebergRead) readTable(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, tbl iceberg.TableInfo) {
	ident := toTableIdentifier(tbl.Namespace, tbl.Name)

	op := Operation{
		OpType:   OpTableGet,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalogName, tbl.Namespace, tbl.Name),
		Endpoint: catalogName,
	}

	op.Start = time.Now()
	_, err := b.Catalog.LoadTable(ctx, ident)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op

	op = Operation{
		OpType:   OpTableHead,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalogName, tbl.Namespace, tbl.Name),
		Endpoint: catalogName,
	}

	op.Start = time.Now()
	_, err = b.Catalog.CheckTableExists(ctx, ident)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op

	op = Operation{
		OpType:   OpTableList,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalogName, tbl.Namespace),
		Endpoint: catalogName,
	}

	op.Start = time.Now()
	for _, err := range b.Catalog.ListTables(ctx, tbl.Namespace) {
		if err != nil {
			op.Err = err.Error()
		}
	}
	op.End = time.Now()

	rcv <- op
}

func (b *IcebergRead) readView(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, vw iceberg.ViewInfo) {
	ident := toTableIdentifier(vw.Namespace, vw.Name)

	op := Operation{
		OpType:   OpViewGet,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalogName, vw.Namespace, vw.Name),
		Endpoint: catalogName,
	}

	op.Start = time.Now()
	_, err := b.Catalog.LoadView(ctx, ident)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op

	op = Operation{
		OpType:   OpViewHead,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalogName, vw.Namespace, vw.Name),
		Endpoint: catalogName,
	}

	op.Start = time.Now()
	_, err = b.Catalog.CheckViewExists(ctx, ident)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op

	op = Operation{
		OpType:   OpViewList,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalogName, vw.Namespace),
		Endpoint: catalogName,
	}

	op.Start = time.Now()
	for _, err := range b.Catalog.ListViews(ctx, vw.Namespace) {
		if err != nil {
			op.Err = err.Error()
		}
	}
	op.End = time.Now()

	rcv <- op
}

func (b *IcebergRead) Cleanup(ctx context.Context) {
	d := &iceberg.DatasetCreator{
		Catalog:    b.Catalog,
		Tree:       b.Tree,
		CatalogURI: b.CatalogURI,
		AccessKey:  b.AccessKey,
		SecretKey:  b.SecretKey,
	}
	d.DeleteAll(ctx)
}

func toTableIdentifier(namespace []string, name string) table.Identifier {
	return append(namespace, name)
}

func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, catalog.ErrNamespaceAlreadyExists) ||
		errors.Is(err, catalog.ErrTableAlreadyExists) ||
		errors.Is(err, catalog.ErrViewAlreadyExists) {
		return true
	}
	errStr := err.Error()
	return strings.Contains(errStr, "AlreadyExists") ||
		strings.Contains(errStr, "already exists") ||
		strings.Contains(errStr, "Conflict")
}
