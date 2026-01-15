package iceberg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	restcat "github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/minio/warp/pkg/iceberg/rest"
)

type DatasetCreator struct {
	Catalog    *restcat.Catalog
	Tree       *Tree
	CatalogURI string
	AccessKey  string
	SecretKey  string
	OnProgress func(float64)
	OnError    func(data ...any)
}

func (d *DatasetCreator) CreateNamespaces(ctx context.Context) error {
	namespaces := d.Tree.AllNamespaces()
	cfg := d.Tree.Config()

	for i, ns := range namespaces {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		props := rest.BuildTableProperties(cfg.PropertiesPerNS, "ns_prop")
		err := d.Catalog.CreateNamespace(ctx, ns.Path, props)
		if err != nil && !IsAlreadyExists(err) {
			if d.OnError != nil {
				d.OnError("namespace create error:", err)
			}
		}
		if d.OnProgress != nil {
			d.OnProgress(float64(i+1) / float64(len(namespaces)))
		}
	}
	return nil
}

func (d *DatasetCreator) CreateTables(ctx context.Context) error {
	tables := d.Tree.AllTables()
	if len(tables) == 0 {
		return nil
	}

	cfg := d.Tree.Config()
	schema := rest.BuildIcebergSchema(cfg.ColumnsPerTable)
	props := rest.BuildTableProperties(cfg.PropertiesPerTbl, "tbl_prop")

	for i, tbl := range tables {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		ident := toTableIdentifier(tbl.Namespace, tbl.Name)
		_, err := d.Catalog.CreateTable(ctx, ident, schema,
			catalog.WithLocation(tbl.Location),
			catalog.WithProperties(props),
		)
		if err != nil && !IsAlreadyExists(err) {
			if d.OnError != nil {
				d.OnError("table create error:", err)
			}
		}
		if d.OnProgress != nil {
			d.OnProgress(float64(i+1) / float64(len(tables)))
		}
	}
	return nil
}

func (d *DatasetCreator) CreateViews(ctx context.Context) error {
	views := d.Tree.AllViews()
	if len(views) == 0 {
		return nil
	}

	cfg := d.Tree.Config()
	schema := rest.BuildIcebergSchema(cfg.ColumnsPerView)
	props := rest.BuildTableProperties(cfg.PropertiesPerVw, "view_prop")

	for i, vw := range views {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		ident := toTableIdentifier(vw.Namespace, vw.Name)
		version := rest.BuildIcebergViewVersion(vw.Namespace, vw.Name)
		_, err := d.Catalog.CreateView(ctx, ident, version, schema,
			catalog.WithViewLocation(vw.Location),
			catalog.WithViewProperties(props),
		)
		if err != nil && !IsAlreadyExists(err) {
			if d.OnError != nil {
				d.OnError("view create error:", err)
			}
		}
		if d.OnProgress != nil {
			d.OnProgress(float64(i+1) / float64(len(views)))
		}
	}
	return nil
}

func (d *DatasetCreator) DeleteAll(ctx context.Context) {
	catalogName := d.Tree.Config().CatalogName

	for _, vw := range d.Tree.AllViews() {
		ident := toTableIdentifier(vw.Namespace, vw.Name)
		_ = d.Catalog.DropView(ctx, ident)
	}

	for _, tbl := range d.Tree.AllTables() {
		ident := toTableIdentifier(tbl.Namespace, tbl.Name)
		_ = d.Catalog.DropTable(ctx, ident)
	}

	namespaces := d.Tree.AllNamespaces()
	for i := len(namespaces) - 1; i >= 0; i-- {
		_ = d.Catalog.DropNamespace(ctx, namespaces[i].Path)
	}

	if d.CatalogURI != "" && d.AccessKey != "" {
		_ = DeleteWarehouse(ctx, CatalogConfig{
			CatalogURI: d.CatalogURI,
			Warehouse:  catalogName,
			AccessKey:  d.AccessKey,
			SecretKey:  d.SecretKey,
		})
	}
}

func (d *DatasetCreator) CreateAll(ctx context.Context, updateStatus func(string)) error {
	if updateStatus != nil {
		updateStatus(fmt.Sprintf("Preparing dataset: %d namespaces, %d tables, %d views",
			d.Tree.TotalNamespaces(), d.Tree.TotalTables(), d.Tree.TotalViews()))
	}

	if d.CatalogURI != "" && d.AccessKey != "" {
		if updateStatus != nil {
			updateStatus("Ensuring warehouse exists...")
		}
		err := EnsureWarehouse(ctx, CatalogConfig{
			CatalogURI: d.CatalogURI,
			Warehouse:  d.Tree.Config().CatalogName,
			AccessKey:  d.AccessKey,
			SecretKey:  d.SecretKey,
		})
		if err != nil && d.OnError != nil {
			d.OnError("Note: warehouse creation returned:", err)
		}
	}

	if updateStatus != nil {
		updateStatus("Creating namespaces...")
	}
	if err := d.CreateNamespaces(ctx); err != nil {
		return fmt.Errorf("create namespaces: %w", err)
	}

	if d.Tree.TotalTables() > 0 {
		if updateStatus != nil {
			updateStatus("Creating tables...")
		}
		if err := d.CreateTables(ctx); err != nil {
			return fmt.Errorf("create tables: %w", err)
		}
	}

	if d.Tree.TotalViews() > 0 {
		if updateStatus != nil {
			updateStatus("Creating views...")
		}
		if err := d.CreateViews(ctx); err != nil {
			return fmt.Errorf("create views: %w", err)
		}
	}

	if updateStatus != nil {
		updateStatus("Preparation complete")
	}
	return nil
}

func toTableIdentifier(namespace []string, name string) table.Identifier {
	return append(namespace, name)
}

func IsAlreadyExists(err error) bool {
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
