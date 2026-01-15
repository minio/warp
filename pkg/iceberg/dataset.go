package iceberg

import (
	"context"
	"fmt"

	"github.com/minio/warp/pkg/iceberg/rest"
)

type DatasetCreator struct {
	RestClient *rest.Client
	Tree       *Tree
	CatalogURI string
	AccessKey  string
	SecretKey  string
	OnProgress func(float64)
	OnError    func(data ...any)
}

func (d *DatasetCreator) CreateNamespaces(ctx context.Context) error {
	namespaces := d.Tree.AllNamespaces()
	catalog := d.Tree.Config().CatalogName
	cfg := d.Tree.Config()

	for i, ns := range namespaces {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		props := rest.BuildTableProperties(cfg.PropertiesPerNS, "ns_prop")
		_, err := d.RestClient.CreateNamespace(ctx, catalog, ns.Path, props)
		if err != nil && !rest.IsConflict(err) {
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

	catalog := d.Tree.Config().CatalogName
	cfg := d.Tree.Config()

	for i, tbl := range tables {
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
		_, err := d.RestClient.CreateTable(ctx, catalog, tbl.Namespace, req)
		if err != nil && !rest.IsConflict(err) {
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

	catalog := d.Tree.Config().CatalogName
	cfg := d.Tree.Config()

	for i, vw := range views {
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
		_, err := d.RestClient.CreateView(ctx, catalog, vw.Namespace, req)
		if err != nil && !rest.IsConflict(err) {
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
	catalog := d.Tree.Config().CatalogName

	// Delete views first
	for _, vw := range d.Tree.AllViews() {
		_ = d.RestClient.DropView(ctx, catalog, vw.Namespace, vw.Name)
	}

	// Delete tables
	for _, tbl := range d.Tree.AllTables() {
		_ = d.RestClient.DropTable(ctx, catalog, tbl.Namespace, tbl.Name, true)
	}

	// Delete namespaces (reverse order - leaf to root)
	namespaces := d.Tree.AllNamespaces()
	for i := len(namespaces) - 1; i >= 0; i-- {
		_ = d.RestClient.DropNamespace(ctx, catalog, namespaces[i].Path)
	}

	// Delete warehouse
	if d.CatalogURI != "" && d.AccessKey != "" {
		_ = DeleteWarehouse(ctx, CatalogConfig{
			CatalogURI: d.CatalogURI,
			Warehouse:  catalog,
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

	// Ensure warehouse exists
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
