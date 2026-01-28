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

package iceberg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/iceberg-go/catalog"
	restcat "github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/minio/warp/pkg/iceberg/rest"
)

type DatasetCreator struct {
	Catalog         *restcat.Catalog
	CatalogPool     *CatalogPool
	Tree            *Tree
	CatalogURI      string
	AccessKey       string
	SecretKey       string
	Concurrency     int
	ExternalCatalog ExternalCatalogType
	OnProgress      func(float64)
	OnError         func(data ...any)
}

func (d *DatasetCreator) getCatalog() *restcat.Catalog {
	if d.CatalogPool != nil {
		return d.CatalogPool.Get()
	}
	return d.Catalog
}

func (d *DatasetCreator) CreateNamespaces(ctx context.Context) error {
	namespaces := d.Tree.AllNamespaces()
	cfg := d.Tree.Config()

	var firstErr error
	for i, ns := range namespaces {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		props := rest.BuildTableProperties(cfg.PropertiesPerNS, "ns_prop")
		err := d.getCatalog().CreateNamespace(ctx, ns.Path, props)
		if err != nil && !IsAlreadyExists(err) {
			if d.OnError != nil {
				d.OnError("namespace create error:", err)
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("namespace %v: %w", ns.Path, err)
			}
		}
		if d.OnProgress != nil {
			d.OnProgress(float64(i+1) / float64(len(namespaces)))
		}
	}
	return firstErr
}

func (d *DatasetCreator) CreateTables(ctx context.Context) error {
	tables := d.Tree.AllTables()
	if len(tables) == 0 {
		return nil
	}

	cfg := d.Tree.Config()
	schema := rest.BuildIcebergSchema(cfg.ColumnsPerTable)
	props := rest.BuildTableProperties(cfg.PropertiesPerTbl, "tbl_prop")

	concurrency := d.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > 20 {
		concurrency = 20
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	var completed uint64
	var firstErr error
	var errMu sync.Mutex

	for _, tbl := range tables {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(tbl TableInfo) {
			defer wg.Done()
			defer func() { <-sem }()

			ident := toTableIdentifier(tbl.Namespace, tbl.Name)
			_, err := d.getCatalog().CreateTable(ctx, ident, schema,
				catalog.WithLocation(tbl.Location),
				catalog.WithProperties(props),
			)
			if err != nil && !IsAlreadyExists(err) {
				if d.OnError != nil {
					d.OnError("table create error:", err)
				}
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("table %s: %w", tbl.Name, err)
				}
				errMu.Unlock()
			}
			if d.OnProgress != nil {
				done := atomic.AddUint64(&completed, 1)
				d.OnProgress(float64(done) / float64(len(tables)))
			}
		}(tbl)
	}

	wg.Wait()
	return firstErr
}

func (d *DatasetCreator) CreateViews(ctx context.Context) error {
	views := d.Tree.AllViews()
	if len(views) == 0 {
		return nil
	}

	cfg := d.Tree.Config()
	schema := rest.BuildIcebergSchema(cfg.ColumnsPerView)
	props := rest.BuildTableProperties(cfg.PropertiesPerVw, "view_prop")

	concurrency := d.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > 20 {
		concurrency = 20
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	var completed uint64
	var firstErr error
	var errMu sync.Mutex

	for _, vw := range views {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(vw ViewInfo) {
			defer wg.Done()
			defer func() { <-sem }()

			ident := toTableIdentifier(vw.Namespace, vw.Name)
			version := rest.BuildIcebergViewVersion(vw.Namespace, vw.Name)
			_, err := d.getCatalog().CreateView(ctx, ident, version, schema,
				catalog.WithViewLocation(vw.Location),
				catalog.WithViewProperties(props),
			)
			if err != nil && !IsAlreadyExists(err) {
				if d.OnError != nil {
					d.OnError("view create error:", err)
				}
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("view %s: %w", vw.Name, err)
				}
				errMu.Unlock()
			}
			if d.OnProgress != nil {
				done := atomic.AddUint64(&completed, 1)
				d.OnProgress(float64(done) / float64(len(views)))
			}
		}(vw)
	}

	wg.Wait()
	return firstErr
}

func (d *DatasetCreator) DeleteAll(ctx context.Context) {
	catalogName := d.Tree.Config().CatalogName

	concurrency := d.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > 20 {
		concurrency = 20
	}

	// Delete views concurrently
	views := d.Tree.AllViews()
	if len(views) > 0 {
		var wg sync.WaitGroup
		sem := make(chan struct{}, concurrency)
		for _, vw := range views {
			wg.Add(1)
			sem <- struct{}{}
			go func(vw ViewInfo) {
				defer wg.Done()
				defer func() { <-sem }()
				ident := toTableIdentifier(vw.Namespace, vw.Name)
				_ = d.getCatalog().DropView(ctx, ident)
			}(vw)
		}
		wg.Wait()
	}

	// Delete tables concurrently
	tables := d.Tree.AllTables()
	if len(tables) > 0 {
		var wg sync.WaitGroup
		sem := make(chan struct{}, concurrency)
		for _, tbl := range tables {
			wg.Add(1)
			sem <- struct{}{}
			go func(tbl TableInfo) {
				defer wg.Done()
				defer func() { <-sem }()
				ident := toTableIdentifier(tbl.Namespace, tbl.Name)
				_ = d.getCatalog().DropTable(ctx, ident)
			}(tbl)
		}
		wg.Wait()
	}

	// Delete namespaces sequentially in reverse order (children before parents)
	namespaces := d.Tree.AllNamespaces()
	for i := len(namespaces) - 1; i >= 0; i-- {
		_ = d.getCatalog().DropNamespace(ctx, namespaces[i].Path)
	}

	if d.ExternalCatalog == ExternalCatalogNone && d.CatalogURI != "" && d.AccessKey != "" {
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

	if d.ExternalCatalog == ExternalCatalogNone && d.CatalogURI != "" && d.AccessKey != "" {
		if updateStatus != nil {
			updateStatus("Ensuring warehouse exists...")
		}
		err := EnsureWarehouse(ctx, CatalogConfig{
			CatalogURI: d.CatalogURI,
			Warehouse:  d.Tree.Config().CatalogName,
			AccessKey:  d.AccessKey,
			SecretKey:  d.SecretKey,
		})
		if err != nil {
			return fmt.Errorf("ensure warehouse: %w", err)
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
