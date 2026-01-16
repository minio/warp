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

package bench

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

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
	Catalog     *rest.Catalog
	CatalogPool *iceberg.CatalogPool
	Tree        *iceberg.Tree
	TreeConfig  iceberg.TreeConfig

	CatalogURI string
	AccessKey  string
	SecretKey  string

	namespaces []iceberg.NamespaceInfo
	tables     []iceberg.TableInfo
	views      []iceberg.ViewInfo
}

func (b *IcebergRead) Prepare(ctx context.Context) error {
	b.Tree = iceberg.NewTree(b.TreeConfig)

	b.namespaces = b.Tree.AllNamespaces()
	b.tables = b.Tree.AllTables()
	b.views = b.Tree.AllViews()

	if len(b.namespaces) == 0 {
		return fmt.Errorf("no namespaces found: check tree configuration")
	}

	creator := &iceberg.DatasetCreator{
		Catalog:     b.Catalog,
		CatalogPool: b.CatalogPool,
		Tree:        b.Tree,
		CatalogURI:  b.CatalogURI,
		AccessKey:   b.AccessKey,
		SecretKey:   b.SecretKey,
		Concurrency: b.Concurrency,
		OnProgress:  b.prepareProgress,
		OnError:     b.Error,
	}

	return creator.CreateAll(ctx, b.UpdateStatus)
}

func (b *IcebergRead) getCatalog() *rest.Catalog {
	if b.CatalogPool != nil {
		return b.CatalogPool.Get()
	}
	return b.Catalog
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
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(thread)))

			<-wait

			nsIdx := thread % len(b.namespaces)
			tblIdx := thread % max(len(b.tables), 1)
			vwIdx := thread % max(len(b.views), 1)

			hasNs := len(b.namespaces) > 0
			hasTables := len(b.tables) > 0
			hasViews := len(b.views) > 0

			for {
				select {
				case <-done:
					return
				default:
				}

				if b.rpsLimit(ctx) != nil {
					return
				}

				cat := b.getCatalog()

				switch rng.Intn(3) {
				case 0:
					if hasNs {
						b.readNamespace(opCtx, rcv, thread, catalogName, b.namespaces[nsIdx%len(b.namespaces)], cat)
						nsIdx++
					}
				case 1:
					if hasTables {
						b.readTable(opCtx, rcv, thread, catalogName, b.tables[tblIdx%len(b.tables)], cat)
						tblIdx++
					}
				case 2:
					if hasViews {
						b.readView(opCtx, rcv, thread, catalogName, b.views[vwIdx%len(b.views)], cat)
						vwIdx++
					}
				}
			}
		}(i)
	}

	wg.Wait()
	return nil
}

func (b *IcebergRead) readNamespace(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, ns iceberg.NamespaceInfo, cat *rest.Catalog) {
	ident := ns.Path

	op := Operation{
		OpType:   OpNSGet,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalogName, ns.Path),
		Endpoint: catalogName,
	}

	op.Start = time.Now()
	_, err := cat.LoadNamespaceProperties(ctx, ident)
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
	_, err = cat.CheckNamespaceExists(ctx, ident)
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
		_, err = cat.ListNamespaces(ctx, ident)
		op.End = time.Now()

		if err != nil {
			op.Err = err.Error()
		}
		rcv <- op
	}
}

func (b *IcebergRead) readTable(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, tbl iceberg.TableInfo, cat *rest.Catalog) {
	ident := toTableIdentifier(tbl.Namespace, tbl.Name)

	op := Operation{
		OpType:   OpTableGet,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalogName, tbl.Namespace, tbl.Name),
		Endpoint: catalogName,
	}

	op.Start = time.Now()
	_, err := cat.LoadTable(ctx, ident)
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
	_, err = cat.CheckTableExists(ctx, ident)
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
	for _, err := range cat.ListTables(ctx, tbl.Namespace) {
		if err != nil {
			op.Err = err.Error()
		}
	}
	op.End = time.Now()

	rcv <- op
}

func (b *IcebergRead) readView(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, vw iceberg.ViewInfo, cat *rest.Catalog) {
	ident := toTableIdentifier(vw.Namespace, vw.Name)

	op := Operation{
		OpType:   OpViewGet,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalogName, vw.Namespace, vw.Name),
		Endpoint: catalogName,
	}

	op.Start = time.Now()
	_, err := cat.LoadView(ctx, ident)
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
	_, err = cat.CheckViewExists(ctx, ident)
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
	for _, err := range cat.ListViews(ctx, vw.Namespace) {
		if err != nil {
			op.Err = err.Error()
		}
	}
	op.End = time.Now()

	rcv <- op
}

func (b *IcebergRead) Cleanup(ctx context.Context) {
	if b.Tree == nil {
		return
	}
	d := &iceberg.DatasetCreator{
		Catalog:     b.Catalog,
		CatalogPool: b.CatalogPool,
		Tree:        b.Tree,
		CatalogURI:  b.CatalogURI,
		AccessKey:   b.AccessKey,
		SecretKey:   b.SecretKey,
	}
	d.DeleteAll(ctx)
}

func toTableIdentifier(namespace []string, name string) table.Identifier {
	return append(namespace, name)
}
