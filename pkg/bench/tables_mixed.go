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
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	icebergpkg "github.com/minio/warp/pkg/iceberg"
)

type IcebergMixed struct {
	Common
	Catalog     *rest.Catalog
	CatalogPool *icebergpkg.CatalogPool
	Tree        *icebergpkg.Tree
	TreeConfig  icebergpkg.TreeConfig

	CatalogURI      string
	AccessKey       string
	SecretKey       string
	ExternalCatalog icebergpkg.ExternalCatalogType

	Dist *IcebergMixedDistribution

	MaxRetries   int
	RetryBackoff time.Duration
	BackoffMax   time.Duration

	namespaces []icebergpkg.NamespaceInfo
	tables     []icebergpkg.TableInfo
	views      []icebergpkg.ViewInfo

	nsUpdateID    uint64
	tableUpdateID uint64
	viewUpdateID  uint64
}

type IcebergMixedDistribution struct {
	Distribution map[string]float64
	ops          []string
	current      int
	mu           sync.Mutex
}

func (d *IcebergMixedDistribution) Generate() error {
	if err := d.normalize(); err != nil {
		return err
	}

	const genOps = 1000
	d.ops = make([]string, 0, genOps)
	for op, dist := range d.Distribution {
		add := int(0.5 + dist*genOps)
		for range add {
			d.ops = append(d.ops, op)
		}
	}

	rng := rand.New(rand.NewPCG(0xabad1dea, 0xcafebabe))
	rng.Shuffle(len(d.ops), func(i, j int) {
		d.ops[i], d.ops[j] = d.ops[j], d.ops[i]
	})
	return nil
}

func (d *IcebergMixedDistribution) normalize() error {
	total := 0.0
	for op, dist := range d.Distribution {
		if dist < 0 {
			return fmt.Errorf("negative distribution requested for op %q", op)
		}
		total += dist
	}
	if total == 0 {
		return fmt.Errorf("no distribution set, total is 0")
	}
	for op, dist := range d.Distribution {
		d.Distribution[op] = dist / total
	}
	return nil
}

func (d *IcebergMixedDistribution) getOp() string {
	d.mu.Lock()
	op := d.ops[d.current]
	d.current = (d.current + 1) % len(d.ops)
	d.mu.Unlock()
	return op
}

func (b *IcebergMixed) Prepare(ctx context.Context) error {
	b.Tree = icebergpkg.NewTree(b.TreeConfig)

	b.namespaces = b.Tree.AllNamespaces()
	b.tables = b.Tree.AllTables()
	b.views = b.Tree.AllViews()

	if len(b.namespaces) == 0 {
		return fmt.Errorf("no namespaces found: check tree configuration")
	}

	if b.ClientIdx > 0 {
		return nil
	}

	creator := &icebergpkg.DatasetCreator{
		Catalog:         b.Catalog,
		CatalogPool:     b.CatalogPool,
		Tree:            b.Tree,
		CatalogURI:      b.CatalogURI,
		AccessKey:       b.AccessKey,
		SecretKey:       b.SecretKey,
		ExternalCatalog: b.ExternalCatalog,
		Concurrency:     b.Concurrency,
		OnProgress:      b.prepareProgress,
		OnError:         b.Error,
	}

	return creator.CreateAll(ctx, b.UpdateStatus)
}

func (b *IcebergMixed) Start(ctx context.Context, wait chan struct{}) error {
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

			nsListIdx := thread % max(len(b.namespaces), 1)
			nsExistsIdx := thread % max(len(b.namespaces), 1)
			nsFetchIdx := thread % max(len(b.namespaces), 1)

			tblListIdx := thread % max(len(b.tables), 1)
			tblExistsIdx := thread % max(len(b.tables), 1)
			tblFetchIdx := thread % max(len(b.tables), 1)

			viewListIdx := thread % max(len(b.views), 1)
			viewExistsIdx := thread % max(len(b.views), 1)
			viewFetchIdx := thread % max(len(b.views), 1)

			<-wait

			for {
				select {
				case <-done:
					return
				default:
				}

				if b.rpsLimit(ctx) != nil {
					return
				}

				cat := b.Catalog
				if b.CatalogPool != nil {
					cat = b.CatalogPool.Get()
				}

				operation := b.Dist.getOp()
				switch operation {
				case OpNSList:
					ns := b.namespaces[nsListIdx%len(b.namespaces)]
					nsListIdx++
					b.doFetchAllChildrenNamespaces(opCtx, rcv, thread, catalogName, ns, cat)
				case OpNSHead:
					ns := b.namespaces[nsExistsIdx%len(b.namespaces)]
					nsExistsIdx++
					b.doCheckNamespaceExists(opCtx, rcv, thread, catalogName, ns, cat)
				case OpNSGet:
					ns := b.namespaces[nsFetchIdx%len(b.namespaces)]
					nsFetchIdx++
					b.doFetchNamespace(opCtx, rcv, thread, catalogName, ns, cat)
				case OpNSUpdate:
					b.doUpdateNamespaceProperties(opCtx, rcv, thread, catalogName, cat)
				case OpTableList:
					if len(b.tables) == 0 {
						continue
					}
					tbl := b.tables[tblListIdx%len(b.tables)]
					tblListIdx++
					b.doFetchAllTables(opCtx, rcv, thread, catalogName, tbl, cat)
				case OpTableHead:
					if len(b.tables) == 0 {
						continue
					}
					tbl := b.tables[tblExistsIdx%len(b.tables)]
					tblExistsIdx++
					b.doCheckTableExists(opCtx, rcv, thread, catalogName, tbl, cat)
				case OpTableGet:
					if len(b.tables) == 0 {
						continue
					}
					tbl := b.tables[tblFetchIdx%len(b.tables)]
					tblFetchIdx++
					b.doFetchTable(opCtx, rcv, thread, catalogName, tbl, cat)
				case OpTableUpdate:
					if len(b.tables) == 0 {
						continue
					}
					b.doUpdateTable(opCtx, rcv, thread, catalogName, cat)
				case OpViewList:
					if len(b.views) == 0 {
						continue
					}
					vw := b.views[viewListIdx%len(b.views)]
					viewListIdx++
					b.doFetchAllViews(opCtx, rcv, thread, catalogName, vw, cat)
				case OpViewHead:
					if len(b.views) == 0 {
						continue
					}
					vw := b.views[viewExistsIdx%len(b.views)]
					viewExistsIdx++
					b.doCheckViewExists(opCtx, rcv, thread, catalogName, vw, cat)
				case OpViewGet:
					if len(b.views) == 0 {
						continue
					}
					vw := b.views[viewFetchIdx%len(b.views)]
					viewFetchIdx++
					b.doFetchView(opCtx, rcv, thread, catalogName, vw, cat)
				case OpViewUpdate:
					if len(b.views) == 0 {
						continue
					}
					b.doUpdateView(opCtx, rcv, thread, catalogName, cat)
				}
			}
		}(i)
	}

	wg.Wait()
	return nil
}

func (b *IcebergMixed) doFetchAllChildrenNamespaces(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, ns icebergpkg.NamespaceInfo, cat *rest.Catalog) {
	op := Operation{
		OpType:   OpNSList,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalogName, ns.Path),
		Endpoint: catalogName,
	}
	op.Start = time.Now()
	_, err := cat.ListNamespaces(ctx, ns.Path)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doCheckNamespaceExists(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, ns icebergpkg.NamespaceInfo, cat *rest.Catalog) {
	op := Operation{
		OpType:   OpNSHead,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalogName, ns.Path),
		Endpoint: catalogName,
	}
	op.Start = time.Now()
	_, err := cat.CheckNamespaceExists(ctx, ns.Path)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doFetchNamespace(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, ns icebergpkg.NamespaceInfo, cat *rest.Catalog) {
	op := Operation{
		OpType:   OpNSGet,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalogName, ns.Path),
		Endpoint: catalogName,
	}
	op.Start = time.Now()
	_, err := cat.LoadNamespaceProperties(ctx, ns.Path)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doFetchAllTables(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, tbl icebergpkg.TableInfo, cat *rest.Catalog) {
	op := Operation{
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

func (b *IcebergMixed) doCheckTableExists(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, tbl icebergpkg.TableInfo, cat *rest.Catalog) {
	ident := toTableIdentifier(tbl.Namespace, tbl.Name)
	op := Operation{
		OpType:   OpTableHead,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalogName, tbl.Namespace, tbl.Name),
		Endpoint: catalogName,
	}
	op.Start = time.Now()
	_, err := cat.CheckTableExists(ctx, ident)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doFetchTable(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, tbl icebergpkg.TableInfo, cat *rest.Catalog) {
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
}

func (b *IcebergMixed) doFetchAllViews(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, vw icebergpkg.ViewInfo, cat *rest.Catalog) {
	op := Operation{
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

func (b *IcebergMixed) doCheckViewExists(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, vw icebergpkg.ViewInfo, cat *rest.Catalog) {
	ident := toTableIdentifier(vw.Namespace, vw.Name)
	op := Operation{
		OpType:   OpViewHead,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalogName, vw.Namespace, vw.Name),
		Endpoint: catalogName,
	}
	op.Start = time.Now()
	_, err := cat.CheckViewExists(ctx, ident)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doFetchView(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, vw icebergpkg.ViewInfo, cat *rest.Catalog) {
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
}

func (b *IcebergMixed) doUpdateNamespaceProperties(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, cat *rest.Catalog) {
	updateID := atomic.AddUint64(&b.nsUpdateID, 1)
	nsIdx := int((updateID - 1) % uint64(len(b.namespaces)))
	ns := b.namespaces[nsIdx]

	updates := iceberg.Properties{
		fmt.Sprintf("updated_attribute_%d", updateID): fmt.Sprintf("%d", updateID),
	}

	op := Operation{
		OpType:   OpNSUpdate,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalogName, ns.Path),
		Endpoint: catalogName,
	}

	op.Start = time.Now()
	_, err := cat.UpdateNamespaceProperties(ctx, ns.Path, nil, updates)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doUpdateTable(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, cat *rest.Catalog) {
	updateID := atomic.AddUint64(&b.tableUpdateID, 1)
	tblIdx := int((updateID - 1) % uint64(len(b.tables)))
	tbl := b.tables[tblIdx]

	ident := toTableIdentifier(tbl.Namespace, tbl.Name)
	updates := []table.Update{
		table.NewSetPropertiesUpdate(iceberg.Properties{
			fmt.Sprintf("new_attribute_%d", updateID): fmt.Sprintf("new_value_%d", updateID),
		}),
	}

	op := Operation{
		OpType:   OpTableUpdate,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalogName, tbl.Namespace, tbl.Name),
		Endpoint: catalogName,
	}

	op.Start = time.Now()
	var err error
	for retry := 0; retry < b.MaxRetries; retry++ {
		_, err = cat.UpdateTable(ctx, ident, nil, updates)
		if err == nil || !isRetryable(err) {
			break
		}
		backoff := b.RetryBackoff * time.Duration(1<<uint(retry))
		if backoff > b.BackoffMax {
			backoff = b.BackoffMax
		}
		jitter := time.Duration(rand.Int64N(int64(backoff) / 2))
		backoff += jitter
		time.Sleep(backoff)
	}
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doUpdateView(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, cat *rest.Catalog) {
	updateID := atomic.AddUint64(&b.viewUpdateID, 1)
	vwIdx := int((updateID - 1) % uint64(len(b.views)))
	vw := b.views[vwIdx]

	ident := toTableIdentifier(vw.Namespace, vw.Name)
	updates := []view.Update{
		view.NewSetPropertiesUpdate(iceberg.Properties{
			fmt.Sprintf("new_attribute_%d", updateID): fmt.Sprintf("new_value_%d", updateID),
		}),
	}

	op := Operation{
		OpType:   OpViewUpdate,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalogName, vw.Namespace, vw.Name),
		Endpoint: catalogName,
	}

	op.Start = time.Now()
	var err error
	for retry := 0; retry < b.MaxRetries; retry++ {
		_, err = cat.UpdateView(ctx, ident, nil, updates)
		if err == nil || !isRetryable(err) {
			break
		}
		backoff := b.RetryBackoff * time.Duration(1<<uint(retry))
		if backoff > b.BackoffMax {
			backoff = b.BackoffMax
		}
		jitter := time.Duration(rand.Int64N(int64(backoff) / 2))
		backoff += jitter
		time.Sleep(backoff)
	}
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) Cleanup(ctx context.Context) {
	if b.Tree == nil || b.ClientIdx > 0 {
		return
	}
	d := &icebergpkg.DatasetCreator{
		Catalog:         b.Catalog,
		CatalogPool:     b.CatalogPool,
		Tree:            b.Tree,
		CatalogURI:      b.CatalogURI,
		AccessKey:       b.AccessKey,
		SecretKey:       b.SecretKey,
		ExternalCatalog: b.ExternalCatalog,
		Concurrency:     b.Concurrency,
	}
	d.DeleteAll(ctx)
}
