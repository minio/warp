package bench

import (
	"context"
	"fmt"
	"math/rand"
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
	Catalog    *rest.Catalog
	Tree       *icebergpkg.Tree
	TreeConfig icebergpkg.TreeConfig

	CatalogURI string
	AccessKey  string
	SecretKey  string

	Dist *IcebergMixedDistribution

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

	rng := rand.New(rand.NewSource(0xabad1dea))
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

	creator := &icebergpkg.DatasetCreator{
		Catalog:    b.Catalog,
		Tree:       b.Tree,
		CatalogURI: b.CatalogURI,
		AccessKey:  b.AccessKey,
		SecretKey:  b.SecretKey,
		OnProgress: b.prepareProgress,
		OnError:    b.Error,
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

			nsListIdx := 0
			nsExistsIdx := 0
			nsFetchIdx := 0

			tblListIdx := 0
			tblExistsIdx := 0
			tblFetchIdx := 0

			viewListIdx := 0
			viewExistsIdx := 0
			viewFetchIdx := 0

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

				operation := b.Dist.getOp()
				switch operation {
				case OpNSList:
					ns := b.namespaces[nsListIdx%len(b.namespaces)]
					nsListIdx++
					b.doFetchAllChildrenNamespaces(opCtx, rcv, thread, catalogName, ns)
				case OpNSHead:
					ns := b.namespaces[nsExistsIdx%len(b.namespaces)]
					nsExistsIdx++
					b.doCheckNamespaceExists(opCtx, rcv, thread, catalogName, ns)
				case OpNSGet:
					ns := b.namespaces[nsFetchIdx%len(b.namespaces)]
					nsFetchIdx++
					b.doFetchNamespace(opCtx, rcv, thread, catalogName, ns)
				case OpNSUpdate:
					b.doUpdateNamespaceProperties(opCtx, rcv, thread, catalogName)
				case OpTableList:
					if len(b.tables) == 0 {
						continue
					}
					tbl := b.tables[tblListIdx%len(b.tables)]
					tblListIdx++
					b.doFetchAllTables(opCtx, rcv, thread, catalogName, tbl)
				case OpTableHead:
					if len(b.tables) == 0 {
						continue
					}
					tbl := b.tables[tblExistsIdx%len(b.tables)]
					tblExistsIdx++
					b.doCheckTableExists(opCtx, rcv, thread, catalogName, tbl)
				case OpTableGet:
					if len(b.tables) == 0 {
						continue
					}
					tbl := b.tables[tblFetchIdx%len(b.tables)]
					tblFetchIdx++
					b.doFetchTable(opCtx, rcv, thread, catalogName, tbl)
				case OpTableUpdate:
					if len(b.tables) == 0 {
						continue
					}
					b.doUpdateTable(opCtx, rcv, thread, catalogName)
				case OpViewList:
					if len(b.views) == 0 {
						continue
					}
					vw := b.views[viewListIdx%len(b.views)]
					viewListIdx++
					b.doFetchAllViews(opCtx, rcv, thread, catalogName, vw)
				case OpViewHead:
					if len(b.views) == 0 {
						continue
					}
					vw := b.views[viewExistsIdx%len(b.views)]
					viewExistsIdx++
					b.doCheckViewExists(opCtx, rcv, thread, catalogName, vw)
				case OpViewGet:
					if len(b.views) == 0 {
						continue
					}
					vw := b.views[viewFetchIdx%len(b.views)]
					viewFetchIdx++
					b.doFetchView(opCtx, rcv, thread, catalogName, vw)
				case OpViewUpdate:
					if len(b.views) == 0 {
						continue
					}
					b.doUpdateView(opCtx, rcv, thread, catalogName)
				}
			}
		}(i)
	}

	wg.Wait()
	return nil
}

func (b *IcebergMixed) doFetchAllChildrenNamespaces(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, ns icebergpkg.NamespaceInfo) {
	op := Operation{
		OpType:   OpNSList,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalogName, ns.Path),
		Endpoint: catalogName,
	}
	op.Start = time.Now()
	_, err := b.Catalog.ListNamespaces(ctx, ns.Path)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doCheckNamespaceExists(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, ns icebergpkg.NamespaceInfo) {
	op := Operation{
		OpType:   OpNSHead,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalogName, ns.Path),
		Endpoint: catalogName,
	}
	op.Start = time.Now()
	_, err := b.Catalog.CheckNamespaceExists(ctx, ns.Path)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doFetchNamespace(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, ns icebergpkg.NamespaceInfo) {
	op := Operation{
		OpType:   OpNSGet,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalogName, ns.Path),
		Endpoint: catalogName,
	}
	op.Start = time.Now()
	_, err := b.Catalog.LoadNamespaceProperties(ctx, ns.Path)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doFetchAllTables(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, tbl icebergpkg.TableInfo) {
	op := Operation{
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

func (b *IcebergMixed) doCheckTableExists(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, tbl icebergpkg.TableInfo) {
	ident := toTableIdentifier(tbl.Namespace, tbl.Name)
	op := Operation{
		OpType:   OpTableHead,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalogName, tbl.Namespace, tbl.Name),
		Endpoint: catalogName,
	}
	op.Start = time.Now()
	_, err := b.Catalog.CheckTableExists(ctx, ident)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doFetchTable(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, tbl icebergpkg.TableInfo) {
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
}

func (b *IcebergMixed) doFetchAllViews(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, vw icebergpkg.ViewInfo) {
	op := Operation{
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

func (b *IcebergMixed) doCheckViewExists(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, vw icebergpkg.ViewInfo) {
	ident := toTableIdentifier(vw.Namespace, vw.Name)
	op := Operation{
		OpType:   OpViewHead,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalogName, vw.Namespace, vw.Name),
		Endpoint: catalogName,
	}
	op.Start = time.Now()
	_, err := b.Catalog.CheckViewExists(ctx, ident)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doFetchView(ctx context.Context, rcv chan<- Operation, thread int, catalogName string, vw icebergpkg.ViewInfo) {
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
}

func (b *IcebergMixed) doUpdateNamespaceProperties(ctx context.Context, rcv chan<- Operation, thread int, catalogName string) {
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
	_, err := b.Catalog.UpdateNamespaceProperties(ctx, ns.Path, nil, updates)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doUpdateTable(ctx context.Context, rcv chan<- Operation, thread int, catalogName string) {
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
	_, err := b.Catalog.UpdateTable(ctx, ident, nil, updates)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doUpdateView(ctx context.Context, rcv chan<- Operation, thread int, catalogName string) {
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
	_, err := b.Catalog.UpdateView(ctx, ident, nil, updates)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) Cleanup(ctx context.Context) {
	d := &icebergpkg.DatasetCreator{
		Catalog:    b.Catalog,
		Tree:       b.Tree,
		CatalogURI: b.CatalogURI,
		AccessKey:  b.AccessKey,
		SecretKey:  b.SecretKey,
	}
	d.DeleteAll(ctx)
}
