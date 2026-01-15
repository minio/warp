package bench

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/warp/pkg/iceberg"
	"github.com/minio/warp/pkg/iceberg/rest"
)

type IcebergMixed struct {
	Common
	RestClient *rest.Client
	Tree       *iceberg.Tree
	TreeConfig iceberg.TreeConfig

	CatalogURI string
	AccessKey  string
	SecretKey  string

	ReadRatio float64

	namespaces []iceberg.NamespaceInfo
	tables     []iceberg.TableInfo
	views      []iceberg.ViewInfo

	nsUpdateID    uint64
	tableUpdateID uint64
	viewUpdateID  uint64
}

func (b *IcebergMixed) Prepare(ctx context.Context) error {
	b.Tree = iceberg.NewTree(b.TreeConfig)

	b.namespaces = b.Tree.AllNamespaces()
	b.tables = b.Tree.AllTables()
	b.views = b.Tree.AllViews()

	if len(b.namespaces) == 0 {
		return fmt.Errorf("no namespaces found: check tree configuration")
	}

	creator := &iceberg.DatasetCreator{
		RestClient: b.RestClient,
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
		ctx = c.AutoTerm(ctx, OpNamespaceGet, b.AutoTermScale, autoTermCheck, autoTermSamples, b.AutoTermDur)
	}

	for i := 0; i < b.Concurrency; i++ {
		go func(thread int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(thread) + time.Now().UnixNano()))
			rcv := c.Receiver()
			done := ctx.Done()
			catalog := b.TreeConfig.CatalogName

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

				isRead := rng.Float64() < b.ReadRatio

				if isRead {
					opChoice := rng.Intn(9)
					switch opChoice {
					case 0:
						ns := b.namespaces[nsListIdx%len(b.namespaces)]
						nsListIdx++
						b.doFetchAllChildrenNamespaces(ctx, rcv, thread, catalog, ns)
					case 1:
						ns := b.namespaces[nsExistsIdx%len(b.namespaces)]
						nsExistsIdx++
						b.doCheckNamespaceExists(ctx, rcv, thread, catalog, ns)
					case 2:
						ns := b.namespaces[nsFetchIdx%len(b.namespaces)]
						nsFetchIdx++
						b.doFetchNamespace(ctx, rcv, thread, catalog, ns)
					case 3:
						if len(b.tables) == 0 {
							continue
						}
						tbl := b.tables[tblListIdx%len(b.tables)]
						tblListIdx++
						b.doFetchAllTables(ctx, rcv, thread, catalog, tbl)
					case 4:
						if len(b.tables) == 0 {
							continue
						}
						tbl := b.tables[tblExistsIdx%len(b.tables)]
						tblExistsIdx++
						b.doCheckTableExists(ctx, rcv, thread, catalog, tbl)
					case 5:
						if len(b.tables) == 0 {
							continue
						}
						tbl := b.tables[tblFetchIdx%len(b.tables)]
						tblFetchIdx++
						b.doFetchTable(ctx, rcv, thread, catalog, tbl)
					case 6:
						if len(b.views) == 0 {
							continue
						}
						vw := b.views[viewListIdx%len(b.views)]
						viewListIdx++
						b.doFetchAllViews(ctx, rcv, thread, catalog, vw)
					case 7:
						if len(b.views) == 0 {
							continue
						}
						vw := b.views[viewExistsIdx%len(b.views)]
						viewExistsIdx++
						b.doCheckViewExists(ctx, rcv, thread, catalog, vw)
					case 8:
						if len(b.views) == 0 {
							continue
						}
						vw := b.views[viewFetchIdx%len(b.views)]
						viewFetchIdx++
						b.doFetchView(ctx, rcv, thread, catalog, vw)
					}
				} else {
					opChoice := rng.Intn(3)
					switch opChoice {
					case 0:
						b.doUpdateNamespaceProperties(ctx, rcv, thread, catalog)
					case 1:
						if len(b.tables) == 0 {
							continue
						}
						b.doUpdateTable(ctx, rcv, thread, catalog)
					case 2:
						if len(b.views) == 0 {
							continue
						}
						b.doUpdateView(ctx, rcv, thread, catalog)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	return nil
}

func (b *IcebergMixed) doFetchAllChildrenNamespaces(ctx context.Context, rcv chan<- Operation, thread int, catalog string, ns iceberg.NamespaceInfo) {
	op := Operation{
		OpType:   OpNamespaceList,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalog, ns.Path),
		ObjPerOp: 0,
		Endpoint: catalog,
	}
	op.Start = time.Now()
	_, err := b.RestClient.ListNamespaces(ctx, catalog, ns.Path)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doCheckNamespaceExists(ctx context.Context, rcv chan<- Operation, thread int, catalog string, ns iceberg.NamespaceInfo) {
	op := Operation{
		OpType:   OpNamespaceHead,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalog, ns.Path),
		ObjPerOp: 0,
		Endpoint: catalog,
	}
	op.Start = time.Now()
	_, err := b.RestClient.NamespaceExists(ctx, catalog, ns.Path)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doFetchNamespace(ctx context.Context, rcv chan<- Operation, thread int, catalog string, ns iceberg.NamespaceInfo) {
	op := Operation{
		OpType:   OpNamespaceGet,
		Thread:   uint32(thread),
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
}

func (b *IcebergMixed) doFetchAllTables(ctx context.Context, rcv chan<- Operation, thread int, catalog string, tbl iceberg.TableInfo) {
	op := Operation{
		OpType:   OpTableList,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalog, tbl.Namespace),
		ObjPerOp: 0,
		Endpoint: catalog,
	}
	op.Start = time.Now()
	_, err := b.RestClient.ListTables(ctx, catalog, tbl.Namespace)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doCheckTableExists(ctx context.Context, rcv chan<- Operation, thread int, catalog string, tbl iceberg.TableInfo) {
	op := Operation{
		OpType:   OpTableHead,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalog, tbl.Namespace, tbl.Name),
		ObjPerOp: 0,
		Endpoint: catalog,
	}
	op.Start = time.Now()
	_, err := b.RestClient.TableExists(ctx, catalog, tbl.Namespace, tbl.Name)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doFetchTable(ctx context.Context, rcv chan<- Operation, thread int, catalog string, tbl iceberg.TableInfo) {
	op := Operation{
		OpType:   OpTableGet,
		Thread:   uint32(thread),
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
}

func (b *IcebergMixed) doFetchAllViews(ctx context.Context, rcv chan<- Operation, thread int, catalog string, vw iceberg.ViewInfo) {
	op := Operation{
		OpType:   OpViewList,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalog, vw.Namespace),
		ObjPerOp: 0,
		Endpoint: catalog,
	}
	op.Start = time.Now()
	_, err := b.RestClient.ListViews(ctx, catalog, vw.Namespace)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doCheckViewExists(ctx context.Context, rcv chan<- Operation, thread int, catalog string, vw iceberg.ViewInfo) {
	op := Operation{
		OpType:   OpViewHead,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalog, vw.Namespace, vw.Name),
		ObjPerOp: 0,
		Endpoint: catalog,
	}
	op.Start = time.Now()
	_, err := b.RestClient.ViewExists(ctx, catalog, vw.Namespace, vw.Name)
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doFetchView(ctx context.Context, rcv chan<- Operation, thread int, catalog string, vw iceberg.ViewInfo) {
	op := Operation{
		OpType:   OpViewGet,
		Thread:   uint32(thread),
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
}

func (b *IcebergMixed) doUpdateNamespaceProperties(ctx context.Context, rcv chan<- Operation, thread int, catalog string) {
	updateID := atomic.AddUint64(&b.nsUpdateID, 1)
	nsIdx := int((updateID - 1) % uint64(len(b.namespaces)))
	ns := b.namespaces[nsIdx]

	updates := map[string]string{
		fmt.Sprintf("UpdatedAttribute_%d", updateID): fmt.Sprintf("%d", updateID),
	}

	op := Operation{
		OpType:   OpNamespaceUpdate,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v", catalog, ns.Path),
		ObjPerOp: 0,
		Endpoint: catalog,
	}

	op.Start = time.Now()
	_, err := b.RestClient.UpdateNamespaceProperties(ctx, catalog, ns.Path, updates, nil)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doUpdateTable(ctx context.Context, rcv chan<- Operation, thread int, catalog string) {
	updateID := atomic.AddUint64(&b.tableUpdateID, 1)
	tblIdx := int((updateID - 1) % uint64(len(b.tables)))
	tbl := b.tables[tblIdx]

	req := rest.CommitTableRequest{
		Updates: []rest.TableUpdate{
			{
				Action: "set-properties",
				Updates: map[string]string{
					fmt.Sprintf("NewAttribute_%d", updateID): fmt.Sprintf("NewValue_%d", updateID),
				},
			},
		},
	}

	op := Operation{
		OpType:   OpTableUpdate,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalog, tbl.Namespace, tbl.Name),
		ObjPerOp: 0,
		Endpoint: catalog,
	}

	op.Start = time.Now()
	_, err := b.RestClient.UpdateTable(ctx, catalog, tbl.Namespace, tbl.Name, req)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) doUpdateView(ctx context.Context, rcv chan<- Operation, thread int, catalog string) {
	updateID := atomic.AddUint64(&b.viewUpdateID, 1)
	vwIdx := int((updateID - 1) % uint64(len(b.views)))
	vw := b.views[vwIdx]

	req := rest.CommitViewRequest{
		Updates: []rest.ViewUpdate{
			{
				Action: "set-properties",
				Updates: map[string]string{
					fmt.Sprintf("NewAttribute_%d", updateID): fmt.Sprintf("NewValue_%d", updateID),
				},
			},
		},
	}

	op := Operation{
		OpType:   OpViewUpdate,
		Thread:   uint32(thread),
		File:     fmt.Sprintf("%s/%v/%s", catalog, vw.Namespace, vw.Name),
		ObjPerOp: 0,
		Endpoint: catalog,
	}

	op.Start = time.Now()
	_, err := b.RestClient.UpdateView(ctx, catalog, vw.Namespace, vw.Name, req)
	op.End = time.Now()

	if err != nil {
		op.Err = err.Error()
	}
	rcv <- op
}

func (b *IcebergMixed) Cleanup(_ context.Context) {
	b.UpdateStatus("Cleanup: skipping (mixed benchmark does not delete data)")
}
