package bench

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/warp/pkg/iceberg"
	"github.com/minio/warp/pkg/iceberg/rest"
)

type IcebergCommits struct {
	Common
	RestClient *rest.Client
	Tree       *iceberg.Tree
	TreeConfig iceberg.TreeConfig

	CatalogURI string
	AccessKey  string
	SecretKey  string

	TableCommitsThroughput int
	ViewCommitsThroughput  int
	MaxRetries             int
	RetryBackoff           time.Duration

	tables []iceberg.TableInfo
	views  []iceberg.ViewInfo
}

func (b *IcebergCommits) Prepare(ctx context.Context) error {
	b.Tree = iceberg.NewTree(b.TreeConfig)

	b.tables = b.Tree.AllTables()
	b.views = b.Tree.AllViews()

	if len(b.tables) == 0 && len(b.views) == 0 {
		return fmt.Errorf("no tables or views found: check tree configuration")
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

func (b *IcebergCommits) Start(ctx context.Context, wait chan struct{}) error {
	var wg sync.WaitGroup
	c := b.Collector

	if b.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, OpTableCommit, b.AutoTermScale, autoTermCheck, autoTermSamples, b.AutoTermDur)
	}

	tableWorkers := b.TableCommitsThroughput
	if tableWorkers == 0 {
		tableWorkers = b.Concurrency / 2
	}
	viewWorkers := b.ViewCommitsThroughput
	if viewWorkers == 0 && len(b.views) > 0 {
		viewWorkers = b.Concurrency / 2
	}

	if len(b.tables) > 0 && tableWorkers > 0 {
		wg.Add(tableWorkers)
		for i := 0; i < tableWorkers; i++ {
			go func(thread int) {
				defer wg.Done()
				b.runTableCommits(ctx, wait, thread)
			}(i)
		}
	}

	if len(b.views) > 0 && viewWorkers > 0 {
		wg.Add(viewWorkers)
		for i := 0; i < viewWorkers; i++ {
			go func(thread int) {
				defer wg.Done()
				b.runViewCommits(ctx, wait, thread+tableWorkers)
			}(i)
		}
	}

	wg.Wait()
	return nil
}

func (b *IcebergCommits) runTableCommits(ctx context.Context, wait chan struct{}, thread int) {
	rcv := b.Collector.Receiver()
	done := ctx.Done()
	catalog := b.TreeConfig.CatalogName

	<-wait

	var globalUpdateID uint64
	tableIdx := 0

	for {
		select {
		case <-done:
			return
		default:
		}

		if b.rpsLimit(ctx) != nil {
			return
		}

		tbl := b.tables[tableIdx%len(b.tables)]
		tableIdx++

		if tableIdx%len(b.tables) == 0 {
			globalUpdateID++
		}

		updateID := atomic.LoadUint64(&globalUpdateID)

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
			OpType:   OpTableCommit,
			Thread:   uint32(thread),
			File:     fmt.Sprintf("%s/%v/%s", catalog, tbl.Namespace, tbl.Name),
			ObjPerOp: 0,
			Endpoint: catalog,
		}

		op.Start = time.Now()
		var err error
		for retry := 0; retry < b.MaxRetries; retry++ {
			_, err = b.RestClient.UpdateTable(ctx, catalog, tbl.Namespace, tbl.Name, req)
			if err == nil || !rest.IsRetryable(err) {
				break
			}
			if b.RetryBackoff > 0 {
				time.Sleep(b.RetryBackoff)
			}
		}
		op.End = time.Now()

		if err != nil {
			op.Err = err.Error()
		}
		rcv <- op
	}
}

func (b *IcebergCommits) runViewCommits(ctx context.Context, wait chan struct{}, thread int) {
	rcv := b.Collector.Receiver()
	done := ctx.Done()
	catalog := b.TreeConfig.CatalogName

	<-wait

	var globalUpdateID uint64
	viewIdx := 0

	for {
		select {
		case <-done:
			return
		default:
		}

		if b.rpsLimit(ctx) != nil {
			return
		}

		vw := b.views[viewIdx%len(b.views)]
		viewIdx++

		if viewIdx%len(b.views) == 0 {
			globalUpdateID++
		}

		updateID := atomic.LoadUint64(&globalUpdateID)

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
			OpType:   OpViewCommit,
			Thread:   uint32(thread),
			File:     fmt.Sprintf("%s/%v/%s", catalog, vw.Namespace, vw.Name),
			ObjPerOp: 0,
			Endpoint: catalog,
		}

		op.Start = time.Now()
		var err error
		for retry := 0; retry < b.MaxRetries; retry++ {
			_, err = b.RestClient.UpdateView(ctx, catalog, vw.Namespace, vw.Name, req)
			if err == nil || !rest.IsRetryable(err) {
				break
			}
			if b.RetryBackoff > 0 {
				time.Sleep(b.RetryBackoff)
			}
		}
		op.End = time.Now()

		if err != nil {
			op.Err = err.Error()
		}
		rcv <- op
	}
}

func (b *IcebergCommits) Cleanup(_ context.Context) {
	b.UpdateStatus("Cleanup: skipping (commits benchmark does not delete data)")
}
