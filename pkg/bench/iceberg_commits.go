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

type IcebergCommits struct {
	Common
	Catalog     *rest.Catalog
	CatalogPool *icebergpkg.CatalogPool
	Tree        *icebergpkg.Tree
	TreeConfig  icebergpkg.TreeConfig

	CatalogURI string
	AccessKey  string
	SecretKey       string
	ExternalCatalog icebergpkg.ExternalCatalogType

	TableCommitsThroughput int
	ViewCommitsThroughput  int
	MaxRetries             int
	RetryBackoff           time.Duration
	BackoffMax             time.Duration

	tables []icebergpkg.TableInfo
	views  []icebergpkg.ViewInfo
}

func (b *IcebergCommits) Prepare(ctx context.Context) error {
	b.Tree = icebergpkg.NewTree(b.TreeConfig)

	b.tables = b.Tree.AllTables()
	b.views = b.Tree.AllViews()

	if len(b.tables) == 0 && len(b.views) == 0 {
		return fmt.Errorf("no tables or views found: check tree configuration")
	}

	creator := &icebergpkg.DatasetCreator{
		Catalog:     b.Catalog,
		CatalogPool: b.CatalogPool,
		Tree:        b.Tree,
		CatalogURI:  b.CatalogURI,
		AccessKey:   b.AccessKey,
		SecretKey:       b.SecretKey,
		ExternalCatalog: b.ExternalCatalog,
		Concurrency: b.Concurrency,
		OnProgress:  b.prepareProgress,
		OnError:     b.Error,
	}

	return creator.CreateAll(ctx, b.UpdateStatus)
}

func (b *IcebergCommits) Start(ctx context.Context, wait chan struct{}) error {
	var wg sync.WaitGroup
	c := b.Collector

	if b.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, OpTableUpdate, b.AutoTermScale, autoTermCheck, autoTermSamples, b.AutoTermDur)
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
	catalogName := b.TreeConfig.CatalogName
	opCtx := context.Background()

	<-wait

	var globalUpdateID uint64
	tableIdx := thread % max(len(b.tables), 1)

	for {
		select {
		case <-done:
			return
		default:
		}

		if b.rpsLimit(ctx) != nil {
			return
		}

		// Get catalog from pool for round-robin, or use single catalog
		cat := b.Catalog
		if b.CatalogPool != nil {
			cat = b.CatalogPool.Get()
		}

		tbl := b.tables[tableIdx%len(b.tables)]
		tableIdx++

		if tableIdx%len(b.tables) == 0 {
			globalUpdateID++
		}

		updateID := atomic.LoadUint64(&globalUpdateID)

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
			_, err = cat.UpdateTable(opCtx, ident, nil, updates)
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
}

func (b *IcebergCommits) runViewCommits(ctx context.Context, wait chan struct{}, thread int) {
	rcv := b.Collector.Receiver()
	done := ctx.Done()
	catalogName := b.TreeConfig.CatalogName
	opCtx := context.Background()

	<-wait

	var globalUpdateID uint64
	viewIdx := thread % max(len(b.views), 1)

	for {
		select {
		case <-done:
			return
		default:
		}

		if b.rpsLimit(ctx) != nil {
			return
		}

		// Get catalog from pool for round-robin, or use single catalog
		cat := b.Catalog
		if b.CatalogPool != nil {
			cat = b.CatalogPool.Get()
		}

		vw := b.views[viewIdx%len(b.views)]
		viewIdx++

		if viewIdx%len(b.views) == 0 {
			globalUpdateID++
		}

		updateID := atomic.LoadUint64(&globalUpdateID)

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
			_, err = cat.UpdateView(opCtx, ident, nil, updates)
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
}

func (b *IcebergCommits) Cleanup(ctx context.Context) {
	if b.Tree == nil {
		return
	}
	d := &icebergpkg.DatasetCreator{
		Catalog:     b.Catalog,
		CatalogPool: b.CatalogPool,
		Tree:        b.Tree,
		CatalogURI:  b.CatalogURI,
		AccessKey:   b.AccessKey,
		SecretKey:       b.SecretKey,
		ExternalCatalog: b.ExternalCatalog,
		Concurrency: b.Concurrency,
	}
	d.DeleteAll(ctx)
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return errStr == "Conflict" || errStr == "Internal Server Error"
}
