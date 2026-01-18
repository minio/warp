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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	catalogpkg "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	warpiceberg "github.com/minio/warp/pkg/iceberg"
)

type Iceberg struct {
	Common

	Catalog     *rest.Catalog
	CatalogPool *warpiceberg.CatalogPool
	TreeConfig  warpiceberg.TreeConfig
	Tree        *warpiceberg.Tree
	CatalogURI  string
	AccessKey   string
	SecretKey       string
	ExternalCatalog warpiceberg.ExternalCatalogType

	NumFiles    int
	RowsPerFile int
	CacheDir    string

	UseTPCDS    bool
	ScaleFactor string
	TPCDSTable  string

	MaxRetries  int
	BackoffBase time.Duration
	BackoffMax  time.Duration

	dataFiles []string
	tables    []warpiceberg.TableInfo
	prefixes  map[string]struct{}
}

func (b *Iceberg) getCatalog() *rest.Catalog {
	if b.CatalogPool != nil {
		return b.CatalogPool.Get()
	}
	return b.Catalog
}

func (b *Iceberg) Prepare(ctx context.Context) error {
	b.Tree = warpiceberg.NewTree(b.TreeConfig)

	if b.UpdateStatus != nil {
		b.UpdateStatus("Creating dataset tree...")
	}

	creator := &warpiceberg.DatasetCreator{
		Catalog:     b.Catalog,
		CatalogPool: b.CatalogPool,
		Tree:        b.Tree,
		CatalogURI:  b.CatalogURI,
		AccessKey:   b.AccessKey,
		SecretKey:       b.SecretKey,
		ExternalCatalog: b.ExternalCatalog,
		Concurrency: b.Concurrency,
	}

	if err := creator.CreateNamespaces(ctx); err != nil {
		return fmt.Errorf("failed to create namespaces: %w", err)
	}

	treeTables := b.Tree.AllTables()
	if len(treeTables) == 0 {
		return fmt.Errorf("no tables in tree config")
	}

	if b.UpdateStatus != nil {
		b.UpdateStatus("Creating tables with write schema...")
	}

	var schema *iceberg.Schema
	if b.UseTPCDS {
		schema = warpiceberg.StoreSalesSchema()
	} else {
		schema = warpiceberg.BenchmarkDataSchema()
	}

	b.tables = make([]warpiceberg.TableInfo, len(treeTables))
	concurrency := b.Concurrency
	if concurrency > 100 {
		concurrency = 100
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	var firstErr error
	var errMu sync.Mutex

	for i, tbl := range treeTables {
		wg.Add(1)
		sem <- struct{}{}

		go func(i int, tbl warpiceberg.TableInfo) {
			defer wg.Done()
			defer func() { <-sem }()

			ident := append([]string{}, tbl.Namespace...)
			ident = append(ident, tbl.Name)

			cat := b.getCatalog()
			loadedTbl, err := cat.CreateTable(ctx, ident, schema,
				catalogpkg.WithLocation(tbl.Location),
			)
			if err != nil {
				if !warpiceberg.IsAlreadyExists(err) {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("failed to create table %s: %w", tbl.Name, err)
					}
					errMu.Unlock()
					return
				}
				loadedTbl, err = cat.LoadTable(ctx, ident)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("failed to load table %s: %w", tbl.Name, err)
					}
					errMu.Unlock()
					return
				}
			}
			b.tables[i] = warpiceberg.TableInfo{
				Index:     tbl.Index,
				Name:      tbl.Name,
				Namespace: tbl.Namespace,
				Location:  loadedTbl.Location(),
			}
		}(i, tbl)
	}
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}

	if b.UpdateStatus != nil {
		b.UpdateStatus(fmt.Sprintf("Created %d namespaces, %d tables", b.Tree.TotalNamespaces(), len(b.tables)))
	}

	if b.UseTPCDS {
		cfg := warpiceberg.TPCDSConfig{
			ScaleFactor: b.ScaleFactor,
			Table:       b.TPCDSTable,
			CacheDir:    b.CacheDir,
			Concurrency: b.Concurrency,
		}

		files, err := warpiceberg.GetCachedTPCDSFiles(cfg)
		if err != nil {
			if b.UpdateStatus != nil {
				b.UpdateStatus(fmt.Sprintf("Downloading TPC-DS %s/%s from GCS...", b.ScaleFactor, b.TPCDSTable))
			}

			result, err := warpiceberg.DownloadTPCDS(ctx, cfg, func(completed, total, _ int64) {
				b.prepareProgress(0.3 + 0.7*float64(completed)/float64(total))
			})
			if err != nil {
				return fmt.Errorf("failed to download TPC-DS data: %w", err)
			}
			files = result.Files

			if b.UpdateStatus != nil {
				b.UpdateStatus(fmt.Sprintf("Downloaded %d files (%.2f GB)", len(files), float64(result.TotalBytes)/(1024*1024*1024)))
			}
		} else if b.UpdateStatus != nil {
			b.UpdateStatus(fmt.Sprintf("Using %d cached TPC-DS files", len(files)))
		}

		b.dataFiles = files
	} else {
		if b.UpdateStatus != nil {
			b.UpdateStatus("Generating test data...")
		}

		dataDir := filepath.Join(b.CacheDir, "iceberg-bench-data")
		result, err := warpiceberg.GenerateParquetFiles(ctx, warpiceberg.GenerateConfig{
			OutputDir:   dataDir,
			NumFiles:    b.NumFiles,
			RowsPerFile: b.RowsPerFile,
			Concurrency: b.Concurrency,
		}, func(completed, total int64) {
			b.prepareProgress(0.3 + 0.7*float64(completed)/float64(total))
		})
		if err != nil {
			return fmt.Errorf("failed to generate test data: %w", err)
		}

		if b.UpdateStatus != nil {
			b.UpdateStatus(fmt.Sprintf("Generated %d files, %d rows, %.1f MB",
				len(result.Files), result.TotalRows, float64(result.TotalBytes)/(1024*1024)))
		}

		b.dataFiles = result.Files
	}

	return nil
}

func (b *Iceberg) Start(ctx context.Context, wait chan struct{}) error {
	var wg sync.WaitGroup
	wg.Add(b.Concurrency)
	c := b.Collector

	if b.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, "ICEBERG", b.AutoTermScale, autoTermCheck, autoTermSamples, b.AutoTermDur)
	}

	b.prefixes = make(map[string]struct{}, b.Concurrency*len(b.tables))

	for i := 0; i < b.Concurrency; i++ {
		for _, tbl := range b.tables {
			_, tablePrefix := parseTableLocation(tbl.Location)
			prefix := fmt.Sprintf("%s/data/worker-%d", tablePrefix, i)
			b.prefixes[prefix] = struct{}{}
		}
	}

	filesPerWorker := len(b.dataFiles) / b.Concurrency
	if filesPerWorker == 0 {
		filesPerWorker = 1
	}

	for i := 0; i < b.Concurrency; i++ {
		workerFiles := b.getWorkerFiles(i, filesPerWorker)

		go func(workerID int, files []string) {
			rcv := c.Receiver()
			defer wg.Done()

			done := ctx.Done()
			opCtx := context.Background()
			tableIdx := workerID

			<-wait

			for iter := 0; ; iter++ {
				select {
				case <-done:
					return
				default:
				}

				tbl := b.tables[tableIdx%len(b.tables)]
				tableIdx++

				tableBucket, tablePrefix := parseTableLocation(tbl.Location)
				if tableBucket == "" {
					tableBucket = b.TreeConfig.CatalogName
				}

				prefix := fmt.Sprintf("%s/data/worker-%d", tablePrefix, workerID)

				var uploadedPaths []string
				for _, localFile := range files {
					select {
					case <-done:
						return
					default:
					}

					if b.rpsLimit(ctx) != nil {
						return
					}

					client, cldone := b.Client()
					objName := fmt.Sprintf("%s/iter-%d/%d/%s", prefix, iter, time.Now().UnixNano(), filepath.Base(localFile))

					op := Operation{
						OpType:   "UPLOAD",
						Thread:   uint32(workerID),
						File:     objName,
						Endpoint: client.EndpointURL().String(),
					}

					f, err := os.Open(localFile)
					if err != nil {
						op.Err = err.Error()
						op.Start = time.Now()
						op.End = op.Start
						rcv <- op
						cldone()
						continue
					}

					info, _ := f.Stat()
					op.Size = info.Size()
					opts := b.PutOpts
					opts.ContentType = "application/octet-stream"
					op.Start = time.Now()

					_, err = client.PutObject(opCtx, tableBucket, objName, f, info.Size(), opts)
					op.End = time.Now()
					f.Close()

					if err != nil {
						op.Err = err.Error()
					} else {
						uploadedPaths = append(uploadedPaths, fmt.Sprintf("s3://%s/%s", tableBucket, objName))
					}

					cldone()
					rcv <- op
				}

				if len(uploadedPaths) > 0 {
					select {
					case <-done:
						return
					default:
					}

					commitOp := Operation{
						OpType: "COMMIT",
						Thread: uint32(workerID),
						File:   fmt.Sprintf("%s/%s", strings.Join(tbl.Namespace, "."), tbl.Name),
					}

					client, cldone := b.Client()
					commitOp.Endpoint = client.EndpointURL().String()
					cldone()

					commitOp.Start = time.Now()

					cat := b.Catalog
					if b.CatalogPool != nil {
						cat = b.CatalogPool.Get()
					}

					ident := catalogpkg.ToIdentifier(fmt.Sprintf("%s.%s", strings.Join(tbl.Namespace, "."), tbl.Name))
					loadedTbl, err := cat.LoadTable(opCtx, ident)
					if err != nil {
						commitOp.End = time.Now()
						commitOp.Err = err.Error()
						rcv <- commitOp
						continue
					}

					result := warpiceberg.CommitWithRetry(opCtx, loadedTbl, uploadedPaths, warpiceberg.CommitConfig{
						MaxRetries:  b.MaxRetries,
						BackoffBase: b.BackoffBase,
						BackoffMax:  b.BackoffMax,
					})

					commitOp.End = time.Now()
					if !result.Success && result.Err != nil {
						commitOp.Err = result.Err.Error()
					}

					rcv <- commitOp
				}
			}
		}(i, workerFiles)
	}

	wg.Wait()
	return nil
}

func (b *Iceberg) Cleanup(ctx context.Context) {
	if b.Tree == nil {
		return
	}
	creator := &warpiceberg.DatasetCreator{
		Catalog:     b.Catalog,
		CatalogPool: b.CatalogPool,
		Tree:        b.Tree,
		CatalogURI:  b.CatalogURI,
		AccessKey:   b.AccessKey,
		SecretKey:       b.SecretKey,
		ExternalCatalog: b.ExternalCatalog,
		Concurrency: b.Concurrency,
	}
	creator.DeleteAll(ctx)
}

func (b *Iceberg) getWorkerFiles(workerID, filesPerWorker int) []string {
	start := workerID * filesPerWorker
	end := start + filesPerWorker
	if end > len(b.dataFiles) {
		end = len(b.dataFiles)
	}
	if start >= len(b.dataFiles) {
		start = workerID % len(b.dataFiles)
		end = start + 1
	}
	return b.dataFiles[start:end]
}

func parseTableLocation(location string) (bucket, prefix string) {
	loc := location
	loc = strings.TrimPrefix(loc, "s3://")
	loc = strings.TrimPrefix(loc, "s3a://")

	parts := strings.SplitN(loc, "/", 2)
	if len(parts) >= 1 {
		bucket = parts[0]
	}
	if len(parts) >= 2 {
		prefix = parts[1]
	}
	return bucket, prefix
}
