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
	"cmp"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/iceberg-go"
	catalogpkg "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	warpiceberg "github.com/minio/warp/pkg/iceberg"
)

type Iceberg struct {
	Common

	Catalog         *rest.Catalog
	CatalogPool     *warpiceberg.CatalogPool
	TreeConfig      warpiceberg.TreeConfig
	Tree            *warpiceberg.Tree
	CatalogURI      string
	AccessKey       string
	SecretKey       string
	ExternalCatalog warpiceberg.ExternalCatalogType

	NumFiles       int
	RowsPerFile    int
	CacheDir       string
	FilesPerCommit int

	UseTPCDS    bool
	ScaleFactor string
	TPCDSTable  string

	MaxRetries  int
	BackoffBase time.Duration
	BackoffMax  time.Duration
	SkipUpload  bool

	S3Hosts     []string
	S3AccessKey string
	S3SecretKey string
	S3TLS       bool
	s3Clients   []*minio.Client
	s3Counter   uint64

	dataFiles        []string
	preparedPaths    []string
	tables           []warpiceberg.TableInfo
	prefixes         map[string]struct{}
	tableLocations   map[string]string
	tableLocationsMu sync.RWMutex
}

func (b *Iceberg) getCatalog() *rest.Catalog {
	if b.CatalogPool != nil {
		return b.CatalogPool.Get()
	}
	return b.Catalog
}

func (b *Iceberg) getS3Client() (*minio.Client, func()) {
	if len(b.s3Clients) > 0 {
		idx := atomic.AddUint64(&b.s3Counter, 1) % uint64(len(b.s3Clients))
		return b.s3Clients[idx], func() {}
	}
	return b.Client()
}

func (b *Iceberg) getTableLocation(ctx context.Context, tbl warpiceberg.TableInfo) (string, error) {
	key := fmt.Sprintf("%s.%s", strings.Join(tbl.Namespace, "."), tbl.Name)

	b.tableLocationsMu.RLock()
	if loc, ok := b.tableLocations[key]; ok {
		b.tableLocationsMu.RUnlock()
		return loc, nil
	}
	b.tableLocationsMu.RUnlock()

	b.tableLocationsMu.Lock()
	defer b.tableLocationsMu.Unlock()

	if loc, ok := b.tableLocations[key]; ok {
		return loc, nil
	}

	ident := append([]string{}, tbl.Namespace...)
	ident = append(ident, tbl.Name)

	cat := b.getCatalog()
	loadedTbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return "", err
	}

	if b.tableLocations == nil {
		b.tableLocations = make(map[string]string)
	}
	b.tableLocations[key] = loadedTbl.Location()
	return loadedTbl.Location(), nil
}

func (b *Iceberg) prepareNonPrimaryClient(ctx context.Context) error {
	b.tables = b.Tree.AllTables()
	if len(b.tables) == 0 {
		return fmt.Errorf("no tables in tree config")
	}

	return b.prepareDataFiles(ctx)
}

func (b *Iceberg) Prepare(ctx context.Context) error {
	b.Tree = warpiceberg.NewTree(b.TreeConfig)

	if len(b.S3Hosts) > 0 {
		b.s3Clients = make([]*minio.Client, len(b.S3Hosts))
		for i, host := range b.S3Hosts {
			client, err := minio.New(host, &minio.Options{
				Creds:  miniocreds.NewStaticV4(b.S3AccessKey, b.S3SecretKey, ""),
				Secure: b.S3TLS,
			})
			if err != nil {
				return fmt.Errorf("failed to create S3 client for %s: %w", host, err)
			}
			b.s3Clients[i] = client
		}
	}

	if b.ClientIdx > 0 {
		return b.prepareNonPrimaryClient(ctx)
	}

	if b.UpdateStatus != nil {
		b.UpdateStatus("Creating dataset tree...")
	}

	creator := &warpiceberg.DatasetCreator{
		Catalog:         b.Catalog,
		CatalogPool:     b.CatalogPool,
		Tree:            b.Tree,
		CatalogURI:      b.CatalogURI,
		AccessKey:       b.AccessKey,
		SecretKey:       b.SecretKey,
		ExternalCatalog: b.ExternalCatalog,
		Concurrency:     b.Concurrency,
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
	if concurrency > 20 {
		concurrency = 20
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	var firstErr error
	var errMu sync.Mutex
	errCtx, cancel := context.WithCancel(ctx)
	defer cancel()

tableLoop:
	for i, tbl := range treeTables {
		select {
		case <-errCtx.Done():
			break tableLoop
		default:
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(i int, tbl warpiceberg.TableInfo) {
			defer wg.Done()
			defer func() { <-sem }()

			ident := append([]string{}, tbl.Namespace...)
			ident = append(ident, tbl.Name)

			cat := b.getCatalog()
			var loadedTbl *table.Table
			var err error
			if b.ExternalCatalog != warpiceberg.ExternalCatalogNone {
				loadedTbl, err = cat.CreateTable(errCtx, ident, schema,
					catalogpkg.WithLocation(tbl.Location),
				)
			} else {
				loadedTbl, err = cat.CreateTable(errCtx, ident, schema)
			}
			if err != nil {
				if !warpiceberg.IsAlreadyExists(err) {
					errMu.Lock()
					firstErr = cmp.Or(firstErr, fmt.Errorf("failed to create table %s: %w", tbl.Name, err))
					cancel()
					errMu.Unlock()
					return
				}
				loadedTbl, err = cat.LoadTable(errCtx, ident)
				if err != nil {
					errMu.Lock()
					firstErr = cmp.Or(firstErr, fmt.Errorf("failed to load table %s: %w", tbl.Name, err))
					cancel()
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

	return b.prepareDataFiles(ctx)
}

func (b *Iceberg) prepareDataFiles(ctx context.Context) error {
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

	if b.SkipUpload && b.ClientIdx == 0 {
		if err := b.uploadPreparedFiles(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (b *Iceberg) uploadPreparedFiles(ctx context.Context) error {
	if len(b.dataFiles) == 0 {
		return nil
	}

	bucket := b.TreeConfig.CatalogName
	prefix := "prepared"

	client, cldone := b.getS3Client()
	defer cldone()

	if b.UpdateStatus != nil {
		b.UpdateStatus("Uploading files for commit-only benchmark...")
	}

	b.preparedPaths = make([]string, 0, len(b.dataFiles))
	for i, localFile := range b.dataFiles {
		objName := fmt.Sprintf("%s/file-%d.parquet", prefix, i)

		f, err := os.Open(localFile)
		if err != nil {
			return fmt.Errorf("failed to open %s: %w", localFile, err)
		}

		info, _ := f.Stat()
		opts := b.PutOpts
		opts.ContentType = "application/octet-stream"

		_, err = client.PutObject(ctx, bucket, objName, f, info.Size(), opts)
		f.Close()
		if err != nil {
			return fmt.Errorf("failed to upload %s: %w", localFile, err)
		}

		b.preparedPaths = append(b.preparedPaths, fmt.Sprintf("s3://%s/%s", bucket, objName))

		if b.UpdateStatus != nil && (i+1)%10 == 0 {
			b.UpdateStatus(fmt.Sprintf("Uploaded %d/%d files...", i+1, len(b.dataFiles)))
		}
	}

	if b.UpdateStatus != nil {
		b.UpdateStatus(fmt.Sprintf("Uploaded %d files for commit-only benchmark", len(b.preparedPaths)))
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

	filesPerWorker := len(b.dataFiles) / b.Concurrency
	if filesPerWorker == 0 {
		filesPerWorker = 1
	}

	filesPerCommit := b.FilesPerCommit
	if filesPerCommit <= 0 {
		filesPerCommit = 1
	}

	if b.SkipUpload && len(b.preparedPaths) == 0 {
		if err := b.buildPreparedPaths(); err != nil {
			return err
		}
	}

	for i := 0; i < b.Concurrency; i++ {
		workerFiles := b.getWorkerFiles(i, filesPerWorker)

		go func(workerID int, files []string) {
			rcv := c.Receiver()
			defer wg.Done()

			done := ctx.Done()
			opCtx := context.Background()
			tableIdx := workerID
			fileIdx := workerID * filesPerCommit

			<-wait

			for iter := 0; ; iter++ {
				select {
				case <-done:
					return
				default:
				}

				tbl := b.tables[tableIdx%len(b.tables)]
				tableIdx++

				loc, err := b.getTableLocation(opCtx, tbl)
				if err != nil {
					continue
				}

				tableBucket, tablePrefix := parseTableLocation(loc)
				if tableBucket == "" {
					tableBucket = b.TreeConfig.CatalogName
				}

				if b.SkipUpload {
					if b.rpsLimit(ctx) != nil {
						return
					}

					numFiles := filesPerCommit
					if numFiles > len(b.preparedPaths) {
						numFiles = len(b.preparedPaths)
					}

					commitPaths := make([]string, 0, numFiles)
					for j := 0; j < numFiles; j++ {
						commitPaths = append(commitPaths, b.preparedPaths[fileIdx%len(b.preparedPaths)])
						fileIdx++
					}

					b.doCommit(opCtx, rcv, tbl, commitPaths, uint32(workerID))
				} else {
					prefix := fmt.Sprintf("%s/data/worker-%d", tablePrefix, workerID)

					var uploadedPaths []string
					for localFileIdx, localFile := range files {
						select {
						case <-done:
							return
						default:
						}

						if b.rpsLimit(ctx) != nil {
							return
						}

						client, cldone := b.getS3Client()
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

						isLastFile := localFileIdx == len(files)-1
						reachedCommitThreshold := len(uploadedPaths) >= filesPerCommit
						if len(uploadedPaths) > 0 && (reachedCommitThreshold || isLastFile) {
							select {
							case <-done:
								return
							default:
							}

							b.doCommit(opCtx, rcv, tbl, uploadedPaths, uint32(workerID))
							uploadedPaths = nil
						}
					}
				}
			}
		}(i, workerFiles)
	}

	wg.Wait()
	return nil
}

func (b *Iceberg) buildPreparedPaths() error {
	if len(b.dataFiles) == 0 {
		return fmt.Errorf("no data files for skip-upload mode")
	}

	bucket := b.TreeConfig.CatalogName
	prefix := "prepared"

	b.preparedPaths = make([]string, len(b.dataFiles))
	for i := range b.dataFiles {
		objName := fmt.Sprintf("%s/file-%d.parquet", prefix, i)
		b.preparedPaths[i] = fmt.Sprintf("s3://%s/%s", bucket, objName)
	}

	return nil
}

func (b *Iceberg) doCommit(ctx context.Context, rcv chan<- Operation, tbl warpiceberg.TableInfo, paths []string, workerID uint32) {
	client, cldone := b.Client()
	endpoint := client.EndpointURL().String()
	cldone()

	cat := b.Catalog
	if b.CatalogPool != nil {
		cat = b.CatalogPool.Get()
	}

	ident := catalogpkg.ToIdentifier(fmt.Sprintf("%s.%s", strings.Join(tbl.Namespace, "."), tbl.Name))
	loadedTbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		commitOp := Operation{
			OpType:   "COMMIT",
			Thread:   workerID,
			File:     fmt.Sprintf("%s/%s", strings.Join(tbl.Namespace, "."), tbl.Name),
			Endpoint: endpoint,
			Start:    time.Now(),
		}
		commitOp.End = commitOp.Start
		commitOp.Err = fmt.Sprintf("LoadTable failed: %s", err.Error())
		rcv <- commitOp
		return
	}

	commitOp := Operation{
		OpType:   "COMMIT",
		Thread:   workerID,
		File:     fmt.Sprintf("%s/%s", strings.Join(tbl.Namespace, "."), tbl.Name),
		Endpoint: endpoint,
	}
	commitOp.Start = time.Now()

	result := warpiceberg.CommitWithRetry(ctx, loadedTbl, paths, warpiceberg.CommitConfig{
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

func (b *Iceberg) Cleanup(ctx context.Context) {
	if b.Tree == nil || b.ClientIdx > 0 {
		return
	}

	if b.SkipUpload {
		client, cldone := b.getS3Client()
		bucket := b.TreeConfig.CatalogName
		prefix := "prepared"
		for obj := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix + "/", Recursive: true}) {
			if obj.Err != nil {
				continue
			}
			_ = client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{})
		}
		cldone()
	}

	creator := &warpiceberg.DatasetCreator{
		Catalog:         b.Catalog,
		CatalogPool:     b.CatalogPool,
		Tree:            b.Tree,
		CatalogURI:      b.CatalogURI,
		AccessKey:       b.AccessKey,
		SecretKey:       b.SecretKey,
		ExternalCatalog: b.ExternalCatalog,
		Concurrency:     b.Concurrency,
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
