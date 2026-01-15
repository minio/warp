/*
 * Warp (C) 2019-2024 MinIO, Inc.
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
	"sync/atomic"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	warpiceberg "github.com/minio/warp/pkg/iceberg"
)

// Iceberg benchmarks Iceberg table write performance with commit conflict handling.
type Iceberg struct {
	Common

	// Iceberg catalog configuration
	CatalogURI string
	Warehouse  string
	Namespace  string
	TableName  string

	// Data generation configuration
	NumFiles    int
	RowsPerFile int
	CacheDir    string

	// TPC-DS mode configuration
	UseTPCDS    bool
	ScaleFactor string
	TPCDSTable  string

	// Benchmark parameters
	MaxRetries  int
	BackoffBase time.Duration

	// Internal state
	dataFiles     []string
	prefixes      map[string]struct{}
	tableLocation string // Table location from catalog (e.g., s3://warehouse/table-uuid)

	// Stats tracking
	totalUploads       atomic.Int64
	totalCommits       atomic.Int64
	totalRetries       atomic.Int64
	totalFailedCommits atomic.Int64
}

// Prepare generates test data and initializes the Iceberg catalog.
func (b *Iceberg) Prepare(ctx context.Context) error {
	// Create bucket if needed
	// Ensure warehouse exists (auto-create if not)
	if b.UpdateStatus != nil {
		b.UpdateStatus("Ensuring warehouse exists...")
	}
	if err := warpiceberg.EnsureWarehouse(ctx, warpiceberg.CatalogConfig{
		CatalogURI: b.CatalogURI,
		Warehouse:  b.Warehouse,
		AccessKey:  b.getAccessKey(),
		SecretKey:  b.getSecretKey(),
		S3Endpoint: b.getS3Endpoint(),
	}); err != nil {
		b.Error("Note: warehouse creation returned: ", err)
	}

	if err := b.createEmptyBucket(ctx); err != nil {
		return err
	}

	// Get data files - either from TPC-DS or generate them
	if b.UseTPCDS {
		cfg := warpiceberg.TPCDSConfig{
			ScaleFactor: b.ScaleFactor,
			Table:       b.TPCDSTable,
			CacheDir:    b.CacheDir,
			Concurrency: b.Concurrency,
		}

		// Try cache first, download if not available
		files, err := warpiceberg.GetCachedTPCDSFiles(cfg)
		if err != nil {
			if b.UpdateStatus != nil {
				b.UpdateStatus(fmt.Sprintf("Downloading TPC-DS %s/%s from GCS...", b.ScaleFactor, b.TPCDSTable))
			}

			result, err := warpiceberg.DownloadTPCDS(ctx, cfg, func(completed, total, _ int64) {
				if b.PrepareProgress != nil {
					select {
					case b.PrepareProgress <- float64(completed) / float64(total):
					default:
					}
				}
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
			if b.PrepareProgress != nil {
				select {
				case b.PrepareProgress <- float64(completed) / float64(total):
				default:
				}
			}
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

	// Initialize Iceberg catalog and ensure table exists
	if b.UpdateStatus != nil {
		b.UpdateStatus("Initializing Iceberg catalog...")
	}

	cat, err := warpiceberg.NewCatalog(ctx, warpiceberg.CatalogConfig{
		CatalogURI: b.CatalogURI,
		Warehouse:  b.Warehouse,
		AccessKey:  b.getAccessKey(),
		SecretKey:  b.getSecretKey(),
		S3Endpoint: b.getS3Endpoint(),
	})
	if err != nil {
		return fmt.Errorf("failed to create Iceberg catalog: %w", err)
	}

	// Ensure namespace exists
	if err := warpiceberg.EnsureNamespace(ctx, cat, b.Namespace); err != nil {
		b.Error("Note: namespace creation returned: ", err)
	}

	// Load or create the table with appropriate schema
	var schema *iceberg.Schema
	if b.UseTPCDS {
		schema = warpiceberg.StoreSalesSchema()
	} else {
		schema = warpiceberg.BenchmarkDataSchema()
	}
	tbl, err := warpiceberg.LoadOrCreateTable(ctx, cat, b.Namespace, b.TableName, schema)
	if err != nil {
		return fmt.Errorf("failed to load/create Iceberg table: %w", err)
	}

	// Store the table location for uploading data files
	b.tableLocation = tbl.Location()
	if b.UpdateStatus != nil {
		b.UpdateStatus(fmt.Sprintf("Table location: %s", b.tableLocation))
	}

	return nil
}

// Start executes the Iceberg benchmark.
func (b *Iceberg) Start(ctx context.Context, wait chan struct{}) error {
	var wg sync.WaitGroup
	wg.Add(b.Concurrency)
	c := b.Collector

	if b.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, "ICEBERG", b.AutoTermScale, autoTermCheck, autoTermSamples, b.AutoTermDur)
	}

	b.prefixes = make(map[string]struct{}, b.Concurrency)

	// Parse table location to get bucket and table UUID prefix
	tableBucket, tablePrefix := b.parseTableLocation()
	if tableBucket == "" {
		tableBucket = b.Bucket // Fallback to configured bucket
	}

	// Calculate files per worker
	filesPerWorker := len(b.dataFiles) / b.Concurrency
	if filesPerWorker == 0 {
		filesPerWorker = 1
	}

	// Reset stats
	b.totalUploads.Store(0)
	b.totalCommits.Store(0)
	b.totalRetries.Store(0)
	b.totalFailedCommits.Store(0)

	for i := 0; i < b.Concurrency; i++ {
		workerFiles := b.getWorkerFiles(i, filesPerWorker)
		// Upload to {table-uuid}/data/worker-{N}/ inside the warehouse bucket
		prefix := fmt.Sprintf("%s/data/worker-%d", tablePrefix, i)
		b.prefixes[prefix] = struct{}{}

		go func(workerID int, files []string, prefix, bucket string) {
			rcv := c.Receiver()
			defer wg.Done()

			done := ctx.Done()
			<-wait

			for iter := 0; ; iter++ {
				select {
				case <-done:
					return
				default:
				}

				// Upload phase - upload all files for this worker
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
						ObjPerOp: 1,
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

					_, err = client.PutObject(ctx, bucket, objName, f, info.Size(), opts)
					op.End = time.Now()
					f.Close()

					if err != nil {
						op.Err = err.Error()
					} else {
						uploadedPaths = append(uploadedPaths, fmt.Sprintf("s3://%s/%s", bucket, objName))
						b.totalUploads.Add(1)
					}

					cldone()
					rcv <- op
				}

				// Commit phase - commit all uploaded files to Iceberg
				if len(uploadedPaths) > 0 {
					select {
					case <-done:
						return
					default:
					}

					commitOp := Operation{
						OpType:   "COMMIT",
						Thread:   uint32(workerID),
						ObjPerOp: len(uploadedPaths),
						File:     fmt.Sprintf("%s.%s", b.Namespace, b.TableName),
					}

					client, cldone := b.Client()
					commitOp.Endpoint = client.EndpointURL().String()
					cldone()

					commitOp.Start = time.Now()

					cat, err := warpiceberg.NewCatalog(ctx, warpiceberg.CatalogConfig{
						CatalogURI: b.CatalogURI,
						Warehouse:  b.Warehouse,
						AccessKey:  b.getAccessKey(),
						SecretKey:  b.getSecretKey(),
						S3Endpoint: b.getS3Endpoint(),
					})
					if err != nil {
						commitOp.End = time.Now()
						commitOp.Err = err.Error()
						b.totalFailedCommits.Add(1)
						rcv <- commitOp
						continue
					}

					tbl, err := cat.LoadTable(ctx, catalog.ToIdentifier(fmt.Sprintf("%s.%s", b.Namespace, b.TableName)))
					if err != nil {
						commitOp.End = time.Now()
						commitOp.Err = err.Error()
						b.totalFailedCommits.Add(1)
						rcv <- commitOp
						continue
					}

					result := warpiceberg.CommitWithRetry(ctx, tbl, uploadedPaths, warpiceberg.CommitConfig{
						MaxRetries:  b.MaxRetries,
						BackoffBase: b.BackoffBase,
						BackoffMax:  5 * time.Second,
					})

					commitOp.End = time.Now()
					if result.Retries > 0 {
						b.totalRetries.Add(int64(result.Retries))
					}

					if result.Success {
						b.totalCommits.Add(1)
					} else {
						b.totalFailedCommits.Add(1)
						if result.Err != nil {
							commitOp.Err = result.Err.Error()
							// Log first few errors for debugging
							if b.totalFailedCommits.Load() <= 3 {
								b.Error("Commit error: ", result.Err.Error())
							}
						}
					}

					rcv <- commitOp
				}
			}
		}(i, workerFiles, prefix, tableBucket)
	}

	wg.Wait()
	return nil
}

// Cleanup removes uploaded data from the bucket.
func (b *Iceberg) Cleanup(ctx context.Context) {
	pf := make([]string, 0, len(b.prefixes))
	for p := range b.prefixes {
		pf = append(pf, p)
	}

	// Use table bucket for cleanup (parsed from table location)
	tableBucket, _ := b.parseTableLocation()
	if tableBucket == "" {
		tableBucket = b.Bucket
	}

	// Temporarily set bucket to table bucket for cleanup
	origBucket := b.Bucket
	b.Bucket = tableBucket
	b.deleteAllInBucket(ctx, pf...)
	b.Bucket = origBucket
}

// StatsSummary returns iceberg-specific statistics.
func (b *Iceberg) StatsSummary() map[string]string {
	return map[string]string{
		"Total Uploads":        fmt.Sprintf("%d", b.totalUploads.Load()),
		"Total Commits":        fmt.Sprintf("%d", b.totalCommits.Load()),
		"Total Retries":        fmt.Sprintf("%d", b.totalRetries.Load()),
		"Total Failed Commits": fmt.Sprintf("%d", b.totalFailedCommits.Load()),
	}
}

// getWorkerFiles returns the subset of files for a specific worker.
func (b *Iceberg) getWorkerFiles(workerID, filesPerWorker int) []string {
	start := workerID * filesPerWorker
	end := start + filesPerWorker
	if end > len(b.dataFiles) {
		end = len(b.dataFiles)
	}
	if start >= len(b.dataFiles) {
		// Wrap around if we have more workers than files
		start = workerID % len(b.dataFiles)
		end = start + 1
	}
	return b.dataFiles[start:end]
}

// getAccessKey extracts access key from the client.
func (b *Iceberg) getAccessKey() string {
	if v, ok := b.ExtraFlags["access-key"]; ok {
		return v
	}
	cl, done := b.Client()
	defer done()
	return cl.EndpointURL().User.Username()
}

// getSecretKey extracts secret key from the client.
func (b *Iceberg) getSecretKey() string {
	if v, ok := b.ExtraFlags["secret-key"]; ok {
		return v
	}
	cl, done := b.Client()
	defer done()
	if p, ok := cl.EndpointURL().User.Password(); ok {
		return p
	}
	return ""
}

// getS3Endpoint returns the S3 endpoint URL.
func (b *Iceberg) getS3Endpoint() string {
	cl, done := b.Client()
	defer done()
	return cl.EndpointURL().Scheme + "://" + cl.EndpointURL().Host
}

// parseTableLocation extracts bucket and prefix from table location.
// e.g., "s3://my-warehouse/abc-123-uuid" -> ("my-warehouse", "abc-123-uuid")
func (b *Iceberg) parseTableLocation() (bucket, prefix string) {
	loc := b.tableLocation
	// Remove s3:// prefix
	loc = strings.TrimPrefix(loc, "s3://")
	loc = strings.TrimPrefix(loc, "s3a://")

	// Split into bucket and path
	parts := strings.SplitN(loc, "/", 2)
	if len(parts) >= 1 {
		bucket = parts[0]
	}
	if len(parts) >= 2 {
		prefix = parts[1]
	}
	return bucket, prefix
}
