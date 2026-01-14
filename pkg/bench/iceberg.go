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
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/minio/minio-go/v7"
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

	// Benchmark parameters
	Iterations  int
	MaxRetries  int
	BackoffBase time.Duration

	// Internal state
	dataFiles []string
	prefixes  map[string]struct{}
}

// Prepare generates test data and initializes the Iceberg catalog.
func (b *Iceberg) Prepare(ctx context.Context) error {
	// Create bucket if needed
	if err := b.createEmptyBucket(ctx); err != nil {
		return err
	}

	// Generate test parquet files
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

	// Load or create the table with benchmark schema
	schema := warpiceberg.BenchmarkDataSchema()
	_, err = warpiceberg.LoadOrCreateTable(ctx, cat, b.Namespace, b.TableName, schema)
	if err != nil {
		return fmt.Errorf("failed to load/create Iceberg table: %w", err)
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

	// Calculate files per worker
	filesPerWorker := len(b.dataFiles) / b.Concurrency
	if filesPerWorker == 0 {
		filesPerWorker = 1
	}

	// Stats tracking
	var totalUploads, totalCommits, totalRetries, totalFailedCommits atomic.Int64

	for i := 0; i < b.Concurrency; i++ {
		workerFiles := b.getWorkerFiles(i, filesPerWorker)
		prefix := fmt.Sprintf("iceberg/%s/worker-%d", b.TableName, i)
		b.prefixes[prefix] = struct{}{}

		go func(workerID int, files []string, prefix string) {
			rcv := c.Receiver()
			defer wg.Done()

			done := ctx.Done()
			<-wait

			for iter := 0; iter < b.Iterations; iter++ {
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
					op.Start = time.Now()

					_, err = client.PutObject(ctx, b.Bucket, objName, f, info.Size(),
						minio.PutObjectOptions{ContentType: "application/octet-stream"})
					op.End = time.Now()
					f.Close()

					if err != nil {
						op.Err = err.Error()
					} else {
						uploadedPaths = append(uploadedPaths, fmt.Sprintf("s3://%s/%s", b.Bucket, objName))
						totalUploads.Add(1)
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
						totalFailedCommits.Add(1)
						rcv <- commitOp
						continue
					}

					tbl, err := cat.LoadTable(ctx, catalog.ToIdentifier(fmt.Sprintf("%s.%s", b.Namespace, b.TableName)))
					if err != nil {
						commitOp.End = time.Now()
						commitOp.Err = err.Error()
						totalFailedCommits.Add(1)
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
						totalRetries.Add(int64(result.Retries))
					}

					if result.Success {
						totalCommits.Add(1)
					} else {
						totalFailedCommits.Add(1)
						if result.Err != nil {
							commitOp.Err = result.Err.Error()
							// Log first few errors for debugging
							if totalFailedCommits.Load() <= 3 {
								b.Error("Commit error: ", result.Err.Error())
							}
						}
					}

					rcv <- commitOp
				}
			}
		}(i, workerFiles, prefix)
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
	b.deleteAllInBucket(ctx, pf...)
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
