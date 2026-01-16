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

package iceberg

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"cloud.google.com/go/storage"
	"github.com/apache/iceberg-go"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// TPC-DS data source constants for GCS.
const (
	TPCDSGCSBucket = "beam-tpcds"
	TPCDSGCSPrefix = "datasets/parquet/nonpartitioned"
)

// scaleFactorMap maps user-friendly names to actual GCS folder names.
var scaleFactorMap = map[string]string{
	"sf1":    "1GB",
	"sf10":   "10GB",
	"sf100":  "100GB",
	"sf1000": "1000GB",
	"1GB":    "1GB",
	"10GB":   "10GB",
	"100GB":  "100GB",
	"1000GB": "1000GB",
}

// TPCDSConfig holds configuration for TPC-DS data operations.
type TPCDSConfig struct {
	ScaleFactor string // e.g., "sf1", "sf10", "sf100", "sf1000"
	Table       string // e.g., "store_sales", "customer", etc.
	CacheDir    string // local directory to cache downloaded files
	Concurrency int    // download concurrency
}

// DefaultTPCDSConfig returns sensible defaults.
func DefaultTPCDSConfig() TPCDSConfig {
	return TPCDSConfig{
		ScaleFactor: "sf100",
		Table:       "store_sales",
		CacheDir:    "/tmp/warp-tpcds-cache",
		Concurrency: 16,
	}
}

// TPCDSDownloadResult holds the result of a download operation.
type TPCDSDownloadResult struct {
	Files           []string
	TotalBytes      int64
	DownloadedBytes int64
	SkippedFiles    int
}

// DownloadTPCDS downloads TPC-DS parquet files from GCS to local cache.
// Files that already exist locally are skipped.
func DownloadTPCDS(ctx context.Context, cfg TPCDSConfig, progress func(completed, total, bytes int64)) (*TPCDSDownloadResult, error) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 16
	}

	// Map scale factor to GCS folder name
	gcsScaleFactor := cfg.ScaleFactor
	if mapped, ok := scaleFactorMap[cfg.ScaleFactor]; ok {
		gcsScaleFactor = mapped
	}

	localDir := filepath.Join(cfg.CacheDir, cfg.ScaleFactor, cfg.Table)

	existingFiles, _ := filepath.Glob(filepath.Join(localDir, "*.parquet"))
	if len(existingFiles) > 0 {
		var totalSize int64
		for _, f := range existingFiles {
			if info, err := os.Stat(f); err == nil {
				totalSize += info.Size()
			}
		}
		return &TPCDSDownloadResult{
			Files:        existingFiles,
			TotalBytes:   totalSize,
			SkippedFiles: len(existingFiles),
		}, nil
	}

	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer client.Close()

	bucket := client.Bucket(TPCDSGCSBucket)
	prefix := fmt.Sprintf("%s/%s/%s/", TPCDSGCSPrefix, gcsScaleFactor, cfg.Table)

	var gcsFiles []string
	it := bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list GCS objects: %w", err)
		}
		if strings.HasSuffix(attrs.Name, ".parquet") {
			gcsFiles = append(gcsFiles, attrs.Name)
		}
	}

	if len(gcsFiles) == 0 {
		return nil, fmt.Errorf("no parquet files found at gs://%s/%s", TPCDSGCSBucket, prefix)
	}

	if err := os.MkdirAll(localDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.Concurrency)

	var completed atomic.Int64
	var downloadedBytes atomic.Int64
	var skipped atomic.Int64
	total := int64(len(gcsFiles))
	localFiles := make([]string, len(gcsFiles))

	for i, gcsFile := range gcsFiles {
		i, gcsFile := i, gcsFile
		g.Go(func() error {
			localPath := filepath.Join(localDir, filepath.Base(gcsFile))
			localFiles[i] = localPath

			if info, err := os.Stat(localPath); err == nil && info.Size() > 0 {
				skipped.Add(1)
				n := completed.Add(1)
				if progress != nil {
					progress(n, total, downloadedBytes.Load())
				}
				return nil
			}

			obj := bucket.Object(gcsFile)
			reader, err := obj.NewReader(ctx)
			if err != nil {
				return fmt.Errorf("failed to read %s: %w", gcsFile, err)
			}
			defer reader.Close()

			f, err := os.Create(localPath)
			if err != nil {
				return fmt.Errorf("failed to create %s: %w", localPath, err)
			}
			defer f.Close()

			written, err := io.Copy(f, reader)
			if err != nil {
				os.Remove(localPath)
				return fmt.Errorf("failed to download %s: %w", gcsFile, err)
			}

			downloadedBytes.Add(written)
			n := completed.Add(1)
			if progress != nil {
				progress(n, total, downloadedBytes.Load())
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var totalSize int64
	for _, f := range localFiles {
		if info, err := os.Stat(f); err == nil {
			totalSize += info.Size()
		}
	}

	return &TPCDSDownloadResult{
		Files:           localFiles,
		TotalBytes:      totalSize,
		DownloadedBytes: downloadedBytes.Load(),
		SkippedFiles:    int(skipped.Load()),
	}, nil
}

// GetCachedTPCDSFiles returns locally cached TPC-DS files if they exist.
func GetCachedTPCDSFiles(cfg TPCDSConfig) ([]string, error) {
	localDir := filepath.Join(cfg.CacheDir, cfg.ScaleFactor, cfg.Table)
	files, err := filepath.Glob(filepath.Join(localDir, "*.parquet"))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no cached TPC-DS files found in %s", localDir)
	}
	return files, nil
}

// StoreSalesSchema returns the Iceberg schema for TPC-DS store_sales table.
func StoreSalesSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "ss_sold_date_sk", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 2, Name: "ss_sold_time_sk", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 3, Name: "ss_item_sk", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 4, Name: "ss_customer_sk", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 5, Name: "ss_cdemo_sk", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 6, Name: "ss_hdemo_sk", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 7, Name: "ss_addr_sk", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 8, Name: "ss_store_sk", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 9, Name: "ss_promo_sk", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 10, Name: "ss_ticket_number", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 11, Name: "ss_quantity", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 12, Name: "ss_wholesale_cost", Type: iceberg.DecimalTypeOf(7, 2), Required: false},
		iceberg.NestedField{ID: 13, Name: "ss_list_price", Type: iceberg.DecimalTypeOf(7, 2), Required: false},
		iceberg.NestedField{ID: 14, Name: "ss_sales_price", Type: iceberg.DecimalTypeOf(7, 2), Required: false},
		iceberg.NestedField{ID: 15, Name: "ss_ext_discount_amt", Type: iceberg.DecimalTypeOf(7, 2), Required: false},
		iceberg.NestedField{ID: 16, Name: "ss_ext_sales_price", Type: iceberg.DecimalTypeOf(7, 2), Required: false},
		iceberg.NestedField{ID: 17, Name: "ss_ext_wholesale_cost", Type: iceberg.DecimalTypeOf(7, 2), Required: false},
		iceberg.NestedField{ID: 18, Name: "ss_ext_list_price", Type: iceberg.DecimalTypeOf(7, 2), Required: false},
		iceberg.NestedField{ID: 19, Name: "ss_ext_tax", Type: iceberg.DecimalTypeOf(7, 2), Required: false},
		iceberg.NestedField{ID: 20, Name: "ss_coupon_amt", Type: iceberg.DecimalTypeOf(7, 2), Required: false},
		iceberg.NestedField{ID: 21, Name: "ss_net_paid", Type: iceberg.DecimalTypeOf(7, 2), Required: false},
		iceberg.NestedField{ID: 22, Name: "ss_net_paid_inc_tax", Type: iceberg.DecimalTypeOf(7, 2), Required: false},
		iceberg.NestedField{ID: 23, Name: "ss_net_profit", Type: iceberg.DecimalTypeOf(7, 2), Required: false},
	)
}
