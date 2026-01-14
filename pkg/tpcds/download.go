// Package tpcds provides utilities for downloading TPC-DS datasets.
package tpcds

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// Default GCS settings for TPC-DS data.
const (
	DefaultGCSBucket = "beam-tpcds"
	DefaultGCSPrefix = "datasets/parquet/nonpartitioned"
)

// DownloadConfig holds configuration for TPC-DS data download.
type DownloadConfig struct {
	GCSBucket   string
	GCSPrefix   string
	ScaleFactor string // 1GB, 10GB, 100GB, 1000GB
	Table       string // store_sales, store_returns, catalog_sales, etc.
	CacheDir    string
	Concurrency int
}

// DownloadResult holds the result of a download operation.
type DownloadResult struct {
	Files      []string
	TotalBytes int64
	FromCache  bool
}

// ProgressFunc is called with download progress updates.
type ProgressFunc func(completed, total int64)

// Download downloads TPC-DS parquet files from GCS to local cache.
// Returns cached files if they already exist.
func Download(ctx context.Context, cfg DownloadConfig, progress ProgressFunc) (*DownloadResult, error) {
	if cfg.GCSBucket == "" {
		cfg.GCSBucket = DefaultGCSBucket
	}
	if cfg.GCSPrefix == "" {
		cfg.GCSPrefix = DefaultGCSPrefix
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 16
	}

	localDir := filepath.Join(cfg.CacheDir, cfg.ScaleFactor, cfg.Table)

	// Check if data already exists in cache
	existingFiles, err := filepath.Glob(filepath.Join(localDir, "*.parquet"))
	if err == nil && len(existingFiles) > 0 {
		var totalBytes int64
		for _, f := range existingFiles {
			if info, err := os.Stat(f); err == nil {
				totalBytes += info.Size()
			}
		}
		return &DownloadResult{
			Files:      existingFiles,
			TotalBytes: totalBytes,
			FromCache:  true,
		}, nil
	}

	// Create cache directory
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Create GCS client (anonymous access for public bucket)
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer client.Close()

	bucket := client.Bucket(cfg.GCSBucket)
	prefix := fmt.Sprintf("%s/%s/%s/", cfg.GCSPrefix, cfg.ScaleFactor, cfg.Table)

	// List files to download
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
		return nil, fmt.Errorf("no parquet files found at gs://%s/%s", cfg.GCSBucket, prefix)
	}

	// Download with concurrency
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.Concurrency)

	var completed atomic.Int64
	var totalBytes atomic.Int64
	total := int64(len(gcsFiles))
	localFiles := make([]string, len(gcsFiles))

	for i, gcsFile := range gcsFiles {
		i, gcsFile := i, gcsFile
		g.Go(func() error {
			localPath := filepath.Join(localDir, filepath.Base(gcsFile))
			localFiles[i] = localPath

			// Skip if already downloaded
			if info, err := os.Stat(localPath); err == nil && info.Size() > 0 {
				totalBytes.Add(info.Size())
				n := completed.Add(1)
				if progress != nil {
					progress(n, total)
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

			totalBytes.Add(written)
			n := completed.Add(1)
			if progress != nil {
				progress(n, total)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &DownloadResult{
		Files:      localFiles,
		TotalBytes: totalBytes.Load(),
		FromCache:  false,
	}, nil
}

// ValidScaleFactors returns the list of valid TPC-DS scale factors.
func ValidScaleFactors() []string {
	return []string{"1GB", "10GB", "100GB", "1000GB"}
}

// ValidTables returns the list of valid TPC-DS tables.
func ValidTables() []string {
	return []string{
		"store_sales",
		"store_returns",
		"catalog_sales",
		"catalog_returns",
		"web_sales",
		"web_returns",
		"inventory",
		"customer",
		"customer_address",
		"customer_demographics",
		"item",
		"store",
		"warehouse",
	}
}
