package iceberg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"golang.org/x/sync/errgroup"
)

// GenerateConfig holds configuration for parquet file generation.
type GenerateConfig struct {
	OutputDir   string
	NumFiles    int
	RowsPerFile int
	Concurrency int
}

// GenerateResult holds the result of parquet generation.
type GenerateResult struct {
	Files      []string
	TotalRows  int64
	TotalBytes int64
}

// BenchmarkSchema returns the Arrow schema for benchmark data files.
// This schema is simple and avoids decimal types to ensure compatibility.
func BenchmarkSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "count", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "flag", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)
}

// GenerateParquetFiles generates test parquet files for benchmarking.
func GenerateParquetFiles(ctx context.Context, cfg GenerateConfig, progress func(completed, total int64)) (*GenerateResult, error) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.RowsPerFile <= 0 {
		cfg.RowsPerFile = 10000
	}

	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.Concurrency)

	var completed atomic.Int64
	var totalBytes atomic.Int64
	total := int64(cfg.NumFiles)
	files := make([]string, cfg.NumFiles)

	schema := BenchmarkSchema()

	for i := 0; i < cfg.NumFiles; i++ {
		i := i
		g.Go(func() error {
			filePath := filepath.Join(cfg.OutputDir, fmt.Sprintf("data-%05d.parquet", i))
			files[i] = filePath

			size, err := writeParquetFile(ctx, filePath, schema, cfg.RowsPerFile, i)
			if err != nil {
				return err
			}

			totalBytes.Add(size)
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

	return &GenerateResult{
		Files:      files,
		TotalRows:  int64(cfg.NumFiles * cfg.RowsPerFile),
		TotalBytes: totalBytes.Load(),
	}, nil
}

func writeParquetFile(_ context.Context, path string, schema *arrow.Schema, numRows, fileIdx int) (int64, error) {
	mem := memory.NewGoAllocator()

	ids := array.NewInt64Builder(mem)
	timestamps := array.NewInt64Builder(mem)
	values := array.NewFloat64Builder(mem)
	categories := array.NewStringBuilder(mem)
	counts := array.NewInt32Builder(mem)
	flags := array.NewBooleanBuilder(mem)

	defer ids.Release()
	defer timestamps.Release()
	defer values.Release()
	defer categories.Release()
	defer counts.Release()
	defer flags.Release()

	baseTime := time.Now().UnixNano()
	cats := []string{"A", "B", "C", "D", "E"}

	for i := 0; i < numRows; i++ {
		ids.Append(int64(fileIdx*numRows + i))
		timestamps.Append(baseTime + int64(i*1000))
		values.Append(float64(i) * 1.5)
		categories.Append(cats[i%len(cats)])
		counts.Append(int32(i % 1000))
		flags.Append(i%2 == 0)
	}

	idArr := ids.NewInt64Array()
	tsArr := timestamps.NewInt64Array()
	valArr := values.NewFloat64Array()
	catArr := categories.NewStringArray()
	cntArr := counts.NewInt32Array()
	flgArr := flags.NewBooleanArray()

	defer idArr.Release()
	defer tsArr.Release()
	defer valArr.Release()
	defer catArr.Release()
	defer cntArr.Release()
	defer flgArr.Release()

	record := array.NewRecord(schema, []arrow.Array{
		idArr, tsArr, valArr, catArr, cntArr, flgArr,
	}, int64(numRows))
	defer record.Release()

	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	writer, err := pqarrow.NewFileWriter(schema, f, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return 0, err
	}

	if err := writer.Write(record); err != nil {
		writer.Close()
		return 0, err
	}

	if err := writer.Close(); err != nil {
		return 0, err
	}

	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}
