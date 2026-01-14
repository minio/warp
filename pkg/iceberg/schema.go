package iceberg

import (
	"github.com/apache/iceberg-go"
)

// BenchmarkDataSchema returns the Iceberg schema for benchmark data files.
// This matches the Arrow schema in BenchmarkSchema() in parquet.go.
func BenchmarkDataSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       1,
			Name:     "id",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
		},
		iceberg.NestedField{
			ID:       2,
			Name:     "timestamp",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
		},
		iceberg.NestedField{
			ID:       3,
			Name:     "value",
			Type:     iceberg.PrimitiveTypes.Float64,
			Required: false,
		},
		iceberg.NestedField{
			ID:       4,
			Name:     "category",
			Type:     iceberg.PrimitiveTypes.String,
			Required: false,
		},
		iceberg.NestedField{
			ID:       5,
			Name:     "count",
			Type:     iceberg.PrimitiveTypes.Int32,
			Required: false,
		},
		iceberg.NestedField{
			ID:       6,
			Name:     "flag",
			Type:     iceberg.PrimitiveTypes.Bool,
			Required: false,
		},
	)
}
