package iceberg

import (
	"github.com/apache/iceberg-go"
)

// ResultsSchema returns the Iceberg schema for benchmark results.
func ResultsSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       1,
			Name:     "node_id",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
		iceberg.NestedField{
			ID:       2,
			Name:     "op_type",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
		iceberg.NestedField{
			ID:       3,
			Name:     "start_time",
			Type:     iceberg.PrimitiveTypes.TimestampTz,
			Required: true,
		},
		iceberg.NestedField{
			ID:       4,
			Name:     "end_time",
			Type:     iceberg.PrimitiveTypes.TimestampTz,
			Required: true,
		},
		iceberg.NestedField{
			ID:       5,
			Name:     "duration_ns",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
		},
		iceberg.NestedField{
			ID:       6,
			Name:     "bytes",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: false,
		},
		iceberg.NestedField{
			ID:       7,
			Name:     "file_path",
			Type:     iceberg.PrimitiveTypes.String,
			Required: false,
		},
		iceberg.NestedField{
			ID:       8,
			Name:     "success",
			Type:     iceberg.PrimitiveTypes.Bool,
			Required: true,
		},
		iceberg.NestedField{
			ID:       9,
			Name:     "error_msg",
			Type:     iceberg.PrimitiveTypes.String,
			Required: false,
		},
		iceberg.NestedField{
			ID:       10,
			Name:     "iteration",
			Type:     iceberg.PrimitiveTypes.Int32,
			Required: true,
		},
		iceberg.NestedField{
			ID:       11,
			Name:     "worker_id",
			Type:     iceberg.PrimitiveTypes.Int32,
			Required: true,
		},
	)
}
