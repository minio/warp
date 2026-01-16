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
