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

package rest

import (
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/view"
)

func BuildTableProperties(numProperties int, prefix string) map[string]string {
	props := make(map[string]string, numProperties)
	for i := 0; i < numProperties; i++ {
		props[fmt.Sprintf("%s_%d", prefix, i)] = fmt.Sprintf("value_%d", i)
	}
	return props
}

func BuildIcebergSchema(numColumns int) *iceberg.Schema {
	fields := make([]iceberg.NestedField, numColumns)
	for i := 0; i < numColumns; i++ {
		fields[i] = iceberg.NestedField{
			ID:       i + 1,
			Name:     fmt.Sprintf("column%d", i+1),
			Type:     iceberg.PrimitiveTypes.String,
			Required: false,
		}
	}
	return iceberg.NewSchema(0, fields...)
}

func BuildIcebergViewVersion(namespace []string, viewName string) *view.Version {
	v, _ := view.NewVersionFromSQL(
		1,
		0,
		fmt.Sprintf("SELECT * FROM %s", viewName),
		namespace,
	)
	return v
}
