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
