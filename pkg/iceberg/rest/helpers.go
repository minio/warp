package rest

import (
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/view"
)

func BuildTableSchema(numColumns int) Schema {
	fields := make([]SchemaField, numColumns)
	for i := 0; i < numColumns; i++ {
		fields[i] = SchemaField{
			ID:       i + 1,
			Name:     fmt.Sprintf("column%d", i+1),
			Type:     "string",
			Required: false,
		}
	}

	return Schema{
		Type:   "struct",
		Fields: fields,
	}
}

func BuildTableProperties(numProperties int, prefix string) map[string]string {
	props := make(map[string]string, numProperties)
	for i := 0; i < numProperties; i++ {
		props[fmt.Sprintf("%s_%d", prefix, i)] = fmt.Sprintf("value_%d", i)
	}
	return props
}

func BuildViewSchema(numColumns int) Schema {
	fields := make([]SchemaField, numColumns)
	for i := 0; i < numColumns; i++ {
		fields[i] = SchemaField{
			ID:       i + 1,
			Name:     fmt.Sprintf("column%d", i+1),
			Type:     "string",
			Required: false,
		}
	}

	return Schema{
		Type:   "struct",
		Fields: fields,
	}
}

func BuildViewVersion(catalog string, namespace []string, viewName string) ViewVersionCreate {
	return ViewVersionCreate{
		Summary: map[string]string{
			"engine-name":    "warp-benchmark",
			"engine-version": "1.0",
		},
		Representations: []ViewRepresentation{
			{
				Type:    "sql",
				SQL:     fmt.Sprintf("SELECT * FROM %s", viewName),
				Dialect: "spark",
			},
		},
		DefaultCatalog:   catalog,
		DefaultNamespace: namespace,
	}
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
