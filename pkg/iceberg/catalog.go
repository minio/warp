// Package iceberg provides utilities for interacting with Iceberg catalogs.
package iceberg

import (
	"context"
	"fmt"
	"net/url"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// CatalogConfig holds configuration for connecting to an Iceberg catalog.
type CatalogConfig struct {
	CatalogURI string
	Warehouse  string
	AccessKey  string
	SecretKey  string
	Region     string
}

// NewCatalog creates a new Iceberg REST catalog connection.
func NewCatalog(ctx context.Context, cfg CatalogConfig) (catalog.Catalog, error) {
	u, err := url.Parse(cfg.CatalogURI)
	if err != nil {
		return nil, fmt.Errorf("invalid catalog URI: %w", err)
	}

	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}

	awsCfg := aws.Config{
		Region: cfg.Region,
		Credentials: credentials.NewStaticCredentialsProvider(
			cfg.AccessKey,
			cfg.SecretKey,
			"",
		),
	}

	opts := []rest.Option{
		rest.WithWarehouseLocation(cfg.Warehouse),
		rest.WithAwsConfig(awsCfg),
		rest.WithSigV4RegionSvc(cfg.Region, "s3tables"),
	}

	cat, err := rest.NewCatalog(ctx, "rest", u.String(), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog: %w", err)
	}

	return cat, nil
}

// LoadOrCreateTable loads an existing table or creates it if it doesn't exist.
func LoadOrCreateTable(ctx context.Context, cat catalog.Catalog, namespace, tableName string, schema *iceberg.Schema) (*table.Table, error) {
	ident := catalog.ToIdentifier(fmt.Sprintf("%s.%s", namespace, tableName))

	tbl, err := cat.LoadTable(ctx, ident)
	if err == nil {
		return tbl, nil
	}

	// Table doesn't exist, create it
	tbl, err = cat.CreateTable(ctx, ident, schema,
		catalog.WithProperties(iceberg.Properties{
			"format-version": "2",
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create table %s.%s: %w", namespace, tableName, err)
	}

	return tbl, nil
}

// EnsureNamespace creates the namespace if it doesn't exist.
func EnsureNamespace(ctx context.Context, cat catalog.Catalog, namespace string) error {
	ident := catalog.ToIdentifier(namespace)
	_, err := cat.LoadNamespaceProperties(ctx, ident)
	if err == nil {
		return nil
	}

	// Namespace doesn't exist, create it
	return cat.CreateNamespace(ctx, ident, nil)
}
