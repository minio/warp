// Package iceberg provides utilities for interacting with Iceberg catalogs.
package iceberg

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

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
	S3Endpoint string // S3 endpoint for file IO (e.g., http://localhost:9000)
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

	// S3 properties for file IO - these are used when reading parquet files
	s3Props := iceberg.Properties{
		"s3.access-key-id":     cfg.AccessKey,
		"s3.secret-access-key": cfg.SecretKey,
		"s3.region":            cfg.Region,
	}
	if cfg.S3Endpoint != "" {
		s3Props["s3.endpoint"] = cfg.S3Endpoint
	}

	opts := []rest.Option{
		rest.WithWarehouseLocation(cfg.Warehouse),
		rest.WithAwsConfig(awsCfg),
		rest.WithSigV4RegionSvc(cfg.Region, "s3tables"),
		rest.WithAdditionalProps(s3Props),
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

	// Try to load existing table first
	tbl, err := cat.LoadTable(ctx, ident)
	if err == nil {
		return tbl, nil
	}

	// Check if table doesn't exist - try to create it
	errStr := err.Error()
	isNotFound := errors.Is(err, catalog.ErrNoSuchTable) ||
		strings.Contains(errStr, "NoSuchTable") ||
		strings.Contains(errStr, "not found") ||
		strings.Contains(errStr, "does not exist")

	if !isNotFound {
		return nil, fmt.Errorf("failed to load table %s.%s: %w", namespace, tableName, err)
	}

	// Table doesn't exist, create it
	tbl, err = cat.CreateTable(ctx, ident, schema,
		catalog.WithProperties(iceberg.Properties{
			"format-version": "2",
		}),
	)
	if err == nil {
		return tbl, nil
	}

	// If table already exists (race condition), retry loading with backoff
	createErrStr := err.Error()
	isAlreadyExists := errors.Is(err, catalog.ErrTableAlreadyExists) ||
		strings.Contains(createErrStr, "AlreadyExists") ||
		strings.Contains(createErrStr, "already exists")

	if !isAlreadyExists {
		return nil, fmt.Errorf("failed to create table %s.%s: %w", namespace, tableName, err)
	}

	// Retry loading with exponential backoff
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(100*(1<<attempt)) * time.Millisecond)
		}
		tbl, lastErr = cat.LoadTable(ctx, ident)
		if lastErr == nil {
			return tbl, nil
		}
	}

	// Table is in a corrupted state - try to drop and recreate
	_ = cat.DropTable(ctx, ident)
	time.Sleep(500 * time.Millisecond)

	tbl, err = cat.CreateTable(ctx, ident, schema,
		catalog.WithProperties(iceberg.Properties{
			"format-version": "2",
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to recreate table %s.%s after drop: %w", namespace, tableName, err)
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
