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

// Package iceberg provides utilities for interacting with Iceberg catalogs.
package iceberg

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync/atomic"
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
func NewCatalog(ctx context.Context, cfg CatalogConfig) (*rest.Catalog, error) {
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

// EnsureWarehouse creates the warehouse if it doesn't exist.
// This uses the MinIO AIStor Tables API directly since the standard
// Iceberg REST catalog API doesn't support warehouse creation.
func EnsureWarehouse(ctx context.Context, cfg CatalogConfig) error {
	u, err := url.Parse(cfg.CatalogURI)
	if err != nil {
		return fmt.Errorf("invalid catalog URI: %w", err)
	}

	// Build the warehouses endpoint URL
	warehousesURL := fmt.Sprintf("%s://%s/_iceberg/v1/warehouses", u.Scheme, u.Host)

	// Create warehouse request
	reqBody := struct {
		Name string `json:"name"`
	}{
		Name: cfg.Warehouse,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, warehousesURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Sign the request with AWS SigV4
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	signRequest(req, body, cfg.AccessKey, cfg.SecretKey, cfg.Region)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create warehouse: %w", err)
	}
	defer resp.Body.Close()

	// Read response body for error details
	respBody, _ := io.ReadAll(resp.Body)

	// 200/201 = success, 409 = already exists (also success for us)
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated ||
		resp.StatusCode == http.StatusConflict {
		return nil
	}

	return fmt.Errorf("failed to create warehouse %s: %s - %s", cfg.Warehouse, resp.Status, string(respBody))
}

// DeleteWarehouse deletes a warehouse.
func DeleteWarehouse(ctx context.Context, cfg CatalogConfig) error {
	u, err := url.Parse(cfg.CatalogURI)
	if err != nil {
		return fmt.Errorf("invalid catalog URI: %w", err)
	}

	warehouseURL := fmt.Sprintf("%s://%s/_iceberg/v1/warehouses/%s", u.Scheme, u.Host, cfg.Warehouse)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, warehouseURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	signRequest(req, nil, cfg.AccessKey, cfg.SecretKey, cfg.Region)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete warehouse: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent ||
		resp.StatusCode == http.StatusNotFound {
		return nil
	}

	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("failed to delete warehouse %s: %s - %s", cfg.Warehouse, resp.Status, string(respBody))
}

// signRequest signs an HTTP request using AWS SigV4.
func signRequest(req *http.Request, payload []byte, accessKey, secretKey, region string) {
	t := time.Now().UTC()
	amzDate := t.Format("20060102T150405Z")
	dateStamp := t.Format("20060102")

	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("Host", req.URL.Host)

	// Create canonical request
	payloadHash := sha256Hash(payload)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signedHeaders := "content-type;host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := fmt.Sprintf("content-type:%s\nhost:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		req.Header.Get("Content-Type"), req.URL.Host, payloadHash, amzDate)

	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.Path,
		req.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	// Create string to sign
	service := "s3tables"
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sha256Hash([]byte(canonicalRequest)),
	}, "\n")

	// Calculate signature
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	signature := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))

	// Add authorization header
	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, credentialScope, signedHeaders, signature)
	req.Header.Set("Authorization", authHeader)
}

func sha256Hash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// Keep sort import used
var _ = sort.Strings

// CatalogPool provides round-robin access to multiple catalog clients.
type CatalogPool struct {
	catalogs []*rest.Catalog
	urls     []string
	counter  atomic.Uint64
}

// NewCatalogPool creates a pool of catalog clients for round-robin access.
// It takes a base config and a list of catalog URLs, creating one client per URL.
func NewCatalogPool(ctx context.Context, catalogURLs []string, baseCfg CatalogConfig) (*CatalogPool, error) {
	if len(catalogURLs) == 0 {
		return nil, fmt.Errorf("no catalog URLs provided")
	}

	pool := &CatalogPool{
		catalogs: make([]*rest.Catalog, len(catalogURLs)),
		urls:     catalogURLs,
	}

	for i, catalogURL := range catalogURLs {
		cfg := baseCfg
		cfg.CatalogURI = catalogURL
		cat, err := NewCatalog(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create catalog for %s: %w", catalogURL, err)
		}
		pool.catalogs[i] = cat
	}

	return pool, nil
}

// Get returns the next catalog client in round-robin order.
func (p *CatalogPool) Get() *rest.Catalog {
	idx := p.counter.Add(1) - 1
	return p.catalogs[idx%uint64(len(p.catalogs))]
}

// First returns the first catalog client (for setup/cleanup operations).
func (p *CatalogPool) First() *rest.Catalog {
	return p.catalogs[0]
}

// Len returns the number of catalogs in the pool.
func (p *CatalogPool) Len() int {
	return len(p.catalogs)
}

// URLs returns the catalog URLs.
func (p *CatalogPool) URLs() []string {
	return p.urls
}
