package rest

import (
	"context"
	"fmt"
	"net/url"
)

func (c *Client) CreateTable(ctx context.Context, catalog string, namespace []string, req CreateTableRequest) (*Table, error) {
	path := fmt.Sprintf("/%s/namespaces/%s/tables", catalog, encodeNamespace(namespace))

	resp, err := c.do(ctx, "POST", path, nil, req)
	if err != nil {
		return nil, err
	}

	var table Table
	if err := c.parseResponse(resp, &table); err != nil {
		return nil, err
	}

	return &table, nil
}

func (c *Client) GetTable(ctx context.Context, catalog string, namespace []string, tableName string) (*Table, error) {
	path := fmt.Sprintf("/%s/namespaces/%s/tables/%s", catalog, encodeNamespace(namespace), url.PathEscape(tableName))

	resp, err := c.do(ctx, "GET", path, nil, nil)
	if err != nil {
		return nil, err
	}

	var table Table
	if err := c.parseResponse(resp, &table); err != nil {
		return nil, err
	}

	return &table, nil
}

func (c *Client) TableExists(ctx context.Context, catalog string, namespace []string, tableName string) (bool, error) {
	path := fmt.Sprintf("/%s/namespaces/%s/tables/%s", catalog, encodeNamespace(namespace), url.PathEscape(tableName))

	resp, err := c.do(ctx, "HEAD", path, nil, nil)
	if err != nil {
		return false, err
	}

	if err := c.checkResponse(resp); err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (c *Client) ListTables(ctx context.Context, catalog string, namespace []string) (*ListTablesResponse, error) {
	path := fmt.Sprintf("/%s/namespaces/%s/tables", catalog, encodeNamespace(namespace))

	resp, err := c.do(ctx, "GET", path, nil, nil)
	if err != nil {
		return nil, err
	}

	var result ListTablesResponse
	if err := c.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *Client) UpdateTable(ctx context.Context, catalog string, namespace []string, tableName string, req CommitTableRequest) (*Table, error) {
	path := fmt.Sprintf("/%s/namespaces/%s/tables/%s", catalog, encodeNamespace(namespace), url.PathEscape(tableName))

	resp, err := c.do(ctx, "POST", path, nil, req)
	if err != nil {
		return nil, err
	}

	var table Table
	if err := c.parseResponse(resp, &table); err != nil {
		return nil, err
	}

	return &table, nil
}

func (c *Client) DropTable(ctx context.Context, catalog string, namespace []string, tableName string, purge bool) error {
	path := fmt.Sprintf("/%s/namespaces/%s/tables/%s", catalog, encodeNamespace(namespace), url.PathEscape(tableName))

	query := url.Values{}
	if purge {
		query.Set("purgeRequested", "true")
	}

	resp, err := c.do(ctx, "DELETE", path, query, nil)
	if err != nil {
		return err
	}

	return c.checkResponse(resp)
}

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
