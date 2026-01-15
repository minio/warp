package rest

import (
	"context"
	"fmt"
	"net/url"
)

func (c *Client) CreateView(ctx context.Context, catalog string, namespace []string, req CreateViewRequest) (*View, error) {
	path := fmt.Sprintf("/%s/namespaces/%s/views", catalog, encodeNamespace(namespace))

	resp, err := c.do(ctx, "POST", path, nil, req)
	if err != nil {
		return nil, err
	}

	var view View
	if err := c.parseResponse(resp, &view); err != nil {
		return nil, err
	}

	return &view, nil
}

func (c *Client) GetView(ctx context.Context, catalog string, namespace []string, viewName string) (*View, error) {
	path := fmt.Sprintf("/%s/namespaces/%s/views/%s", catalog, encodeNamespace(namespace), url.PathEscape(viewName))

	resp, err := c.do(ctx, "GET", path, nil, nil)
	if err != nil {
		return nil, err
	}

	var view View
	if err := c.parseResponse(resp, &view); err != nil {
		return nil, err
	}

	return &view, nil
}

func (c *Client) ViewExists(ctx context.Context, catalog string, namespace []string, viewName string) (bool, error) {
	path := fmt.Sprintf("/%s/namespaces/%s/views/%s", catalog, encodeNamespace(namespace), url.PathEscape(viewName))

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

func (c *Client) ListViews(ctx context.Context, catalog string, namespace []string) (*ListViewsResponse, error) {
	path := fmt.Sprintf("/%s/namespaces/%s/views", catalog, encodeNamespace(namespace))

	resp, err := c.do(ctx, "GET", path, nil, nil)
	if err != nil {
		return nil, err
	}

	var result ListViewsResponse
	if err := c.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *Client) UpdateView(ctx context.Context, catalog string, namespace []string, viewName string, req CommitViewRequest) (*View, error) {
	path := fmt.Sprintf("/%s/namespaces/%s/views/%s", catalog, encodeNamespace(namespace), url.PathEscape(viewName))

	resp, err := c.do(ctx, "POST", path, nil, req)
	if err != nil {
		return nil, err
	}

	var view View
	if err := c.parseResponse(resp, &view); err != nil {
		return nil, err
	}

	return &view, nil
}

func (c *Client) DropView(ctx context.Context, catalog string, namespace []string, viewName string) error {
	path := fmt.Sprintf("/%s/namespaces/%s/views/%s", catalog, encodeNamespace(namespace), url.PathEscape(viewName))

	resp, err := c.do(ctx, "DELETE", path, nil, nil)
	if err != nil {
		return err
	}

	return c.checkResponse(resp)
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
