package rest

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

func (c *Client) CreateNamespace(ctx context.Context, catalog string, namespace []string, properties map[string]string) (*Namespace, error) {
	path := fmt.Sprintf("/%s/namespaces", catalog)

	req := CreateNamespaceRequest{
		Namespace:  namespace,
		Properties: properties,
	}

	resp, err := c.do(ctx, "POST", path, nil, req)
	if err != nil {
		return nil, err
	}

	var ns Namespace
	if err := c.parseResponse(resp, &ns); err != nil {
		return nil, err
	}

	return &ns, nil
}

func (c *Client) GetNamespace(ctx context.Context, catalog string, namespace []string) (*Namespace, error) {
	path := fmt.Sprintf("/%s/namespaces/%s", catalog, encodeNamespace(namespace))

	resp, err := c.do(ctx, "GET", path, nil, nil)
	if err != nil {
		return nil, err
	}

	var ns Namespace
	if err := c.parseResponse(resp, &ns); err != nil {
		return nil, err
	}

	return &ns, nil
}

func (c *Client) NamespaceExists(ctx context.Context, catalog string, namespace []string) (bool, error) {
	path := fmt.Sprintf("/%s/namespaces/%s", catalog, encodeNamespace(namespace))

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

func (c *Client) ListNamespaces(ctx context.Context, catalog string, parent []string) (*ListNamespacesResponse, error) {
	path := fmt.Sprintf("/%s/namespaces", catalog)

	query := url.Values{}
	if len(parent) > 0 {
		query.Set("parent", encodeNamespace(parent))
	}

	resp, err := c.do(ctx, "GET", path, query, nil)
	if err != nil {
		return nil, err
	}

	var result ListNamespacesResponse
	if err := c.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *Client) UpdateNamespaceProperties(ctx context.Context, catalog string, namespace []string, updates map[string]string, removals []string) (*UpdateNamespacePropertiesResponse, error) {
	path := fmt.Sprintf("/%s/namespaces/%s/properties", catalog, encodeNamespace(namespace))

	req := UpdateNamespacePropertiesRequest{
		Updates:  updates,
		Removals: removals,
	}

	resp, err := c.do(ctx, "POST", path, nil, req)
	if err != nil {
		return nil, err
	}

	var result UpdateNamespacePropertiesResponse
	if err := c.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *Client) DropNamespace(ctx context.Context, catalog string, namespace []string) error {
	path := fmt.Sprintf("/%s/namespaces/%s", catalog, encodeNamespace(namespace))

	resp, err := c.do(ctx, "DELETE", path, nil, nil)
	if err != nil {
		return err
	}

	return c.checkResponse(resp)
}

func encodeNamespace(namespace []string) string {
	encoded := make([]string, len(namespace))
	for i, part := range namespace {
		encoded[i] = url.PathEscape(part)
	}
	return strings.Join(encoded, url.PathEscape("\x1f"))
}
