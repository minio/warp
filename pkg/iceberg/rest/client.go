package rest

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7/pkg/s3utils"
)

type Client struct {
	baseURLs   []string
	hostIdx    uint64
	apiPrefix  string
	httpClient *http.Client

	accessKey string
	secretKey string
	region    string
	service   string
}

type ClientConfig struct {
	BaseURLs  []string
	APIPrefix string
	AccessKey string
	SecretKey string
	Region    string
	Service   string
}

func NewClient(cfg ClientConfig) *Client {
	if cfg.APIPrefix == "" {
		cfg.APIPrefix = "/v1"
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.Service == "" {
		cfg.Service = "s3tables"
	}

	baseURLs := make([]string, len(cfg.BaseURLs))
	for i, u := range cfg.BaseURLs {
		baseURLs[i] = strings.TrimSuffix(u, "/")
	}

	return &Client{
		baseURLs:   baseURLs,
		apiPrefix:  cfg.APIPrefix,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		accessKey:  cfg.AccessKey,
		secretKey:  cfg.SecretKey,
		region:     cfg.Region,
		service:    cfg.Service,
	}
}

func (c *Client) nextBaseURL() string {
	if len(c.baseURLs) == 1 {
		return c.baseURLs[0]
	}
	idx := atomic.AddUint64(&c.hostIdx, 1)
	return c.baseURLs[idx%uint64(len(c.baseURLs))]
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, body interface{}) (*http.Response, error) {
	fullURL := c.nextBaseURL() + c.apiPrefix + path
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}

	var bodyReader io.Reader
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	if err := c.signRequest(req, bodyBytes); err != nil {
		return nil, fmt.Errorf("sign request: %w", err)
	}

	return c.httpClient.Do(req)
}

func (c *Client) signRequest(req *http.Request, payload []byte) error {
	t := time.Now().UTC()
	amzDate := t.Format("20060102T150405Z")
	dateStamp := t.Format("20060102")

	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("Host", req.URL.Host)

	payloadHash := sha256Hex(payload)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signedHeaders, canonicalHeaders := c.buildCanonicalHeaders(req)

	canonicalURI := req.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	// EOS/MinIO S3 Tables expects %1F (encoded) in the signed path for nested namespaces
	canonicalURI = strings.ReplaceAll(canonicalURI, "\x1f", "%1F")
	// Apply s3utils.EncodePath to match server's signature calculation
	canonicalURI = s3utils.EncodePath(canonicalURI)

	canonicalQueryString := req.URL.Query().Encode()

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	credentialScope := strings.Join([]string{dateStamp, c.region, c.service, "aws4_request"}, "/")

	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	signingKey := c.deriveSigningKey(dateStamp)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	authHeader := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.accessKey, credentialScope, signedHeaders, signature,
	)
	req.Header.Set("Authorization", authHeader)

	return nil
}

func (c *Client) buildCanonicalHeaders(req *http.Request) (signedHeaders, canonicalHeaders string) {
	headers := make(map[string]string)
	var headerNames []string

	for name, values := range req.Header {
		lowerName := strings.ToLower(name)
		if lowerName == "host" || lowerName == "content-type" || strings.HasPrefix(lowerName, "x-amz-") {
			headers[lowerName] = strings.TrimSpace(values[0])
			headerNames = append(headerNames, lowerName)
		}
	}

	if _, ok := headers["host"]; !ok {
		headers["host"] = req.URL.Host
		headerNames = append(headerNames, "host")
	}

	sort.Strings(headerNames)

	var canonicalHeadersBuilder strings.Builder
	for _, name := range headerNames {
		canonicalHeadersBuilder.WriteString(name)
		canonicalHeadersBuilder.WriteString(":")
		canonicalHeadersBuilder.WriteString(headers[name])
		canonicalHeadersBuilder.WriteString("\n")
	}

	return strings.Join(headerNames, ";"), canonicalHeadersBuilder.String()
}

func (c *Client) deriveSigningKey(dateStamp string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+c.secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(c.region))
	kService := hmacSHA256(kRegion, []byte(c.service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func (c *Client) parseResponse(resp *http.Response, v interface{}) error {
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return &APIError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
		}
	}

	if v != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}

	return nil
}

func (c *Client) checkResponse(resp *http.Response) error {
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return &APIError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
		}
	}

	return nil
}

type APIError struct {
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("API error %d: %s", e.StatusCode, e.Body)
}

func IsNotFound(err error) bool {
	if apiErr, ok := err.(*APIError); ok {
		return apiErr.StatusCode == http.StatusNotFound
	}
	return false
}

func IsConflict(err error) bool {
	if apiErr, ok := err.(*APIError); ok {
		return apiErr.StatusCode == http.StatusConflict
	}
	return false
}

func IsRetryable(err error) bool {
	if apiErr, ok := err.(*APIError); ok {
		return apiErr.StatusCode == http.StatusConflict ||
			apiErr.StatusCode == http.StatusInternalServerError
	}
	return false
}
