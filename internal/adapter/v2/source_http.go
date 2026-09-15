package v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// HTTPSource is a Source that fetches logs from a v2 ledger HTTP API.
type HTTPSource struct {
	baseURL    string
	ledgerName string
	httpClient *http.Client
}

// NewHTTPSource creates a new HTTP-based v2 log source.
// If httpClient is nil, a default http.Client is used.
func NewHTTPSource(baseURL, ledgerName string, httpClient *http.Client) *HTTPSource {
	if httpClient == nil {
		httpClient = &http.Client{}
	}

	return &HTTPSource{
		baseURL:    baseURL,
		ledgerName: ledgerName,
		httpClient: httpClient,
	}
}

// doGet performs a GET request against the v2 API and returns the response body.
// The caller is responsible for closing the response body.
func (s *HTTPSource) doGet(ctx context.Context, path string, query url.Values) (*http.Response, error) {
	u, err := url.Parse(fmt.Sprintf("%s/v2/%s/%s", s.baseURL, s.ledgerName, path))
	if err != nil {
		// url.Error includes the supplied URL, and even its underlying cause
		// may echo sensitive input (for example an invalid port). Do not retain
		// either in errors consumed by worker logs and replicated mirror status.
		return nil, safeHTTPURLParseError(err)
	}

	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %w", path, err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		_ = resp.Body.Close()

		return nil, fmt.Errorf("v2 API returned status %d: %s", resp.StatusCode, string(body))
	}

	return resp, nil
}

// safeHTTPURLParseError preserves the failure category without retaining any
// input fragments or wrapping chain. net/url exports types for only some parse
// failures; recognize the remaining known messages but emit only fixed text.
// Unknown errors keep a generic diagnostic, including after toolchain upgrades.
func safeHTTPURLParseError(err error) error {
	reason := "invalid mirror source URL"
	if parseErr, ok := errors.AsType[*url.Error](err); ok {
		var escapeErr url.EscapeError
		var hostErr url.InvalidHostError
		switch {
		case errors.As(parseErr.Err, &escapeErr):
			reason = "invalid URL escape; check percent-encoding"
		case errors.As(parseErr.Err, &hostErr):
			reason = "invalid character in host"
		case strings.HasPrefix(parseErr.Err.Error(), "invalid port "):
			reason = "invalid port; use a numeric port after ':'"
		case strings.HasPrefix(parseErr.Err.Error(), "invalid host:"):
			reason = "invalid IP-literal; check the bracketed IPv6 address"
		default:
			switch parseErr.Err.Error() {
			case "net/url: invalid control character in URL":
				reason = "control character in URL"
			case "net/url: invalid userinfo":
				reason = "invalid userinfo; percent-encode special characters in credentials"
			case "missing ']' in host":
				reason = "missing ']' in host; enclose IPv6 addresses in brackets"
			case "invalid IP-literal":
				reason = "invalid IP-literal; check the bracketed IPv6 address"
			case "missing protocol scheme":
				reason = "missing protocol scheme"
			case "first path segment in URL cannot contain colon":
				reason = "first path segment cannot contain ':'; check the URL scheme"
			}
		}
	}

	return fmt.Errorf("parsing URL: %s", reason)
}

// FetchLogs fetches logs from the v2 API.
// afterID is the last known log ID (0 to start from the beginning).
// Returns logs (oldest first), whether there are more, and any error.
func (s *HTTPSource) FetchLogs(ctx context.Context, afterID uint64, pageSize int) ([]V2Log, bool, error) {
	q := url.Values{}
	q.Set("pageSize", strconv.Itoa(pageSize))
	q.Set("sort", "id:asc")
	// v2 supports a numeric query filter, not an "after" parameter. Rebuild
	// the query from the applied boundary on every fetch, including after an
	// empty tail or a failed batch; no HTTP cursor becomes a second authority.
	q.Set("query", fmt.Sprintf(`{"$gt":{"id":%d}}`, afterID))

	resp, err := s.doGet(ctx, "logs", q)
	if err != nil {
		return nil, false, err
	}

	defer func() { _ = resp.Body.Close() }()

	var page V2LogPage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, false, fmt.Errorf("decoding response: %w", err)
	}

	return page.Cursor.Data, page.Cursor.HasMore, nil
}

// GetLatestLogID returns the latest log ID from the v2 source by fetching
// the first page (newest first) with pageSize=1.
func (s *HTTPSource) GetLatestLogID(ctx context.Context) (uint64, error) {
	q := url.Values{}
	q.Set("pageSize", "1")
	q.Set("sort", "id:desc")

	resp, err := s.doGet(ctx, "logs", q)
	if err != nil {
		return 0, err
	}

	defer func() { _ = resp.Body.Close() }()

	var page V2LogPage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return 0, fmt.Errorf("decoding response: %w", err)
	}

	if len(page.Cursor.Data) == 0 {
		return 0, nil
	}

	return page.Cursor.Data[0].ID, nil
}

// Close closes idle connections in the underlying HTTP transport.
func (s *HTTPSource) Close() error {
	s.httpClient.CloseIdleConnections()

	return nil
}
