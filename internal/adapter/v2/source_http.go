package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// HTTPSource is a Source that fetches logs from a v2 ledger HTTP API.
type HTTPSource struct {
	baseURL    string
	ledgerName string
	authToken  string
	httpClient *http.Client
}

// NewHTTPSource creates a new HTTP-based v2 log source.
func NewHTTPSource(baseURL, ledgerName, authToken string) *HTTPSource {
	return &HTTPSource{
		baseURL:    baseURL,
		ledgerName: ledgerName,
		authToken:  authToken,
		httpClient: &http.Client{},
	}
}

// doGet performs a GET request against the v2 API and returns the response body.
// The caller is responsible for closing the response body.
func (s *HTTPSource) doGet(ctx context.Context, path string, query url.Values) (*http.Response, error) {
	u, err := url.Parse(fmt.Sprintf("%s/v2/%s/%s", s.baseURL, s.ledgerName, path))
	if err != nil {
		return nil, fmt.Errorf("parsing URL: %w", err)
	}

	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	if s.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.authToken)
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

// FetchLogs fetches logs from the v2 API.
// afterID is the last known log ID (0 to start from the beginning).
// Returns logs (oldest first), whether there are more, and any error.
func (s *HTTPSource) FetchLogs(ctx context.Context, afterID uint64, pageSize int) ([]V2Log, bool, error) {
	q := url.Values{}
	q.Set("pageSize", strconv.Itoa(pageSize))
	if afterID > 0 {
		q.Set("after", strconv.FormatUint(afterID, 10))
	}

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
