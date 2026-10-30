package v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/pkg/httptransport"
)

// HTTPSource is a Source that fetches logs from a v2 ledger HTTP API.
type HTTPSource struct {
	baseURL    string
	ledgerName string
	httpClient *http.Client
	// closeIdle is false when the default client shares an uncloneable
	// http.DefaultTransport, whose idle connections Close must leave alone.
	closeIdle bool
}

// NewHTTPSource creates a new HTTP-based v2 log source.
// If httpClient is nil, a client with its own transport is used when possible,
// so Close does not drop connections of other clients sharing
// http.DefaultTransport.
func NewHTTPSource(baseURL, ledgerName string, httpClient *http.Client) *HTTPSource {
	return newHTTPSource(baseURL, ledgerName, httpClient, http.DefaultTransport)
}

// newHTTPSource derives the nil-client default from defaultTransport.
func newHTTPSource(baseURL, ledgerName string, httpClient *http.Client, defaultTransport http.RoundTripper) *HTTPSource {
	closeIdle := true
	if httpClient == nil {
		var transport http.RoundTripper
		transport, closeIdle = httptransport.Clone(defaultTransport)
		httpClient = &http.Client{Transport: transport}
	}

	return &HTTPSource{
		baseURL:    baseURL,
		ledgerName: ledgerName,
		httpClient: httpClient,
		closeIdle:  closeIdle,
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
		return nil, errors.New("parsing URL: invalid mirror source URL")
	}

	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	// Observe credentials before net/http masks passwords in its final URL
	// error. Copy the client so concurrent source calls keep separate state.
	users := []*url.Userinfo{u.User}
	client := *s.httpClient
	client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		users = append(users, next.URL.User)
		if s.httpClient.CheckRedirect != nil {
			err := s.httpClient.CheckRedirect(next, via)
			users = append(users, next.URL.User)

			return err
		}
		// Preserve net/http's default redirect limit when no policy was supplied.
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}

		return nil
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %w", path, s.transportDiagnostic(err, users...))
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		_ = resp.Body.Close()

		return nil, fmt.Errorf("v2 API returned status %d: %s", resp.StatusCode, string(body))
	}

	return resp, nil
}

// transportDiagnostic detaches the diagnostic from the unsafe client error.
// net/http masks passwords in its URL wrapper, but not passwordless userinfo;
// transport causes may also repeat credentials. Never retain that error chain.
func (s *HTTPSource) transportDiagnostic(err error, users ...*url.Userinfo) error {
	values := []string{}
	for _, user := range users {
		if user == nil {
			continue
		}
		values = append(values, user.String())
		credentials := []string{user.Username()}
		if password, ok := user.Password(); ok {
			credentials = append(credentials, password)
		}
		for _, credential := range credentials {
			if credential == "" {
				continue
			}
			values = append(values, url.User(credential).String(), url.QueryEscape(credential), url.PathEscape(credential), credential)
		}
	}
	// A credential can itself equal the usual replacement marker. In that case
	// remove it rather than reintroducing it through the replacement text.
	replacement := "[redacted]"
	for _, value := range values {
		if value != "" && strings.Contains(replacement, value) {
			replacement = ""

			break
		}
	}
	// Prefer complete userinfo and longer overlapping forms. A single pass
	// prevents subsequent replacements from modifying already redacted text.
	sort.SliceStable(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	pairs := make([]string, 0, 2*len(values))
	for _, value := range values {
		if value != "" {
			pairs = append(pairs, value, replacement)
		}
	}
	message := strings.NewReplacer(pairs...).Replace(err.Error())
	// Preserve cancellation identity without retaining dependency error objects.
	for _, cancellation := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, cancellation) {
			return fmt.Errorf("%s: %w", message, cancellation)
		}
	}

	return errors.New(message)
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
	if s.closeIdle {
		s.httpClient.CloseIdleConnections()
	}

	return nil
}
