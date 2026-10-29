package v2

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPSource_FetchLogs_Success(t *testing.T) {
	t.Parallel()

	logs := []V2Log{
		{ID: 1, Type: "NEW_TRANSACTION", Data: json.RawMessage(`{}`)},
		{ID: 2, Type: "SET_METADATA", Data: json.RawMessage(`{}`)},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v2/default/logs", r.URL.Path)
		require.Equal(t, "10", r.URL.Query().Get("pageSize"))
		require.Equal(t, "id:asc", r.URL.Query().Get("sort"))

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(V2LogPage{
			Cursor: V2LogCursor{
				PageSize: 10,
				HasMore:  false,
				Data:     logs,
			},
		})
	}))
	defer srv.Close()

	source := NewHTTPSource(srv.URL, "default", nil)

	defer func() { _ = source.Close() }()

	result, hasMore, err := source.FetchLogs(context.Background(), 0, 10)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, result, 2)
	require.Equal(t, uint64(1), result[0].ID)
	require.Equal(t, uint64(2), result[1].ID)
}

// Closing one default-client source must not drop another source's idle
// connections through a shared http.DefaultTransport.
func TestHTTPSource_CloseDoesNotAffectOtherSources(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(V2LogPage{})
	}))
	defer srv.Close()

	active := NewHTTPSource(srv.URL, "default", nil)
	defer func() { _ = active.Close() }()
	closed := NewHTTPSource(srv.URL, "default", nil)

	_, _, err := active.FetchLogs(context.Background(), 0, 10)
	require.NoError(t, err)
	require.NoError(t, closed.Close())

	var reused bool
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
	})
	_, _, err = active.FetchLogs(ctx, 0, 10)
	require.NoError(t, err)
	require.True(t, reused, "the active source's idle connection must survive another source's Close")
}

type closeCountingTransport struct {
	http.RoundTripper

	closes int
}

func (c *closeCountingTransport) CloseIdleConnections() { c.closes++ }

// A default-client source sharing an uncloneable http.DefaultTransport must
// leave its idle connections alone on Close; a caller-supplied client keeps
// being closed as before.
func TestHTTPSource_CloseIdleOwnership(t *testing.T) {
	t.Parallel()

	shared := &closeCountingTransport{RoundTripper: http.DefaultTransport}
	source := NewHTTPSource("http://example.com", "default", nil)
	source.httpClient.Transport, source.closeIdle = shared, false
	require.NoError(t, source.Close())
	require.Zero(t, shared.closes)

	supplied := &closeCountingTransport{RoundTripper: http.DefaultTransport}
	require.NoError(t, NewHTTPSource("http://example.com", "default", &http.Client{Transport: supplied}).Close())
	require.Equal(t, 1, supplied.closes)
}

func TestHTTPSource_FetchLogs_WithAfterID(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.JSONEq(t, `{"$gt":{"id":5}}`, r.URL.Query().Get("query"))
		require.Empty(t, r.URL.Query().Get("after"))

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(V2LogPage{
			Cursor: V2LogCursor{
				PageSize: 10,
				HasMore:  true,
				Data: []V2Log{
					{ID: 6, Type: "NEW_TRANSACTION", Data: json.RawMessage(`{}`)},
				},
			},
		})
	}))
	defer srv.Close()

	source := NewHTTPSource(srv.URL, "default", nil)
	result, hasMore, err := source.FetchLogs(context.Background(), 5, 10)
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, result, 1)
}

func TestHTTPSource_FetchLogs_NilClient(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(V2LogPage{
			Cursor: V2LogCursor{Data: nil},
		})
	}))
	defer srv.Close()

	source := NewHTTPSource(srv.URL, "my-ledger", nil)
	result, hasMore, err := source.FetchLogs(context.Background(), 0, 10)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Empty(t, result)
}

func TestHTTPSource_FetchLogs_ServerError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	source := NewHTTPSource(srv.URL, "default", nil)
	_, _, err := source.FetchLogs(context.Background(), 0, 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "500")
}

func TestHTTPSource_FetchLogs_InvalidJSON(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{invalid`))
	}))
	defer srv.Close()

	source := NewHTTPSource(srv.URL, "default", nil)
	_, _, err := source.FetchLogs(context.Background(), 0, 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decoding")
}

func TestHTTPSource_FetchLogs_ContextCancelled(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(V2LogPage{})
	}))
	defer srv.Close()

	source := NewHTTPSource(srv.URL, "default", nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := source.FetchLogs(ctx, 0, 10)
	require.Error(t, err)
}

func TestHTTPSource_Close(t *testing.T) {
	t.Parallel()

	source := NewHTTPSource("http://localhost:0", "default", nil)
	err := source.Close()
	require.NoError(t, err)
}

func TestHTTPSource_GetLatestLogID_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v2/default/logs", r.URL.Path)
		require.Equal(t, "1", r.URL.Query().Get("pageSize"))
		require.Equal(t, "id:desc", r.URL.Query().Get("sort"))
		require.Empty(t, r.URL.Query().Get("query"))

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(V2LogPage{
			Cursor: V2LogCursor{
				PageSize: 1,
				HasMore:  true,
				Data: []V2Log{
					{ID: 42, Type: "NEW_TRANSACTION", Data: json.RawMessage(`{}`)},
				},
			},
		})
	}))
	defer srv.Close()

	source := NewHTTPSource(srv.URL, "default", nil)
	latestID, err := source.GetLatestLogID(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(42), latestID)
}

func TestHTTPSource_GetLatestLogID_Empty(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(V2LogPage{
			Cursor: V2LogCursor{Data: nil},
		})
	}))
	defer srv.Close()

	source := NewHTTPSource(srv.URL, "default", nil)
	latestID, err := source.GetLatestLogID(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(0), latestID)
}

func TestHTTPSource_GetLatestLogID_ServerError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	source := NewHTTPSource(srv.URL, "default", nil)
	_, err := source.GetLatestLogID(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "500")
}

func TestHTTPSource_FetchLogs_HasMore(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(V2LogPage{
			Cursor: V2LogCursor{
				PageSize: 2,
				HasMore:  true,
				Data: []V2Log{
					{ID: 1, Type: "NEW_TRANSACTION", Data: json.RawMessage(`{}`)},
					{ID: 2, Type: "NEW_TRANSACTION", Data: json.RawMessage(`{}`)},
				},
			},
		})
	}))
	defer srv.Close()

	source := NewHTTPSource(srv.URL, "default", nil)
	result, hasMore, err := source.FetchLogs(context.Background(), 0, 2)
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, result, 2)
}
