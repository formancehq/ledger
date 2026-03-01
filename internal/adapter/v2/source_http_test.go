package v2

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		require.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))

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

	source := NewHTTPSource(srv.URL, "default", "test-token")
	defer func() { _ = source.Close() }()

	result, hasMore, err := source.FetchLogs(context.Background(), 0, 10)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, result, 2)
	require.Equal(t, uint64(1), result[0].ID)
	require.Equal(t, uint64(2), result[1].ID)
}

func TestHTTPSource_FetchLogs_WithAfterID(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "5", r.URL.Query().Get("after"))

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

	source := NewHTTPSource(srv.URL, "default", "")
	result, hasMore, err := source.FetchLogs(context.Background(), 5, 10)
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, result, 1)
}

func TestHTTPSource_FetchLogs_NoAuthToken(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Empty(t, r.Header.Get("Authorization"))

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(V2LogPage{
			Cursor: V2LogCursor{Data: nil},
		})
	}))
	defer srv.Close()

	source := NewHTTPSource(srv.URL, "my-ledger", "")
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

	source := NewHTTPSource(srv.URL, "default", "")
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

	source := NewHTTPSource(srv.URL, "default", "")
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

	source := NewHTTPSource(srv.URL, "default", "")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := source.FetchLogs(ctx, 0, 10)
	require.Error(t, err)
}

func TestHTTPSource_Close(t *testing.T) {
	t.Parallel()

	source := NewHTTPSource("http://localhost:0", "default", "")
	err := source.Close()
	require.NoError(t, err)
}

func TestHTTPSource_GetLatestLogID_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v2/default/logs", r.URL.Path)
		require.Equal(t, "1", r.URL.Query().Get("pageSize"))

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

	source := NewHTTPSource(srv.URL, "default", "")
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

	source := NewHTTPSource(srv.URL, "default", "")
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

	source := NewHTTPSource(srv.URL, "default", "")
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

	source := NewHTTPSource(srv.URL, "default", "token")
	result, hasMore, err := source.FetchLogs(context.Background(), 0, 2)
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, result, 2)
}
