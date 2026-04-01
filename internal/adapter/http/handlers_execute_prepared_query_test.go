package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

func TestHandleExecutePreparedQuery_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		executePreparedQueryFn: func(_ context.Context, _ *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
			return &servicepb.ExecutePreparedQueryResponse{}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute", strings.NewReader(`{"pageSize":10}`), map[string]string{
		"ledgerName": "ledger1",
		"queryName":  "my-query",
	})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleExecutePreparedQuery_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/prepared-queries/my-query/execute", nil, map[string]string{
		"ledgerName": "",
		"queryName":  "my-query",
	})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleExecutePreparedQuery_MissingQueryName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries//execute", nil, map[string]string{
		"ledgerName": "ledger1",
		"queryName":  "",
	})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleExecutePreparedQuery_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute", strings.NewReader(`not-json`), map[string]string{
		"ledgerName": "ledger1",
		"queryName":  "my-query",
	})
	r.Header.Set("Content-Length", "8")

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleExecutePreparedQuery_NotFound(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		executePreparedQueryFn: func(_ context.Context, _ *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
			return nil, &domain.ErrPreparedQueryNotFound{Ledger: "ledger1", Name: "missing"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/missing/execute", nil, map[string]string{
		"ledgerName": "ledger1",
		"queryName":  "missing",
	})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleExecutePreparedQuery_NoBody(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		executePreparedQueryFn: func(_ context.Context, _ *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
			return &servicepb.ExecutePreparedQueryResponse{}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute?pageSize=5&cursor=abc", nil, map[string]string{
		"ledgerName": "ledger1",
		"queryName":  "my-query",
	})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleExecutePreparedQuery_WithParameters(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		executePreparedQueryFn: func(_ context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
			require.NotNil(t, req.GetParameters())

			return &servicepb.ExecutePreparedQueryResponse{}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute",
		strings.NewReader(`{"parameters":{"account":"alice","active":true,"amount":42},"mode":"AGGREGATE_VOLUMES"}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "my-query",
		})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleExecutePreparedQuery_UnsupportedParameterType(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries/my-query/execute",
		strings.NewReader(`{"parameters":{"bad":[1,2,3]}}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "my-query",
		})

	srv.handleExecutePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
