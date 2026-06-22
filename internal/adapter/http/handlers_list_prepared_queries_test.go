package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleListPreparedQueries_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListPreparedQueries(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) ([]*commonpb.PreparedQuery, error) {
			return []*commonpb.PreparedQuery{
				{Name: "query1"},
				{Name: "query2"},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/prepared-queries", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListPreparedQueries(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListPreparedQueries_Empty(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListPreparedQueries(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) ([]*commonpb.PreparedQuery, error) {
			return nil, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/prepared-queries", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListPreparedQueries(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

// TestHandleListPreparedQueries_CamelCaseBodyShape pins the wire format of
// the LIST response. Without this assertion, the existing status-only checks
// let #478 ship: PreparedQuery's oneof variants and enum field went out as
// PascalCase / raw int because the response path bypassed protojson.
func TestHandleListPreparedQueries_CamelCaseBodyShape(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListPreparedQueries(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) ([]*commonpb.PreparedQuery, error) {
			return []*commonpb.PreparedQuery{{
				Name:   "q1",
				Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
				Filter: &commonpb.QueryFilter{
					Filter: &commonpb.QueryFilter_Reference{
						Reference: &commonpb.ReferenceCondition{
							Cond: &commonpb.StringCondition{
								Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "order-123"},
							},
						},
					},
				},
			}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/prepared-queries", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListPreparedQueries(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	body := w.Body.String()
	require.Contains(t, body, `"target":"QUERY_TARGET_TRANSACTIONS"`)
	require.Contains(t, body, `"reference"`)
	require.Contains(t, body, `"hardcoded":"order-123"`)
	require.NotContains(t, body, `"Reference"`)
	require.NotContains(t, body, `"Hardcoded"`)
	require.NotContains(t, body, `"target":1`)
}

func TestHandleListPreparedQueries_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/prepared-queries", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleListPreparedQueries(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListPreparedQueries_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListPreparedQueries(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) ([]*commonpb.PreparedQuery, error) {
			return nil, errors.New("unexpected error")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/prepared-queries", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListPreparedQueries(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
