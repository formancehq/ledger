package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleUpdatePreparedQuery_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/prepared-queries/my-query",
		strings.NewReader(`{"filter":`+validFieldFilterJSON+`}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "my-query",
		})

	srv.handleUpdatePreparedQuery(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

// TestHandleUpdatePreparedQuery_NestedOneofs mirrors the create regression for
// the update path (same handler bug, same root cause — see #376).
func TestHandleUpdatePreparedQuery_NestedOneofs(t *testing.T) {
	t.Parallel()

	var captured *servicepb.Request
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, reqs ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			captured = reqs[0].GetUnsigned()

			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	body := `{
		"filter": {
			"or": {
				"filters": [
					{"field": {"field": {"metadata": "tier"}, "stringCond": {"hardcoded": "gold"}}},
					{"field": {"field": {"metadata": "tier"}, "stringCond": {"hardcoded": "platinum"}}}
				]
			}
		}
	}`

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/prepared-queries/my-query",
		strings.NewReader(body),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "my-query",
		})

	srv.handleUpdatePreparedQuery(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
	require.NotNil(t, captured)
	update := captured.GetUpdatePreparedQuery()
	require.NotNil(t, update)
	filter := update.GetFilter()
	require.NotNil(t, filter)

	or := filter.GetOr()
	require.NotNil(t, or, "outer OR oneof was not dispatched")
	require.Len(t, or.GetFilters(), 2)
	for i, child := range or.GetFilters() {
		f := child.GetField()
		require.NotNil(t, f, "child %d field oneof was not dispatched", i)
		require.NotNil(t, f.GetStringCond(), "child %d string_cond oneof was not dispatched", i)
	}
}

func TestHandleUpdatePreparedQuery_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/prepared-queries/my-query",
		strings.NewReader(`{"filter":`+validFieldFilterJSON+`}`),
		map[string]string{
			"ledgerName": "",
			"queryName":  "my-query",
		})

	srv.handleUpdatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpdatePreparedQuery_MissingQueryName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/prepared-queries/",
		strings.NewReader(`{"filter":`+validFieldFilterJSON+`}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "",
		})

	srv.handleUpdatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpdatePreparedQuery_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/prepared-queries/my-query",
		strings.NewReader(`not-json`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "my-query",
		})

	srv.handleUpdatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpdatePreparedQuery_MissingFilter(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/prepared-queries/my-query",
		strings.NewReader(`{}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "my-query",
		})

	srv.handleUpdatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpdatePreparedQuery_EmptyFilter(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/prepared-queries/my-query",
		strings.NewReader(`{"filter":{}}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "my-query",
		})

	srv.handleUpdatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpdatePreparedQuery_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			return nil, &domain.ErrPreparedQueryNotFound{Ledger: "ledger1", Name: "missing"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/prepared-queries/missing",
		strings.NewReader(`{"filter":`+validFieldFilterJSON+`}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "missing",
		})

	srv.handleUpdatePreparedQuery(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}
