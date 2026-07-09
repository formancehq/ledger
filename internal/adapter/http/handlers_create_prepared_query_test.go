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

// validFieldFilterJSON is a minimal QueryFilter wire shape (v2-aligned query
// DSL) with a populated leaf condition, used to exercise the create/update
// paths without depending on the full filter grammar.
const validFieldFilterJSON = `{"$exists":{"metadata":"foo"}}`

func TestHandleCreatePreparedQuery_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries",
		strings.NewReader(`{"name":"my-query","target":"ACCOUNTS","filter":`+validFieldFilterJSON+`}`),
		map[string]string{
			"ledgerName": "ledger1",
		})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

// TestHandleCreatePreparedQuery_NestedOneofs is the regression for #376: the
// previous handler decoded the body with encoding/json, which silently dropped
// the inner oneof variants and stored a filter with .Filter == nil. The
// resulting prepared query then failed at execute time with
// "unknown filter type: <nil>". Asserting on the captured request lets us
// observe what would have been persisted.
func TestHandleCreatePreparedQuery_NestedOneofs(t *testing.T) {
	t.Parallel()

	var captured *servicepb.Request
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, reqs *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			captured = reqs.GetUnsigned().GetRequests()[0]

			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	body := `{
		"name": "vip-high-risk",
		"target": "ACCOUNTS",
		"filter": {
			"$and": [
				{"$gte": {"metadata[risk_score]": 70}},
				{"$match": {"metadata[vip]": true}}
			]
		}
	}`

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries",
		strings.NewReader(body),
		map[string]string{"ledgerName": "ledger1"})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
	require.NotNil(t, captured)
	create := captured.GetCreatePreparedQuery()
	require.NotNil(t, create)
	filter := create.GetQuery().GetFilter()
	require.NotNil(t, filter, "filter was lost during decoding")

	and := filter.GetAnd()
	require.NotNil(t, and, "outer $and was not dispatched")
	require.Len(t, and.GetFilters(), 2)

	first := and.GetFilters()[0].GetField()
	require.NotNil(t, first, "first child metadata condition was not dispatched")
	require.Equal(t, "risk_score", first.GetField().GetMetadata())
	require.NotNil(t, first.GetIntCond(), "first child int range was not dispatched")
	require.Equal(t, int64(70), first.GetIntCond().GetMin())

	second := and.GetFilters()[1].GetField()
	require.NotNil(t, second)
	require.Equal(t, "vip", second.GetField().GetMetadata())
	require.NotNil(t, second.GetBoolCond(), "second child bool condition was not dispatched")
	require.True(t, second.GetBoolCond().GetHardcoded())
}

func TestHandleCreatePreparedQuery_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/prepared-queries",
		strings.NewReader(`{"name":"my-query","filter":`+validFieldFilterJSON+`}`),
		map[string]string{"ledgerName": ""})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreatePreparedQuery_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries",
		strings.NewReader(`{"target":"ACCOUNTS","filter":`+validFieldFilterJSON+`}`),
		map[string]string{"ledgerName": "ledger1"})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreatePreparedQuery_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries",
		strings.NewReader(`not-json`),
		map[string]string{"ledgerName": "ledger1"})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreatePreparedQuery_MissingFilter(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries",
		strings.NewReader(`{"name":"my-query","target":"ACCOUNTS"}`),
		map[string]string{"ledgerName": "ledger1"})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreatePreparedQuery_EmptyFilter(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries",
		strings.NewReader(`{"name":"my-query","filter":{}}`),
		map[string]string{"ledgerName": "ledger1"})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreatePreparedQuery_LogsTargetRejected(t *testing.T) {
	t.Parallel()

	// LOGS is not a usable prepared-query target over REST: query.Execute has
	// no LOGS hydration path (PreparedQueryCursor carries only account/txn
	// data), so a LOGS prepared query would execute to an empty cursor. REST
	// must reject it at ingestion rather than advertise a broken path.
	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries",
		strings.NewReader(`{"name":"logs-q","target":"LOGS","filter":`+validFieldFilterJSON+`}`),
		map[string]string{"ledgerName": "ledger1"})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreatePreparedQuery_UnknownTargetRejected(t *testing.T) {
	t.Parallel()

	// An unknown target must be rejected loudly, not silently coerced to a
	// default (which would run a different query than requested).
	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries",
		strings.NewReader(`{"name":"q","target":"BOGUS","filter":`+validFieldFilterJSON+`}`),
		map[string]string{"ledgerName": "ledger1"})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreatePreparedQuery_MissingTargetRejected(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries",
		strings.NewReader(`{"name":"q","filter":`+validFieldFilterJSON+`}`),
		map[string]string{"ledgerName": "ledger1"})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreatePreparedQuery_AlreadyExists(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, &domain.ErrPreparedQueryAlreadyExists{Ledger: "ledger1", Name: "my-query"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries",
		strings.NewReader(`{"name":"my-query","target":"TRANSACTIONS","filter":`+validFieldFilterJSON+`}`),
		map[string]string{"ledgerName": "ledger1"})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusConflict, w.Code)
}
