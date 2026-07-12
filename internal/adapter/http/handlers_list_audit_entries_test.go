package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleListAuditEntries_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAuditEntries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ uint32, _ uint64, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*auditpb.AuditEntry], error) {
			return cursor.NewSliceCursor([]*auditpb.AuditEntry{
				{Sequence: 1},
				{Sequence: 2},
			}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/audit-entries", nil, nil)

	srv.handleListAuditEntries(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	resp := decodeResponse[BaseResponse[[]map[string]any]](t, w)
	require.Len(t, resp.Data, 2)
	// camelCase JSON: sequence rendered as a JSON number.
	require.EqualValues(t, 1, resp.Data[0]["sequence"])
}

func TestHandleListAuditEntries_Empty(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAuditEntries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ uint32, _ uint64, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*auditpb.AuditEntry], error) {
			return cursor.NewSliceCursor[*auditpb.AuditEntry](nil), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/audit-entries", nil, nil)

	srv.handleListAuditEntries(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	// An empty result must serialize as an array, not null (OpenAPI contract).
	require.JSONEq(t, `{"data":[]}`, w.Body.String())
}

func TestHandleListAuditEntries_Pagination(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAuditEntries(gomock.Any(), uint32(10), uint64(42), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, pageSize uint32, afterSequence uint64, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*auditpb.AuditEntry], error) {
			require.EqualValues(t, 10, pageSize)
			require.EqualValues(t, 42, afterSequence)

			return cursor.NewSliceCursor[*auditpb.AuditEntry](nil), nil
		}).Times(1)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/audit-entries?pageSize=10&after=42", nil, nil)

	srv.handleListAuditEntries(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListAuditEntries_Reverse(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAuditEntries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), true).DoAndReturn(
		func(_ context.Context, _ uint32, _ uint64, _ *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*auditpb.AuditEntry], error) {
			require.True(t, reverse)

			return cursor.NewSliceCursor[*auditpb.AuditEntry](nil), nil
		}).Times(1)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/audit-entries?reverse=true", nil, nil)

	srv.handleListAuditEntries(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

// TestHandleListAuditEntries_MarshalFailureIsClean500 asserts that a failure
// while marshaling an audit DTO (here a chain-bound callerSnapshot with an
// invalid-UTF8 scope, which protojson rejects) produces a clean 500 with an
// error body — NOT a 200 with a truncated body. writeOKChecked buffers before
// writing any header, so the marshal error is routed through handleError.
func TestHandleListAuditEntries_MarshalFailureIsClean500(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAuditEntries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ uint32, _ uint64, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*auditpb.AuditEntry], error) {
			return cursor.NewSliceCursor([]*auditpb.AuditEntry{
				{
					Sequence:       1,
					CallerSnapshot: &commonpb.CallerSnapshot{Scopes: []string{"\xff\xfe"}},
				},
			}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/audit-entries", nil, nil)

	srv.handleListAuditEntries(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	// The body is the error envelope, never a partial success payload.
	require.Contains(t, w.Body.String(), `"errorCode":"INTERNAL_ERROR"`)
	require.NotContains(t, w.Body.String(), `"data"`)
}

func TestHandleListAuditEntries_InvalidAfter(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/audit-entries?after=notanumber", nil, nil)

	srv.handleListAuditEntries(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListAuditEntries_InvalidPageSize(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/audit-entries?pageSize=abc", nil, nil)

	srv.handleListAuditEntries(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestHandleListAuditEntries_LedgerFilter checks that a `filter` with an
// audit[ledger] condition is parsed via filterexpr and forwarded to the backend.
func TestHandleListAuditEntries_LedgerFilter(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAuditEntries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ uint32, _ uint64, filter *commonpb.QueryFilter, _ bool) (cursor.Cursor[*auditpb.AuditEntry], error) {
			require.NotNil(t, filter)
			audit := filter.GetAudit()
			require.NotNil(t, audit)
			require.Equal(t, commonpb.AuditField_AUDIT_FIELD_LEDGER, audit.GetField())
			require.Equal(t, "main", audit.GetStringCond().GetHardcoded())

			return cursor.NewSliceCursor[*auditpb.AuditEntry](nil), nil
		}).Times(1)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/audit-entries?filter="+url.QueryEscape("audit[ledger] == main"), nil, nil)

	srv.handleListAuditEntries(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

// TestHandleListAuditEntries_OutcomeFilter exercises a representative audit
// outcome filter expression.
func TestHandleListAuditEntries_OutcomeFilter(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAuditEntries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ uint32, _ uint64, filter *commonpb.QueryFilter, _ bool) (cursor.Cursor[*auditpb.AuditEntry], error) {
			require.NotNil(t, filter)
			require.Equal(t, commonpb.AuditField_AUDIT_FIELD_OUTCOME, filter.GetAudit().GetField())

			return cursor.NewSliceCursor[*auditpb.AuditEntry](nil), nil
		}).Times(1)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/audit-entries?filter="+url.QueryEscape("audit[outcome] == failure"), nil, nil)

	srv.handleListAuditEntries(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListAuditEntries_InvalidFilter(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/audit-entries?filter="+url.QueryEscape("this is not a filter"), nil, nil)

	srv.handleListAuditEntries(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestHandleListAuditEntries_UnsupportedFilterMapsTo400 covers a filter that
// parses (filterexpr accepts it) but the audit compiler rejects with a gRPC
// codes.InvalidArgument (e.g. `not audit[...]`, a non-audit condition). Such an
// error must surface as a 400, not a 500.
func TestHandleListAuditEntries_UnsupportedFilterMapsTo400(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAuditEntries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ uint32, _ uint64, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*auditpb.AuditEntry], error) {
			return nil, status.Error(codes.InvalidArgument, "unsupported filter for audit entries")
		}).Times(1)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	// `not audit[...]` parses fine but the compiler rejects it.
	r := newRequest(t, http.MethodGet, "/_/audit-entries?filter="+url.QueryEscape("not audit[outcome] == failure"), nil, nil)

	srv.handleListAuditEntries(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
