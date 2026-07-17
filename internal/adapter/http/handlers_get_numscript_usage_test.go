package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestToTemplateUsageJSON_EpochLastUsed is the regression guard for the
// dropped-epoch bug: a non-nil lastUsed at Data==0 (the Unix epoch) must be
// emitted, not silently omitted. nil is the only "never used" sentinel.
func TestToTemplateUsageJSON_EpochLastUsed(t *testing.T) {
	t.Parallel()

	out := toTemplateUsageJSON(&commonpb.TemplateUsage{
		Count:    3,
		LastUsed: &commonpb.Timestamp{Data: 0},
	})

	require.NotNil(t, out.LastUsed, "a non-nil epoch timestamp must be present, not omitted")
	assert.Equal(t, "1970-01-01T00:00:00Z", *out.LastUsed, "Data==0 is the Unix epoch, formatted from microseconds")
	assert.Equal(t, uint64(3), out.Count)
}

// TestToTemplateUsageJSON_NilLastUsedOmitted confirms nil (never invoked)
// still drops lastUsed from the JSON (omitempty on the pointer).
func TestToTemplateUsageJSON_NilLastUsedOmitted(t *testing.T) {
	t.Parallel()

	out := toTemplateUsageJSON(&commonpb.TemplateUsage{Count: 0})
	require.Nil(t, out.LastUsed)

	raw, err := json.Marshal(out)
	require.NoError(t, err)
	assert.JSONEq(t, `{"count":0}`, string(raw), "nil lastUsed must be omitted, not serialized as null")
}

// TestToTemplateUsageJSON_MicrosecondUnitAndCamelCase locks the two sibling
// contract points flemzord flagged alongside the epoch bug:
//   - the DTO serializes camelCase (`lastUsed`, `count`) with no raw protobuf
//     tags (`last_used`) or wire encoding (`{data: <int64>}`);
//   - Timestamp.Data is interpreted as microseconds, not nanoseconds.
func TestToTemplateUsageJSON_MicrosecondUnitAndCamelCase(t *testing.T) {
	t.Parallel()

	// 1_700_000_000_000_000 µs = 2023-11-14T22:13:20Z. If Data were treated
	// as nanoseconds the year would collapse to 1970.
	out := toTemplateUsageJSON(&commonpb.TemplateUsage{
		Count:    42,
		LastUsed: &commonpb.Timestamp{Data: 1_700_000_000_000_000},
	})

	require.NotNil(t, out.LastUsed)
	assert.Equal(t, "2023-11-14T22:13:20Z", *out.LastUsed, "Data must be read as microseconds")

	raw, err := json.Marshal(out)
	require.NoError(t, err)
	assert.JSONEq(t, `{"count":42,"lastUsed":"2023-11-14T22:13:20Z"}`, string(raw),
		"DTO must be camelCase with no raw protobuf field names or wire encoding")
}

// TestHandleGetNumscriptUsage_Success drives the handler end-to-end through the
// generated MockBackend: it asserts the writeOK envelope wraps the
// toTemplateUsageJSON payload (count + camelCase lastUsed), and that the handler
// forwards {ledgerName} and {name} to the backend unchanged.
func TestHandleGetNumscriptUsage_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetTemplateUsage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ledger, name string) (*commonpb.TemplateUsage, error) {
			require.Equal(t, "my-ledger", ledger)
			require.Equal(t, "payout", name)

			return &commonpb.TemplateUsage{
				Count:    7,
				LastUsed: &commonpb.Timestamp{Data: 1_700_000_000_000_000},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/numscripts/payout/usage", nil, map[string]string{
		"ledgerName": "my-ledger",
		"name":       "payout",
	})

	srv.handleGetNumscriptUsage(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	wrapper := decodeResponse[BaseResponse[templateUsageJSON]](t, w)
	require.Equal(t, uint64(7), wrapper.Data.Count)
	require.NotNil(t, wrapper.Data.LastUsed)
	assert.Equal(t, "2023-11-14T22:13:20Z", *wrapper.Data.LastUsed)
}

// TestHandleGetNumscriptUsage_NeverInvoked confirms the zero-valued contract: a
// never-invoked template on an existing ledger yields count 0 and an omitted
// lastUsed, not a 404 (the controller returns a zero TemplateUsage).
func TestHandleGetNumscriptUsage_NeverInvoked(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetTemplateUsage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _, _ string) (*commonpb.TemplateUsage, error) {
			return &commonpb.TemplateUsage{}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/numscripts/unused/usage", nil, map[string]string{
		"ledgerName": "my-ledger",
		"name":       "unused",
	})

	srv.handleGetNumscriptUsage(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	wrapper := decodeResponse[BaseResponse[templateUsageJSON]](t, w)
	require.Equal(t, uint64(0), wrapper.Data.Count)
	require.Nil(t, wrapper.Data.LastUsed, "never-invoked template must omit lastUsed")
}

// TestHandleGetNumscriptUsage_MissingName asserts that an empty {name} URL param
// is rejected with 400 INVALID_REQUEST before the backend is ever consulted.
func TestHandleGetNumscriptUsage_MissingName(t *testing.T) {
	t.Parallel()

	// No EXPECT() on the backend: gomock fails the test if GetTemplateUsage is
	// called, proving the handler short-circuits on the missing name.
	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/numscripts//usage", nil, map[string]string{
		"ledgerName": "my-ledger",
		"name":       "",
	})

	srv.handleGetNumscriptUsage(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)

	errResp := decodeResponse[ErrorResponse](t, w)
	assert.Equal(t, "INVALID_REQUEST", errResp.ErrorCode)
}

// TestHandleGetNumscriptUsage_MissingLedgerName asserts requireLedgerName gates
// an empty {ledgerName} with a 400 before touching the backend.
func TestHandleGetNumscriptUsage_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/numscripts/payout/usage", nil, map[string]string{
		"ledgerName": "",
		"name":       "payout",
	})

	srv.handleGetNumscriptUsage(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)

	errResp := decodeResponse[ErrorResponse](t, w)
	assert.Equal(t, "INVALID_REQUEST", errResp.ErrorCode)
}

// TestHandleGetNumscriptUsage_LedgerNotFound confirms the handler routes a
// controller not-found through handleError into a 404.
func TestHandleGetNumscriptUsage_LedgerNotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetTemplateUsage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _, _ string) (*commonpb.TemplateUsage, error) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", "missing")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/missing/numscripts/payout/usage", nil, map[string]string{
		"ledgerName": "missing",
		"name":       "payout",
	})

	srv.handleGetNumscriptUsage(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

// TestHandleGetNumscriptUsage_BackendError confirms a generic backend error is
// sanitized into a 500 via handleError's fallthrough.
func TestHandleGetNumscriptUsage_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetTemplateUsage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _, _ string) (*commonpb.TemplateUsage, error) {
			return nil, errors.New("usage store unavailable")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/numscripts/payout/usage", nil, map[string]string{
		"ledgerName": "my-ledger",
		"name":       "payout",
	})

	srv.handleGetNumscriptUsage(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

// TestHandleGetNumscriptUsage_NoLeaderError confirms a not-a-leader error maps
// to 503 (mirrors the sibling stats handler test).
func TestHandleGetNumscriptUsage_NoLeaderError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetTemplateUsage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _, _ string) (*commonpb.TemplateUsage, error) {
			return nil, commonpb.ErrNoLeader
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/numscripts/payout/usage", nil, map[string]string{
		"ledgerName": "my-ledger",
		"name":       "payout",
	})

	srv.handleGetNumscriptUsage(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// TestHandleGetNumscriptUsage_FullRouteIntegration exercises the route through
// NewHandler + a real HTTP request, proving GET
// /v3/{ledgerName}/numscripts/{name}/usage is wired and the path params reach
// the handler.
func TestHandleGetNumscriptUsage_FullRouteIntegration(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetTemplateUsage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ledger, name string) (*commonpb.TemplateUsage, error) {
			require.Equal(t, "my-ledger", ledger)
			require.Equal(t, "payout", name)

			return &commonpb.TemplateUsage{Count: 3}, nil
		}).AnyTimes()

	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v3/my-ledger/numscripts/payout/usage", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	wrapper := decodeResponse[BaseResponse[templateUsageJSON]](t, w)
	require.Equal(t, uint64(3), wrapper.Data.Count)
}
