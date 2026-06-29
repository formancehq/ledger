package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleAnalyzeAccounts_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ledgerName string, variableThreshold uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
			require.Equal(t, "my-ledger", ledgerName)
			require.Equal(t, uint32(0), variableThreshold)

			return &servicepb.AnalyzeAccountsResponse{
				TotalAccounts: 42,
				Patterns: []*servicepb.AccountPattern{
					{
						Pattern:      "users:{user_id}",
						AccountCount: 20,
						Assets:       []string{"USD", "EUR"},
						MetadataKeys: []string{"role"},
						Segments: []*servicepb.PatternSegment{
							{Position: 0, Type: servicepb.PatternSegmentType_PATTERN_SEGMENT_TYPE_FIXED, FixedValue: "users"},
							{Position: 1, Type: servicepb.PatternSegmentType_PATTERN_SEGMENT_TYPE_VARIABLE, VariableName: "user_id", InferredPattern: "^[a-f0-9-]+$", UniqueValues: 20, Examples: []string{"abc-123"}},
						},
					},
				},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/analyze-accounts", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAnalyzeAccounts(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	wrapper := decodeResponse[BaseResponse[analyzeAccountsResponseJSON]](t, w)
	resp := wrapper.Data
	require.Equal(t, uint64(42), resp.TotalAccounts)

	require.Len(t, resp.Patterns, 1)
	require.Equal(t, "users:{user_id}", resp.Patterns[0].Pattern)
	require.Equal(t, uint64(20), resp.Patterns[0].AccountCount)
	require.Equal(t, []string{"USD", "EUR"}, resp.Patterns[0].Assets)
	require.Equal(t, []string{"role"}, resp.Patterns[0].MetadataKeys)
	require.Len(t, resp.Patterns[0].Segments, 2)
	require.Equal(t, "fixed", resp.Patterns[0].Segments[0].Type)
	require.Equal(t, "variable", resp.Patterns[0].Segments[1].Type)
}

func TestHandleAnalyzeAccounts_WithThreshold(t *testing.T) {
	t.Parallel()

	var capturedThreshold uint32

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, variableThreshold uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
			capturedThreshold = variableThreshold

			return &servicepb.AnalyzeAccountsResponse{
				TotalAccounts: 0,
				Patterns:      nil,
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/analyze-accounts?variableThreshold=25", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAnalyzeAccounts(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, uint32(25), capturedThreshold)
}

func TestHandleAnalyzeAccounts_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/analyze-accounts", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleAnalyzeAccounts(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAnalyzeAccounts_InvalidThreshold(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/analyze-accounts?variableThreshold=abc", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAnalyzeAccounts(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "INVALID_REQUEST", resp.ErrorCode)
	require.Contains(t, resp.ErrorMessage, "variableThreshold")
}

func TestHandleAnalyzeAccounts_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
			return nil, errors.New("internal error")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/analyze-accounts", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAnalyzeAccounts(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleAnalyzeAccounts_LedgerNotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
			return nil, &domain.ErrLedgerNotFound{Name: "missing"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/missing/analyze-accounts", nil, map[string]string{
		"ledgerName": "missing",
	})

	srv.handleAnalyzeAccounts(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleAnalyzeAccounts_EmptyResponse(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
			return &servicepb.AnalyzeAccountsResponse{
				TotalAccounts: 0,
				Patterns:      nil,
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/empty-ledger/analyze-accounts", nil, map[string]string{
		"ledgerName": "empty-ledger",
	})

	srv.handleAnalyzeAccounts(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	wrapper := decodeResponse[BaseResponse[analyzeAccountsResponseJSON]](t, w)
	require.Equal(t, uint64(0), wrapper.Data.TotalAccounts)
	require.Empty(t, wrapper.Data.Patterns)
}

func TestHandleAnalyzeAccounts_NoLeaderError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
			return nil, commonpb.ErrNoLeader
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/analyze-accounts", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAnalyzeAccounts(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// TestHandleAnalyzeAccounts_FullRouteIntegration tests that the route is correctly
// registered in NewHandler and accessible via a full HTTP request.
func TestHandleAnalyzeAccounts_FullRouteIntegration(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
			return &servicepb.AnalyzeAccountsResponse{
				TotalAccounts: 5,
				Patterns:      []*servicepb.AccountPattern{},
			}, nil
		}).AnyTimes()

	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v3/my-ledger/analyze-accounts", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}
