package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// bulkWriteBody is a single-element bulk payload whose action (CREATE_TRANSACTION)
// requires the ledger:TransactionWrite granular scope.
const bulkWriteBody = `[{"action":"CREATE_TRANSACTION","data":{"postings":[{"source":"world","destination":"bank","amount":100,"asset":"USD/2"}]}}]`

func TestHandleBulk_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`not json`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleBulk_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`[]`)
	r := newRequest(t, http.MethodPost, "/bulk", body, map[string]string{
		"ledgerName": "",
	})

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleBulk_SizeLimitExceeded(t *testing.T) {
	t.Parallel()

	srv := newTestServerWithBulkLimit(t, NewMockBackend(gomock.NewController(t)), 1)

	// Two elements but limit is 1
	w := httptest.NewRecorder()
	body := strings.NewReader(`[
		{"action":"CREATE_TRANSACTION","data":{"postings":[{"source":"world","destination":"bank","amount":100,"asset":"USD/2"}]}},
		{"action":"CREATE_TRANSACTION","data":{"postings":[{"source":"world","destination":"bank","amount":100,"asset":"USD/2"}]}}
	]`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
}

// TestHandleBulk_OrderSkippedSurfacesInResponse pins the contract for the
// shared CreateTransactionPayload: a bulk CREATE_TRANSACTION item that
// opts into `skippableReasons` and triggers a whitelisted business
// failure must surface as a structured OrderSkippedResponse in the bulk
// result's `data` field (not the legacy null that dropped the skip
// reason/context).
func TestHandleBulk_OrderSkippedSurfacesInResponse(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{
									Id: 17,
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_OrderSkipped{
											OrderSkipped: &commonpb.OrderSkippedLog{
												Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
												Context: map[string]string{
													"reference":             "dup",
													"existingTransactionId": "42",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	body := strings.NewReader(`[
		{"action":"CREATE_TRANSACTION","data":{"reference":"dup"},"skippableReasons":["TRANSACTION_REFERENCE_CONFLICT"]}
	]`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk", body, map[string]string{
		"ledgerName": "ledger1",
	})
	w := httptest.NewRecorder()

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	resp := decodeResponse[bulkResponse](t, w)
	require.Len(t, resp.Data, 1)
	require.Equal(t, "CREATE_TRANSACTION", resp.Data[0].ResponseType)
	require.Equal(t, uint64(17), resp.Data[0].LogID)
	require.NotNil(t, resp.Data[0].Data)

	// Data is unmarshalled as a map[string]any (interface{} round-trip).
	skip, ok := resp.Data[0].Data.(map[string]any)
	require.True(t, ok, "Data must be the structured OrderSkippedResponse shape (got %T)", resp.Data[0].Data)
	require.Equal(t, true, skip["skipped"])
	require.Equal(t, "TRANSACTION_REFERENCE_CONFLICT", skip["reason"])
}

func TestHandleBulk_EmptyArray(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`[]`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestRunBulkAtomic_AllFail(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("atomic failure")
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, expectedErr
		}).AnyTimes()
	srv := newTestServer(t, backend)

	requests := []*servicepb.Request{{}, {}}
	results := srv.runBulkAtomic(context.Background(), "", requests)

	require.Len(t, results, 2)

	for _, r := range results {
		require.ErrorIs(t, r.err, expectedErr)
	}
}

func TestRunBulkAtomic_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Id: 1}}}}},
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Id: 2}}}}},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	requests := []*servicepb.Request{{}, {}}
	results := srv.runBulkAtomic(context.Background(), "", requests)

	require.Len(t, results, 2)

	for _, r := range results {
		require.NoError(t, r.err)
		require.NotNil(t, r.log)
	}
}

func TestRunBulkSequential_StopOnError(t *testing.T) {
	t.Parallel()

	callCount := 0
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			callCount++
			if callCount == 1 {
				return nil, errors.New("first fails")
			}

			return []*commonpb.Log{
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{}}}}},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	requests := []*servicepb.Request{{}, {}, {}}
	keys := []string{"", "", ""}
	results := srv.runBulkSequential(context.Background(), requests, keys, false)

	require.Len(t, results, 3)
	require.Error(t, results[0].err)
	require.ErrorIs(t, results[1].err, context.Canceled)
	require.ErrorIs(t, results[2].err, context.Canceled)
}

func TestRunBulkSequential_ContinueOnFailure(t *testing.T) {
	t.Parallel()

	callCount := 0
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			callCount++
			if callCount == 1 {
				return nil, errors.New("first fails")
			}

			return []*commonpb.Log{
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{}}}}},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	requests := []*servicepb.Request{{}, {}}
	keys := []string{"", ""}
	results := srv.runBulkSequential(context.Background(), requests, keys, true)

	require.Len(t, results, 2)
	require.Error(t, results[0].err)
	require.NoError(t, results[1].err)
}

// TestHandleBulk_AuthEnabled_NoToken_Unauthorized covers the unauthenticated
// write path: auth is enabled with the default mapping (no anonymous scopes),
// the request carries no bearer token, and a write element must be rejected with
// 401 before reaching the backend. The mock backend has no Apply expectation, so
// any call to it fails the test.
func TestHandleBulk_AuthEnabled_NoToken_Unauthorized(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	authCfg := internalauth.AuthConfig{
		Enabled:      true,
		ScopeMapping: internalauth.DefaultMapping("ledger"),
	}
	handler := NewHandler(logging.Testing(), backend, authCfg, version.Info{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v3/ledger1/bulk", strings.NewReader(bulkWriteBody))

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.Equal(t, "UNAUTHENTICATED", decodeResponse[bulkResponse](t, w).ErrorCode)
}

// TestHandleBulk_AuthDisabled_NoToken_Allowed is the control for the case above:
// with auth disabled the same no-token write must pass through to the backend.
func TestHandleBulk_AuthDisabled_NoToken_Allowed(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{}}}}},
			}, nil
		}).Times(1)
	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v3/ledger1/bulk", strings.NewReader(bulkWriteBody))

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}
