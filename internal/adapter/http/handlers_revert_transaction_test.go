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

func TestHandleRevertTransaction_Success(t *testing.T) {
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
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
											RevertedTransaction: &commonpb.RevertedTransaction{
												RevertTransaction: &commonpb.Transaction{
													Id: 2,
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

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

func TestHandleRevertTransaction_NoLogReturned(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})

	// An apply that returns no log is a backend contract violation; the handler
	// panics (the jsonRecoverer middleware turns this into a 500 in production).
	require.Panics(t, func() {
		srv.handleRevertTransaction(w, r)
	})
}

func TestHandleRevertTransaction_AlreadyReverted(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, &domain.ErrTransactionAlreadyReverted{TransactionID: 1}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusConflict, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "TRANSACTION_ALREADY_REVERTED", resp.ErrorCode)
}

func TestHandleRevertTransaction_InvalidTxID(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/abc/revert", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "abc",
	})

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleRevertTransaction_WithBody(t *testing.T) {
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
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
											RevertedTransaction: &commonpb.RevertedTransaction{
												RevertTransaction: &commonpb.Transaction{
													Id: 2,
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

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"force": true, "atEffectiveDate": true}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})
	r.Header.Set("Content-Length", "42")

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

// revertPayloadFromApply extracts the RevertTransactionPayload that the handler
// forwarded to the backend, so tests can assert the metadata mapping.
func revertPayloadFromApply(t *testing.T, req *servicepb.ApplyRequest) *servicepb.RevertTransactionPayload {
	t.Helper()

	requests := req.GetUnsigned().GetRequests()
	require.Len(t, requests, 1)

	action := requests[0].GetApply().GetAction()
	rt, ok := action.GetData().(*servicepb.LedgerAction_RevertTransaction)
	require.True(t, ok, "expected a revert-transaction action")

	return rt.RevertTransaction
}

func revertBackendReturningLog(t *testing.T, captured **servicepb.RevertTransactionPayload) *MockBackend {
	t.Helper()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			if captured != nil {
				*captured = revertPayloadFromApply(t, req)
			}

			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
											RevertedTransaction: &commonpb.RevertedTransaction{
												RevertTransaction: &commonpb.Transaction{Id: 2},
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

	return backend
}

// TestHandleRevertTransaction_TypedMetadata verifies that typed metadata values
// (string, numeric, boolean, and date-as-string) survive the HTTP revert path
// without being coerced or dropped (EN-1509).
func TestHandleRevertTransaction_TypedMetadata(t *testing.T) {
	t.Parallel()

	var captured *servicepb.RevertTransactionPayload
	backend := revertBackendReturningLog(t, &captured)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"metadata":{"reason":"fraud","count":42,"negative":-7,"active":true,"effectiveAt":"2026-07-11T00:00:00Z"}}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})
	r.ContentLength = int64(body.Len())

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, captured)

	got := commonpb.MetadataToAnyMap(captured.GetMetadata())
	require.Equal(t, "fraud", got["reason"])
	require.EqualValues(t, uint64(42), got["count"])
	require.EqualValues(t, int64(-7), got["negative"])
	require.Equal(t, true, got["active"])
	require.Equal(t, "2026-07-11T00:00:00Z", got["effectiveAt"])
}

// TestHandleRevertTransaction_StringMetadata keeps the plain string-metadata
// contract green alongside the typed one.
func TestHandleRevertTransaction_StringMetadata(t *testing.T) {
	t.Parallel()

	var captured *servicepb.RevertTransactionPayload
	backend := revertBackendReturningLog(t, &captured)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"metadata":{"reason":"duplicate"}}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})
	r.ContentLength = int64(body.Len())

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, captured)

	got := commonpb.MetadataToAnyMap(captured.GetMetadata())
	require.Equal(t, "duplicate", got["reason"])
}

// TestHandleRevertTransaction_InvalidMetadata verifies unsupported metadata
// values (objects/arrays) are rejected with 400 INVALID_REQUEST instead of
// being silently dropped (EN-1509).
func TestHandleRevertTransaction_InvalidMetadata(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"metadata":{"nested":{"not":"allowed"}}}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})
	r.ContentLength = int64(body.Len())

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "INVALID_REQUEST", resp.ErrorCode)
}
