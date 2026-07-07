package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// --------------------------------------------------------------------------
// handler.go: NewHandler, contentTypeMiddleware, contentTypeResponseWriter
// --------------------------------------------------------------------------

func TestNewHandler_ReturnsNonNil(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().IsHealthy().Return(true).AnyTimes()
	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
	require.NotNil(t, handler)
}

func TestNewHandler_HealthEndpoint(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().IsHealthy().Return(true).AnyTimes()
	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/health", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
}

func TestNewHandler_LivezEndpoint(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/livez", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestNewHandler_ReadyzEndpoint(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().NotReadyReasons().Return(nil).AnyTimes()
	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestContentTypeMiddleware_SetsJSON(t *testing.T) {
	t.Parallel()

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	handler := contentTypeMiddleware(inner)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
}

func TestContentTypeMiddleware_NoContentType204(t *testing.T) {
	t.Parallel()

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := contentTypeMiddleware(inner)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
	// 204 should not set Content-Type
	require.Empty(t, w.Header().Get("Content-Type"))
}

func TestContentTypeMiddleware_WriteWithoutExplicitHeader(t *testing.T) {
	t.Parallel()

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Write without explicitly calling WriteHeader; should default to 200
		_, _ = w.Write([]byte(`{"data":"test"}`))
	})

	handler := contentTypeMiddleware(inner)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
}

func TestContentTypeMiddleware_ExplicitContentType(t *testing.T) {
	t.Parallel()

	// The middleware detects a pre-existing Content-Type only if Header() is called
	// after Content-Type was already set. If the caller sets Content-Type via
	// w.Header().Set() then calls WriteHeader, the middleware sees contentTypeSet=false
	// because Header() was only called once (before Content-Type was set on the returned map).
	// To trigger the "already set" path, Content-Type must be set before calling Header() again.
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// First call to Header() sets Content-Type
		w.Header().Set("Content-Type", "text/plain")
		// Second call to Header() will detect it was already set
		_ = w.Header()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello"))
	})

	handler := contentTypeMiddleware(inner)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	// The explicit Content-Type should be preserved because contentTypeSet was true
	require.Equal(t, "text/plain", w.Header().Get("Content-Type"))
}

func TestContentTypeMiddleware_DoubleWriteHeader(t *testing.T) {
	t.Parallel()

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		// Second call should be a no-op
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	})

	handler := contentTypeMiddleware(inner)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)

	handler.ServeHTTP(w, r)

	// Should use the first status code
	require.Equal(t, http.StatusOK, w.Code)
}

// --------------------------------------------------------------------------
// handler.go: chiLogFormatter, chiLogEntry
// --------------------------------------------------------------------------

func TestChiLogFormatter_NewLogEntry(t *testing.T) {
	t.Parallel()

	formatter := &chiLogFormatter{logger: logging.Testing()}

	r := httptest.NewRequest(http.MethodGet, "/test", nil)
	entry := formatter.NewLogEntry(r)

	require.NotNil(t, entry)
}

func TestChiLogEntry_Write(t *testing.T) {
	t.Parallel()

	entry := chiLogEntry{
		logger: logging.Testing(),
		ctx:    context.Background(),
	}

	// Should not panic
	entry.Write(200, 42, nil, 100, nil)
}

func TestChiLogEntry_WriteWithExtra(t *testing.T) {
	t.Parallel()

	entry := chiLogEntry{
		logger: logging.Testing(),
		ctx:    context.Background(),
	}

	// Should not panic even with extra data
	entry.Write(500, 0, nil, 100, "some extra info")
}

func TestChiLogEntry_Panic(t *testing.T) {
	t.Parallel()

	entry := chiLogEntry{
		logger: logging.Testing(),
		ctx:    context.Background(),
	}

	// Should not panic when logging a panic
	entry.Panic("test panic value", []byte("fake stack trace"))
}

// --------------------------------------------------------------------------
// handlers_revert_transaction.go: missing paths
// --------------------------------------------------------------------------

func TestHandleRevertTransaction_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/revert", nil, map[string]string{
		"ledgerName":    "",
		"transactionId": "1",
	})

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleRevertTransaction_MissingTransactionID(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/transactions//revert", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "",
	})

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleRevertTransaction_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`not json`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})
	r.ContentLength = int64(len("not json"))

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleRevertTransaction_WithMetadataInBody(t *testing.T) {
	t.Parallel()

	var capturedRequest *servicepb.Request

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			capturedRequest = req.GetUnsigned().GetRequests()[0]

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
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"metadata":{"reason":"fraud"},"force":true,"atEffectiveDate":true}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})
	r.ContentLength = int64(len(`{"metadata":{"reason":"fraud"},"force":true,"atEffectiveDate":true}`))

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, capturedRequest)

	applyType, ok := capturedRequest.GetType().(*servicepb.Request_Apply)
	require.True(t, ok)
	revertData, ok := applyType.Apply.GetAction().GetData().(*servicepb.LedgerAction_RevertTransaction)
	require.True(t, ok)
	revertPayload := revertData.RevertTransaction
	require.True(t, revertPayload.GetForce())
	require.True(t, revertPayload.GetAtEffectiveDate())
	require.NotNil(t, revertPayload.GetMetadata())
}

func TestHandleRevertTransaction_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, errors.New("internal error")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

// --------------------------------------------------------------------------
// handlers_save_account_metadata.go: missing paths
// --------------------------------------------------------------------------

func TestHandleSaveAccountMetadata_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"key":"val"}`)
	r := newRequest(t, http.MethodPost, "/accounts/addr/metadata", body, map[string]string{
		"ledgerName": "",
		"address":    "addr",
	})

	srv.handleSaveAccountMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSaveAccountMetadata_BackendApplyError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, errors.New("apply failed")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"role":"admin"}`)
	r := newRequest(t, http.MethodPost, "/ledger1/accounts/users:001/metadata", body, map[string]string{
		"ledgerName": "ledger1",
		"address":    "users:001",
	})

	srv.handleSaveAccountMetadata(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

// --------------------------------------------------------------------------
// handlers_save_transaction_metadata.go: missing paths
// --------------------------------------------------------------------------

func TestHandleSaveTransactionMetadata_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"key":"val"}`)
	r := newRequest(t, http.MethodPost, "/transactions/1/metadata", body, map[string]string{
		"ledgerName":    "",
		"transactionId": "1",
	})

	srv.handleSaveTransactionMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSaveTransactionMetadata_MissingTransactionID(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"key":"val"}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions//metadata", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "",
	})

	srv.handleSaveTransactionMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSaveTransactionMetadata_BackendApplyError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, errors.New("apply failed")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"category":"refund"}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/metadata", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})

	srv.handleSaveTransactionMetadata(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

// --------------------------------------------------------------------------
// handlers_get_account.go: missing GetAccount error path + missing ledger name
// --------------------------------------------------------------------------

func TestHandleGetAccount_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/accounts/addr", nil, map[string]string{
		"ledgerName": "",
		"address":    "addr",
	})

	srv.handleGetAccount(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetAccount_GetAccountError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	backend.EXPECT().GetAccount(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ string) (*commonpb.Account, error) {
			return nil, errors.New("storage error")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts/users:001", nil, map[string]string{
		"ledgerName": "ledger1",
		"address":    "users:001",
	})

	srv.handleGetAccount(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

// --------------------------------------------------------------------------
// handlers_get_transaction.go: missing transactionId empty
// --------------------------------------------------------------------------

func TestHandleGetTransaction_MissingTransactionID(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/transactions/", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "",
	})

	srv.handleGetTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetTransaction_LedgerLookupError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return nil, &domain.ErrLedgerNotFound{Name: "missing"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/missing/transactions/1", nil, map[string]string{
		"ledgerName":    "missing",
		"transactionId": "1",
	})

	srv.handleGetTransaction(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

// --------------------------------------------------------------------------
// handlers_delete_account_metadata.go: missing paths
// --------------------------------------------------------------------------

func TestHandleDeleteAccountMetadata_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/accounts/addr/metadata/key", nil, map[string]string{
		"ledgerName": "",
		"address":    "addr",
		"key":        "key",
	})

	srv.handleDeleteAccountMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleDeleteAccountMetadata_MissingAddress(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/accounts//metadata/key", nil, map[string]string{
		"ledgerName": "ledger1",
		"address":    "",
		"key":        "key",
	})

	srv.handleDeleteAccountMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

// --------------------------------------------------------------------------
// handlers_delete_transaction_metadata.go: missing paths
// --------------------------------------------------------------------------

func TestHandleDeleteTransactionMetadata_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/transactions/1/metadata/key", nil, map[string]string{
		"ledgerName":    "",
		"transactionId": "1",
		"key":           "key",
	})

	srv.handleDeleteTransactionMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleDeleteTransactionMetadata_MissingTransactionID(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/transactions//metadata/key", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "",
		"key":           "key",
	})

	srv.handleDeleteTransactionMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

// --------------------------------------------------------------------------
// handlers_set_metadata_type.go: missing paths (Apply error, missing ledger)
// --------------------------------------------------------------------------

func TestHandleSetMetadataType_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"type":"string"}`)
	r := newRequest(t, http.MethodPut, "/metadata-schema/account/key", body, map[string]string{
		"ledgerName": "",
		"targetType": "account",
		"key":        "key",
	})

	srv.handleSetMetadataType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSetMetadataType_BackendApplyError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, errors.New("apply failed")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"type":"string"}`)
	r := newRequest(t, http.MethodPut, "/ledger1/metadata-schema/account/key", body, map[string]string{
		"ledgerName": "ledger1",
		"targetType": "account",
		"key":        "key",
	})

	srv.handleSetMetadataType(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

// --------------------------------------------------------------------------
// handlers_remove_metadata_type.go: missing paths
// --------------------------------------------------------------------------

func TestHandleRemoveMetadataType_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/metadata-schema/account/key", nil, map[string]string{
		"ledgerName": "",
		"targetType": "account",
		"key":        "key",
	})

	srv.handleRemoveMetadataType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleRemoveMetadataType_BackendApplyError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, errors.New("apply failed")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/metadata-schema/account/key", nil, map[string]string{
		"ledgerName": "ledger1",
		"targetType": "account",
		"key":        "key",
	})

	srv.handleRemoveMetadataType(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

// --------------------------------------------------------------------------
// handlers_list_accounts.go: missing backend error, cursor iteration error
// --------------------------------------------------------------------------

func TestHandleListAccounts_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ string, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
			return nil, commonpb.ErrNoLeader
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestHandleListAccounts_CursorIterationError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ string, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
			return &errorCursor[*commonpb.Account]{err: errors.New("cursor iteration failed")}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleListAccounts_WithPrefix(t *testing.T) {
	t.Parallel()

	var capturedFilter *commonpb.QueryFilter

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAccounts(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ string, filter *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
			capturedFilter = filter

			return cursor.NewSliceCursor[*commonpb.Account](nil), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts?prefix=users:", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, capturedFilter)
	require.Equal(t, "users:", capturedFilter.GetAddress().GetHardcodedPrefix())
}

// --------------------------------------------------------------------------
// handlers_list_all_ledgers.go: cursor iteration error
// --------------------------------------------------------------------------

func TestHandleListAllLedgers_CursorIterationError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListLedgers(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*commonpb.LedgerInfo], error) {
			return &errorCursor[*commonpb.LedgerInfo]{err: errors.New("cursor error")}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/", nil, nil)

	srv.handleListAllLedgers(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

// --------------------------------------------------------------------------
// handlers_bulk.go: convertBulkElementToRequest, runBulk dispatch, writeBulkResponse
// --------------------------------------------------------------------------

func TestConvertBulkElementToRequest(t *testing.T) {
	t.Parallel()

	elem := &servicepb.BulkElement{
		IdempotencyKey: "ik-123",
		Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{},
			},
		},
	}

	// convertBulkElementToRequest only builds the Request; the element's
	// idempotency key is collected separately and applied at the batch level.
	req := convertBulkElementToRequest("ledger1", elem)

	require.NotNil(t, req)
	applyType, ok := req.GetType().(*servicepb.Request_Apply)
	require.True(t, ok)
	applyReq := applyType.Apply
	require.Equal(t, "ledger1", applyReq.GetLedger())
}

func TestRunBulk_EmptyElements(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	results := srv.runBulk(context.Background(), "ledger1", nil, bulkOptions{})
	require.Nil(t, results)
}

func TestRunBulk_Atomic(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			reqs := req.GetUnsigned().GetRequests()
			logs := make([]*commonpb.Log, len(reqs))
			for i := range reqs {
				logs[i] = &commonpb.Log{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{Id: uint64(i + 1)},
							},
						},
					},
				}
			}

			return logs, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	elements := []*servicepb.BulkElement{
		{Action: &servicepb.LedgerAction{}},
		{Action: &servicepb.LedgerAction{}},
	}

	results := srv.runBulk(context.Background(), "ledger1", elements, bulkOptions{atomic: true})

	require.Len(t, results, 2)

	for _, r := range results {
		require.NoError(t, r.err)
	}
}

func TestRunBulk_Sequential(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{Id: 1},
							},
						},
					},
				},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	elements := []*servicepb.BulkElement{
		{Action: &servicepb.LedgerAction{}},
	}

	results := srv.runBulk(context.Background(), "ledger1", elements, bulkOptions{atomic: false})

	require.Len(t, results, 1)
	require.NoError(t, results[0].err)
}

func TestWriteBulkResponse_WithErrors(t *testing.T) {
	t.Parallel()

	elements := []*servicepb.BulkElement{
		{Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{},
			},
		}},
		{Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{},
			},
		}},
	}

	results := []bulkResult{
		{err: domain.ErrEmptyTransaction},
		{
			log: &commonpb.LedgerLog{
				Id: 2,
				Data: &commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
						CreatedTransaction: &commonpb.CreatedTransaction{
							Transaction: &commonpb.Transaction{Id: 2},
						},
					},
				},
			},
		},
	}

	w := httptest.NewRecorder()
	writeBulkResponse(w, elements, results, false)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestWriteBulkResponse_ContinueOnFailure(t *testing.T) {
	t.Parallel()

	elements := []*servicepb.BulkElement{
		{Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{},
			},
		}},
		{Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{},
			},
		}},
	}

	results := []bulkResult{
		{err: domain.ErrEmptyTransaction},
		{
			log: &commonpb.LedgerLog{
				Id: 2,
				Data: &commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
						CreatedTransaction: &commonpb.CreatedTransaction{
							Transaction: &commonpb.Transaction{Id: 2},
						},
					},
				},
			},
		},
	}

	w := httptest.NewRecorder()
	writeBulkResponse(w, elements, results, true)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestWriteBulkResponse_AllSuccess(t *testing.T) {
	t.Parallel()

	elements := []*servicepb.BulkElement{
		{Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{},
			},
		}},
	}

	results := []bulkResult{
		{
			log: &commonpb.LedgerLog{
				Id: 1,
				Data: &commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
						CreatedTransaction: &commonpb.CreatedTransaction{
							Transaction: &commonpb.Transaction{Id: 1},
						},
					},
				},
			},
		},
	}

	w := httptest.NewRecorder()
	writeBulkResponse(w, elements, results, false)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestWriteBulkResponse_NilLog(t *testing.T) {
	t.Parallel()

	elements := []*servicepb.BulkElement{
		{Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_AddMetadata{},
		}},
	}

	results := []bulkResult{
		{log: nil},
	}

	w := httptest.NewRecorder()
	writeBulkResponse(w, elements, results, false)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestWriteBulkErrorResponse_NilError(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	writeBulkErrorResponse(w, http.StatusBadRequest, "VALIDATION", nil)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleBulk_WithAtomicFlag(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			reqs := req.GetUnsigned().GetRequests()
			logs := make([]*commonpb.Log, len(reqs))
			for i := range reqs {
				logs[i] = &commonpb.Log{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{
									Id: uint64(i + 1),
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
											CreatedTransaction: &commonpb.CreatedTransaction{
												Transaction: &commonpb.Transaction{Id: uint64(i + 1)},
											},
										},
									},
								},
							},
						},
					},
				}
			}

			return logs, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`[{"action":"CREATE_TRANSACTION","data":{"script":{"plain":"send [USD 100] (source = @world destination = @users:001)"}}}]`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk?atomic=true", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleBulk_WithContinueOnFailure(t *testing.T) {
	t.Parallel()

	callCount := 0
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			callCount++
			if callCount == 1 {
				// Domain-level (Describable) business error — the caller
				// opted into continuing on this kind of per-element
				// failure. Non-domain (infra) errors are asserted
				// separately, see TestHandleBulk_InfraErrorNotSwallowed.
				return nil, domain.ErrEmptyTransaction
			}

			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{
									Id: 2,
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
											CreatedTransaction: &commonpb.CreatedTransaction{
												Transaction: &commonpb.Transaction{Id: 2},
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
	body := strings.NewReader(`[{"action":"CREATE_TRANSACTION","data":{"script":{"plain":"a"}}},{"action":"CREATE_TRANSACTION","data":{"script":{"plain":"b"}}}]`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk?continueOnFailure=true", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleBulk(w, r)

	// continueOnFailure=true → per-element domain failures don't turn the
	// request itself into a top-level failure. Response stays 200 and the
	// caller reads per-element errorCode.
	require.Equal(t, http.StatusOK, w.Code)
}

// TestWriteBulkResponse_AbortedElementsDontEscalate asserts that
// runBulkSequential's context.Canceled sentinel (marking elements skipped
// after a prior failed with continueOnFailure=false) never turns the
// top-level status into 500. The originating domain error alone drives the
// 400 rollup; the skipped tail contributes nothing.
func TestWriteBulkResponse_AbortedElementsDontEscalate(t *testing.T) {
	t.Parallel()

	elements := []*servicepb.BulkElement{
		{Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{},
			},
		}},
		{Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{},
			},
		}},
	}

	results := []bulkResult{
		{err: domain.ErrEmptyTransaction},
		{err: context.Canceled}, // skipped by runBulkSequential
	}

	w := httptest.NewRecorder()
	writeBulkResponse(w, elements, results, false)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestHandleBulk_InfraErrorNotSwallowed asserts that continueOnFailure=true
// does NOT swallow an infrastructure-level Apply error (any non-Describable
// error, e.g. leader loss, cache-horizon exceeded, transport timeout). Those
// mean the request could not complete deterministically and the caller must
// see a 5xx regardless of the continueOnFailure opt-in.
func TestHandleBulk_InfraErrorNotSwallowed(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, errors.New("boom: pebble store unavailable")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`[{"action":"CREATE_TRANSACTION","data":{"script":{"plain":"a"}}}]`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk?continueOnFailure=true", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

// --------------------------------------------------------------------------
// handlers_get_metadata_schema.go: toSchemaStatusJSON with transaction fields
// --------------------------------------------------------------------------

func TestToSchemaStatusJSON_WithTransactionFields(t *testing.T) {
	t.Parallel()

	resp := &servicepb.GetMetadataSchemaStatusResponse{
		AccountFields: map[string]*servicepb.MetadataFieldStatus{
			"role": {DeclaredType: commonpb.MetadataType_METADATA_TYPE_STRING},
		},
		TransactionFields: map[string]*servicepb.MetadataFieldStatus{
			"category": {DeclaredType: commonpb.MetadataType_METADATA_TYPE_STRING},
		},
	}

	result := toSchemaStatusJSON(resp)

	require.Len(t, result.AccountFields, 1)
	require.Len(t, result.TransactionFields, 1)
	require.Equal(t, "string", result.AccountFields["role"].DeclaredType)
	require.Equal(t, "string", result.TransactionFields["category"].DeclaredType)
}

// --------------------------------------------------------------------------
// server.go: NewServer
// --------------------------------------------------------------------------

func TestNewServer(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	srv := NewServer(logging.Testing(), backend, internalauth.AuthConfig{}, 100)

	require.NotNil(t, srv)
	require.Equal(t, 100, srv.bulkMaxSize)
}

// --------------------------------------------------------------------------
// handler.go: NewHandler with routing - full integration tests
// --------------------------------------------------------------------------

func TestNewHandler_CreateLedgerRoute(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_CreateLedger{
							CreateLedger: &commonpb.CreatedLedgerLog{
								Name: "test",
							},
						},
					},
				},
			}, nil
		}).AnyTimes()

	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v3/test", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

func TestNewHandler_GetLedgerRoute(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, name string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: name}, nil
		}).AnyTimes()

	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v3/my-ledger", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestNewHandler_ListAllLedgersRoute(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListLedgers(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*commonpb.LedgerInfo], error) {
			return cursor.NewSliceCursor[*commonpb.LedgerInfo](nil), nil
		}).AnyTimes()

	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v3/", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

// --------------------------------------------------------------------------
// errorCursor helper for testing cursor iteration errors
// --------------------------------------------------------------------------

// errorCursor is a cursor that returns an error on the first Next() call.
type errorCursor[T any] struct {
	err error
}

func (c *errorCursor[T]) Next() (T, error) {
	var zero T

	return zero, c.err
}

func (c *errorCursor[T]) Close() error {
	return nil
}

var _ cursor.Cursor[any] = (*errorCursor[any])(nil)

// --------------------------------------------------------------------------
// handlers_bulk.go: writeBulkResponse with log that has Data with created transaction
// --------------------------------------------------------------------------

func TestWriteBulkResponse_LogWithCreatedTransaction(t *testing.T) {
	t.Parallel()

	elements := []*servicepb.BulkElement{
		{Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{},
			},
		}},
	}

	results := []bulkResult{
		{
			log: &commonpb.LedgerLog{
				Id: 1,
				Data: &commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
						CreatedTransaction: &commonpb.CreatedTransaction{
							Transaction: &commonpb.Transaction{Id: 42},
						},
					},
				},
			},
		},
	}

	w := httptest.NewRecorder()
	writeBulkResponse(w, elements, results, false)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "CREATE_TRANSACTION")
}

func TestWriteBulkResponse_LogWithoutData(t *testing.T) {
	t.Parallel()

	elements := []*servicepb.BulkElement{
		{Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_AddMetadata{},
		}},
	}

	results := []bulkResult{
		{
			log: &commonpb.LedgerLog{
				Id:   1,
				Data: nil,
			},
		},
	}

	w := httptest.NewRecorder()
	writeBulkResponse(w, elements, results, false)

	require.Equal(t, http.StatusOK, w.Code)
}

// --------------------------------------------------------------------------
// handlers_bulk.go: handleBulk with invalid element parsing
// --------------------------------------------------------------------------

func TestHandleBulk_InvalidElementParsing(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	// Array of raw elements where second element has invalid action
	body := strings.NewReader(`[{"action":"CREATE_TRANSACTION","data":{"script":{"plain":"a"}}}, {"action":"INVALID_ACTION_TYPE","data":{}}]`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleBulk(w, r)

	// Elements are valid JSON so they should parse; the action type validation
	// happens at the handler level or during execution
	// This test verifies the parsing path completes
	require.True(t, w.Code == http.StatusOK || w.Code == http.StatusBadRequest)
}

// --------------------------------------------------------------------------
// handlers_save_account_metadata.go: invalid metadata conversion error
// --------------------------------------------------------------------------

func TestHandleSaveAccountMetadata_InvalidMetadataType(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	// Float values (non-integer) are not supported and trigger MetadataFromAnyMap error
	body := strings.NewReader(`{"price":1.5}`)
	r := newRequest(t, http.MethodPost, "/ledger1/accounts/users:001/metadata", body, map[string]string{
		"ledgerName": "ledger1",
		"address":    "users:001",
	})

	srv.handleSaveAccountMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "INVALID_REQUEST", resp.ErrorCode)
	require.Contains(t, resp.ErrorMessage, "invalid metadata")
}

// --------------------------------------------------------------------------
// handlers_save_transaction_metadata.go: invalid metadata conversion error
// --------------------------------------------------------------------------

func TestHandleSaveTransactionMetadata_InvalidMetadataType(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	// Float values (non-integer) trigger MetadataFromAnyMap error
	body := strings.NewReader(`{"amount":3.14}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/metadata", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})

	srv.handleSaveTransactionMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "INVALID_REQUEST", resp.ErrorCode)
	require.Contains(t, resp.ErrorMessage, "invalid metadata")
}

// --------------------------------------------------------------------------
// handler.go: chiLogFormatter.NewLogEntry with valid trace span
// --------------------------------------------------------------------------

func TestChiLogFormatter_NewLogEntry_WithSpan(t *testing.T) {
	t.Parallel()

	formatter := &chiLogFormatter{logger: logging.Testing()}

	// Create a context with a valid span using the noop tracer
	tracer := noop.NewTracerProvider().Tracer("test")

	ctx, span := tracer.Start(context.Background(), "test-span")
	defer span.End()

	r := httptest.NewRequest(http.MethodGet, "/test", nil)
	r = r.WithContext(ctx)

	entry := formatter.NewLogEntry(r)
	require.NotNil(t, entry)
}

// --------------------------------------------------------------------------
// handler.go: chiLogEntry.Panic with valid trace span
// --------------------------------------------------------------------------

func TestChiLogEntry_Panic_WithSpan(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")

	ctx, span := tracer.Start(context.Background(), "test-span")
	defer span.End()

	entry := chiLogEntry{
		logger: logging.Testing(),
		ctx:    ctx,
	}

	// Should not panic and should record the error on the span
	entry.Panic(errors.New("test panic"), []byte("stack trace data"))
}

// --------------------------------------------------------------------------
// handlers_create_ledger.go: Idempotency-Key propagation
// --------------------------------------------------------------------------

func TestHandleCreateLedger_IdempotencyKeyPropagated(t *testing.T) {
	t.Parallel()

	var capturedBatch *servicepb.ApplyBatch

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			capturedBatch = req.GetUnsigned()

			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_CreateLedger{
							CreateLedger: &commonpb.CreatedLedgerLog{
								Name: "test",
							},
						},
					},
				},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/test", nil, map[string]string{
		"ledgerName": "test",
	})
	r.Header.Set("Idempotency-Key", "create-ledger-ik-123")

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, capturedBatch)
	require.Equal(t, "create-ledger-ik-123", capturedBatch.GetIdempotencyKey())
}

// --------------------------------------------------------------------------
// handlers_delete_ledger.go: Idempotency-Key propagation
// --------------------------------------------------------------------------

func TestHandleDeleteLedger_IdempotencyKeyPropagated(t *testing.T) {
	t.Parallel()

	var capturedBatch *servicepb.ApplyBatch

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			capturedBatch = req.GetUnsigned()

			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_DeleteLedger{},
					},
				},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/test", nil, map[string]string{
		"ledgerName": "test",
	})
	r.Header.Set("Idempotency-Key", "delete-ledger-ik-456")

	srv.handleDeleteLedger(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
	require.NotNil(t, capturedBatch)
	require.Equal(t, "delete-ledger-ik-456", capturedBatch.GetIdempotencyKey())
}
