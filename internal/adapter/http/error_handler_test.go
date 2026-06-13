package http

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		err            error
		expectedStatus int
		expectedCode   string
		checkRetry     bool
	}{
		{
			name:           "no leader",
			err:            commonpb.ErrNoLeader,
			expectedStatus: http.StatusServiceUnavailable,
			expectedCode:   "NO_LEADER",
			checkRetry:     true,
		},
		{
			name:           "not found error",
			err:            commonpb.NewNotFoundError("item %d", 1),
			expectedStatus: http.StatusNotFound,
			expectedCode:   "NOT_FOUND",
		},
		{
			name:           "ledger already exists",
			err:            &domain.ErrLedgerAlreadyExists{Name: "test"},
			expectedStatus: http.StatusConflict,
			expectedCode:   "LEDGER_ALREADY_EXISTS",
		},
		{
			name:           "ledger not found",
			err:            &domain.ErrLedgerNotFound{Name: "test"},
			expectedStatus: http.StatusNotFound,
			expectedCode:   "LEDGER_NOT_FOUND",
		},
		{
			name:           "transaction reference conflict",
			err:            &domain.ErrTransactionReferenceConflict{Reference: "ref1"},
			expectedStatus: http.StatusConflict,
			expectedCode:   "TRANSACTION_REFERENCE_CONFLICT",
		},
		{
			name:           "idempotency key conflict",
			err:            &domain.ErrIdempotencyKeyConflict{Key: "key1"},
			expectedStatus: http.StatusConflict,
			expectedCode:   "IDEMPOTENCY_KEY_CONFLICT",
		},
		{
			name:           "transaction not found",
			err:            &domain.ErrTransactionNotFound{TransactionID: 42},
			expectedStatus: http.StatusNotFound,
			expectedCode:   "TRANSACTION_NOT_FOUND",
		},
		{
			name:           "transaction already reverted",
			err:            &domain.ErrTransactionAlreadyReverted{TransactionID: 42},
			expectedStatus: http.StatusConflict,
			expectedCode:   "TRANSACTION_ALREADY_REVERTED",
		},
		{
			name:           "insufficient funds",
			err:            &domain.ErrInsufficientFunds{Account: "a", Asset: "USD", Amount: "100", Balance: "50"},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "INSUFFICIENT_FUNDS",
		},
		{
			name:           "balance not found",
			err:            &domain.ErrBalanceNotFound{Account: "a", Asset: "USD"},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "BALANCE_NOT_FOUND",
		},
		{
			name:           "numscript parse error",
			err:            &domain.ErrNumscriptParse{Details: "syntax error"},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "NUMSCRIPT_PARSE_ERROR",
		},
		{
			name:           "numscript dependency discovery failed",
			err:            &domain.ErrDependencyDiscoveryFailed{Cause: errors.New("non-deterministic script")},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "VALIDATION",
		},
		{
			name:           "metadata not found",
			err:            &domain.ErrMetadataNotFound{Target: "account:foo", Key: "bar"},
			expectedStatus: http.StatusNotFound,
			expectedCode:   "METADATA_NOT_FOUND",
		},
		{
			name:           "target required",
			err:            domain.ErrTargetRequired,
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "VALIDATION",
		},
		{
			name:           "metadata key required",
			err:            domain.ErrMetadataKeyRequired,
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "VALIDATION",
		},
		{
			name:           "script required",
			err:            domain.ErrScriptRequired,
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "VALIDATION",
		},
		{
			name:           "unknown error",
			err:            errors.New("something unexpected"),
			expectedStatus: http.StatusInternalServerError,
			expectedCode:   "INTERNAL_ERROR",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/", nil)

			handleError(w, r, tc.err)

			require.Equal(t, tc.expectedStatus, w.Code)

			resp := decodeResponse[ErrorResponse](t, w)
			require.Equal(t, tc.expectedCode, resp.ErrorCode)
			require.NotEmpty(t, resp.ErrorMessage)

			if tc.checkRetry {
				require.Equal(t, "1", w.Header().Get("Retry-After"))
			}
		})
	}
}
