package application

import (
	"errors"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing/numscript"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBusinessErrorToGRPCStatus_LedgerAlreadyExists(t *testing.T) {
	t.Parallel()

	bizErr := &processing.BusinessError{Err: &processing.ErrLedgerAlreadyExists{Name: "my-ledger"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.AlreadyExists, st.Code())
	require.Contains(t, st.Message(), "my-ledger")

	info := extractErrorInfo(t, st)
	require.Equal(t, processing.ErrReasonLedgerAlreadyExists, info.Reason)
	require.Equal(t, errorDomain, info.Domain)
	require.Equal(t, "my-ledger", info.Metadata["name"])
}

func TestBusinessErrorToGRPCStatus_LedgerNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &processing.BusinessError{Err: &processing.ErrLedgerNotFound{Name: "missing-ledger"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.NotFound, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, processing.ErrReasonLedgerNotFound, info.Reason)
	require.Equal(t, "missing-ledger", info.Metadata["name"])
}

func TestBusinessErrorToGRPCStatus_IdempotencyKeyConflict(t *testing.T) {
	t.Parallel()

	bizErr := &processing.BusinessError{Err: &processing.ErrIdempotencyKeyConflict{Key: "ik-123"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.AlreadyExists, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, processing.ErrReasonIdempotencyKeyConflict, info.Reason)
	require.Equal(t, "ik-123", info.Metadata["key"])
}

func TestBusinessErrorToGRPCStatus_TransactionReferenceConflict(t *testing.T) {
	t.Parallel()

	bizErr := &processing.BusinessError{Err: &processing.ErrTransactionReferenceConflict{
		LedgerID:  42,
		Reference: "ref-001",
	}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.AlreadyExists, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, processing.ErrReasonTransactionReferenceConflict, info.Reason)
	require.Equal(t, "42", info.Metadata["ledgerId"])
	require.Equal(t, "ref-001", info.Metadata["reference"])
}

func TestBusinessErrorToGRPCStatus_TransactionNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &processing.BusinessError{Err: &processing.ErrTransactionNotFound{TransactionID: 999}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.NotFound, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, processing.ErrReasonTransactionNotFound, info.Reason)
	require.Equal(t, "999", info.Metadata["transactionId"])
}

func TestBusinessErrorToGRPCStatus_TransactionAlreadyReverted(t *testing.T) {
	t.Parallel()

	bizErr := &processing.BusinessError{Err: &processing.ErrTransactionAlreadyReverted{TransactionID: 42}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, processing.ErrReasonTransactionAlreadyReverted, info.Reason)
	require.Equal(t, "42", info.Metadata["transactionId"])
}

func TestBusinessErrorToGRPCStatus_InsufficientFunds(t *testing.T) {
	t.Parallel()

	bizErr := &processing.BusinessError{Err: &processing.ErrInsufficientFunds{
		Account: "user:001",
		Asset:   "USD",
		Amount:  "1000",
		Balance: "500",
	}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, processing.ErrReasonInsufficientFunds, info.Reason)
	require.Equal(t, "user:001", info.Metadata["account"])
	require.Equal(t, "USD", info.Metadata["asset"])
	require.Equal(t, "1000", info.Metadata["amount"])
	require.Equal(t, "500", info.Metadata["balance"])
}

func TestBusinessErrorToGRPCStatus_BalanceNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &processing.BusinessError{Err: &processing.ErrBalanceNotFound{
		Account: "user:002",
		Asset:   "EUR",
	}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, processing.ErrReasonBalanceNotFound, info.Reason)
	require.Equal(t, "user:002", info.Metadata["account"])
	require.Equal(t, "EUR", info.Metadata["asset"])
}

func TestBusinessErrorToGRPCStatus_BalanceNotPreloaded(t *testing.T) {
	t.Parallel()

	bizErr := &processing.BusinessError{Err: &numscript.ErrBalanceNotPreloaded{
		Account: "user:003",
		Asset:   "BTC",
	}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, processing.ErrReasonBalanceNotPreloaded, info.Reason)
	require.Equal(t, "user:003", info.Metadata["account"])
	require.Equal(t, "BTC", info.Metadata["asset"])
}

func TestBusinessErrorToGRPCStatus_NumscriptParseError(t *testing.T) {
	t.Parallel()

	bizErr := &processing.BusinessError{Err: &numscript.ErrNumscriptParse{Details: "unexpected token at line 3"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.InvalidArgument, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, processing.ErrReasonNumscriptParseError, info.Reason)
	require.Equal(t, "unexpected token at line 3", info.Metadata["details"])
}

func TestBusinessErrorToGRPCStatus_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"target required", processing.ErrTargetRequired},
		{"metadata key required", processing.ErrMetadataKeyRequired},
		{"script required", numscript.ErrScriptRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bizErr := &processing.BusinessError{Err: tt.err}
			st := businessErrorToGRPCStatus(bizErr)

			require.Equal(t, codes.InvalidArgument, st.Code())

			info := extractErrorInfo(t, st)
			require.Equal(t, processing.ErrReasonValidation, info.Reason)
			require.Equal(t, errorDomain, info.Domain)
		})
	}
}

func TestConvertToGRPCError_BusinessError(t *testing.T) {
	t.Parallel()

	bizErr := &processing.BusinessError{Err: &processing.ErrLedgerNotFound{Name: "test"}}
	grpcErr := convertToGRPCError(bizErr)

	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
}

func TestConvertToGRPCError_UnknownError(t *testing.T) {
	t.Parallel()

	err := errors.New("some random error")
	grpcErr := convertToGRPCError(err)

	// Unknown errors pass through as-is
	require.Equal(t, err, grpcErr)
}

// extractErrorInfo extracts the ErrorInfo detail from a gRPC status.
func extractErrorInfo(t *testing.T, st *status.Status) *errdetails.ErrorInfo {
	t.Helper()

	details := st.Details()
	require.NotEmpty(t, details, "expected status to have details")

	for _, detail := range details {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info
		}
	}

	t.Fatal("no ErrorInfo found in status details")
	return nil
}
