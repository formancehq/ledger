package application

import (
	"errors"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/application/admission"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing/numscript"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBusinessErrorToGRPCStatus_LedgerAlreadyExists(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrLedgerAlreadyExists{Name: "my-ledger"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.AlreadyExists, st.Code())
	require.Contains(t, st.Message(), "my-ledger")

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonLedgerAlreadyExists, info.Reason)
	require.Equal(t, errorDomain, info.Domain)
	require.Equal(t, "my-ledger", info.Metadata["name"])
}

func TestBusinessErrorToGRPCStatus_LedgerNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrLedgerNotFound{Name: "missing-ledger"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.NotFound, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonLedgerNotFound, info.Reason)
	require.Equal(t, "missing-ledger", info.Metadata["name"])
}

func TestBusinessErrorToGRPCStatus_IdempotencyKeyConflict(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrIdempotencyKeyConflict{Key: "ik-123"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.AlreadyExists, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonIdempotencyKeyConflict, info.Reason)
	require.Equal(t, "ik-123", info.Metadata["key"])
}

func TestBusinessErrorToGRPCStatus_TransactionReferenceConflict(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrTransactionReferenceConflict{
		Ledger:    "test",
		Reference: "ref-001",
	}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.AlreadyExists, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonTransactionReferenceConflict, info.Reason)
	require.Equal(t, "test", info.Metadata["ledger"])
	require.Equal(t, "ref-001", info.Metadata["reference"])
}

func TestBusinessErrorToGRPCStatus_TransactionNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrTransactionNotFound{TransactionID: 999}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.NotFound, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonTransactionNotFound, info.Reason)
	require.Equal(t, "999", info.Metadata["transactionId"])
}

func TestBusinessErrorToGRPCStatus_TransactionAlreadyReverted(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrTransactionAlreadyReverted{TransactionID: 42}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonTransactionAlreadyReverted, info.Reason)
	require.Equal(t, "42", info.Metadata["transactionId"])
}

func TestBusinessErrorToGRPCStatus_InsufficientFunds(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrInsufficientFunds{
		Account: "user:001",
		Asset:   "USD",
		Amount:  "1000",
		Balance: "500",
	}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonInsufficientFunds, info.Reason)
	require.Equal(t, "user:001", info.Metadata["account"])
	require.Equal(t, "USD", info.Metadata["asset"])
	require.Equal(t, "1000", info.Metadata["amount"])
	require.Equal(t, "500", info.Metadata["balance"])
}

func TestBusinessErrorToGRPCStatus_BalanceNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrBalanceNotFound{
		Account: "user:002",
		Asset:   "EUR",
	}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonBalanceNotFound, info.Reason)
	require.Equal(t, "user:002", info.Metadata["account"])
	require.Equal(t, "EUR", info.Metadata["asset"])
}

func TestBusinessErrorToGRPCStatus_BalanceNotPreloaded(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &numscript.ErrBalanceNotPreloaded{
		Account: "user:003",
		Asset:   "BTC",
	}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonBalanceNotPreloaded, info.Reason)
	require.Equal(t, "user:003", info.Metadata["account"])
	require.Equal(t, "BTC", info.Metadata["asset"])
}

func TestBusinessErrorToGRPCStatus_NumscriptParseError(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &numscript.ErrNumscriptParse{Details: "unexpected token at line 3"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.InvalidArgument, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonNumscriptParseError, info.Reason)
	require.Equal(t, "unexpected token at line 3", info.Metadata["details"])
}

func TestBusinessErrorToGRPCStatus_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"target required", domain.ErrTargetRequired},
		{"metadata key required", domain.ErrMetadataKeyRequired},
		{"script required", numscript.ErrScriptRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bizErr := &domain.BusinessError{Err: tt.err}
			st := businessErrorToGRPCStatus(bizErr)

			require.Equal(t, codes.InvalidArgument, st.Code())

			info := extractErrorInfo(t, st)
			require.Equal(t, domain.ErrReasonValidation, info.Reason)
			require.Equal(t, errorDomain, info.Domain)
		})
	}
}

func TestBusinessErrorToGRPCStatus_SinkAlreadyExists(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrSinkAlreadyExists{Name: "my-sink"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.AlreadyExists, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonSinkAlreadyExists, info.Reason)
	require.Equal(t, "my-sink", info.Metadata["name"])
}

func TestBusinessErrorToGRPCStatus_SinkNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrSinkNotFound{Name: "missing-sink"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.NotFound, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonSinkNotFound, info.Reason)
	require.Equal(t, "missing-sink", info.Metadata["name"])
}

func TestBusinessErrorToGRPCStatus_MetadataNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrMetadataNotFound{Target: "account:foo", Key: "role"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.NotFound, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonMetadataNotFound, info.Reason)
	require.Equal(t, "account:foo", info.Metadata["target"])
	require.Equal(t, "role", info.Metadata["key"])
}

func TestBusinessErrorToGRPCStatus_NoPeriodOpen(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: domain.ErrNoPeriodOpen}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonNoPeriodOpen, info.Reason)
}

func TestBusinessErrorToGRPCStatus_PeriodAlreadyClosing(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: domain.ErrPeriodAlreadyClosing}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonPeriodAlreadyClosing, info.Reason)
}

func TestBusinessErrorToGRPCStatus_PeriodNotFound(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrPeriodNotFound{PeriodID: 7}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.NotFound, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonPeriodNotFound, info.Reason)
	require.Equal(t, "7", info.Metadata["periodId"])
}

func TestBusinessErrorToGRPCStatus_PeriodNotClosing(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrPeriodNotClosing{PeriodID: 3}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.FailedPrecondition, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonPeriodNotClosing, info.Reason)
	require.Equal(t, "3", info.Metadata["periodId"])
}

func TestBusinessErrorToGRPCStatus_InvalidReceipt(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrInvalidReceipt{Reason: "bad signature"}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.InvalidArgument, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonInvalidReceipt, info.Reason)
	require.Equal(t, "bad signature", info.Metadata["reason"])
}

func TestBusinessErrorToGRPCStatus_MaintenanceMode(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: domain.ErrMaintenanceMode}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.Unavailable, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonMaintenanceMode, info.Reason)
}

func TestBusinessErrorToGRPCStatus_InvalidCronExpression(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrInvalidCronExpression{
		Expression: "* * * *",
		Details:    "expected 5 fields",
	}}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.InvalidArgument, st.Code())

	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonInvalidCronExpression, info.Reason)
	require.Equal(t, "* * * *", info.Metadata["expression"])
	require.Equal(t, "expected 5 fields", info.Metadata["details"])
}

func TestBusinessErrorToGRPCStatus_UnknownError(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: errors.New("something unexpected")}
	st := businessErrorToGRPCStatus(bizErr)

	require.Equal(t, codes.Internal, st.Code())
	require.Contains(t, st.Message(), "something unexpected")
	// Default case has no ErrorInfo details
	require.Empty(t, st.Details())
}

func TestConvertToGRPCError_BusinessError(t *testing.T) {
	t.Parallel()

	bizErr := &domain.BusinessError{Err: &domain.ErrLedgerNotFound{Name: "test"}}
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

func TestConvertToGRPCError_MissingSignature(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(signing.ErrMissingSignature)
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.Unauthenticated, st.Code())
}

func TestConvertToGRPCError_InvalidSignature(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(signing.ErrInvalidSignature)
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.PermissionDenied, st.Code())
}

func TestConvertToGRPCError_UnknownKeyID(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(signing.ErrUnknownKeyID)
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.PermissionDenied, st.Code())
}

func TestConvertToGRPCError_MaintenanceMode(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(admission.ErrMaintenanceMode)
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code())
}

func TestConvertToGRPCError_NoLeader(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(commonpb.ErrNoLeader)
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code())
}

func TestConvertToGRPCError_NotFoundError(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(commonpb.NewNotFoundError("ledger %s not found", "test"))
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
}

func TestConvertToGRPCError_AuditDisabled(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(domain.ErrAuditDisabled)
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestConvertToGRPCError_PeriodNotClosed(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(&domain.ErrPeriodNotClosed{PeriodID: 5})
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestConvertToGRPCError_PeriodNotArchiving(t *testing.T) {
	t.Parallel()

	grpcErr := convertToGRPCError(&domain.ErrPeriodNotArchiving{PeriodID: 3})
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestConvertToGRPCError_AlreadyGRPCStatus(t *testing.T) {
	t.Parallel()

	original := status.Error(codes.Internal, "already grpc")
	grpcErr := convertToGRPCError(original)
	require.Equal(t, original, grpcErr)
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
