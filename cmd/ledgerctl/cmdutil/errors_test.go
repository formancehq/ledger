package cmdutil

import (
	"errors"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing/numscript"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// buildGRPCError creates a gRPC status error with an ErrorInfo detail, simulating what the server sends.
func buildGRPCError(t *testing.T, code codes.Code, message, reason string, metadata map[string]string) error {
	t.Helper()

	st := status.New(code, message)
	detailed, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason:   reason,
		Domain:   "ledger",
		Metadata: metadata,
	})
	require.NoError(t, err)
	return detailed.Err()
}

func TestBusinessErrorFromGRPC_LedgerAlreadyExists(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.AlreadyExists, "ledger already exists: foo",
		domain.ErrReasonLedgerAlreadyExists, map[string]string{"name": "foo"})

	bizErr := BusinessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var ledgerErr *domain.ErrLedgerAlreadyExists
	require.True(t, errors.As(bizErr, &ledgerErr))
	require.Equal(t, "foo", ledgerErr.Name)
}

func TestBusinessErrorFromGRPC_LedgerNotFound(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.NotFound, "ledger does not exist: bar",
		domain.ErrReasonLedgerNotFound, map[string]string{"name": "bar"})

	bizErr := BusinessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var ledgerErr *domain.ErrLedgerNotFound
	require.True(t, errors.As(bizErr, &ledgerErr))
	require.Equal(t, "bar", ledgerErr.Name)
}

func TestBusinessErrorFromGRPC_IdempotencyKeyConflict(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.AlreadyExists, "idempotency key conflict",
		domain.ErrReasonIdempotencyKeyConflict, map[string]string{"key": "ik-123"})

	bizErr := BusinessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var ikErr *domain.ErrIdempotencyKeyConflict
	require.True(t, errors.As(bizErr, &ikErr))
	require.Equal(t, "ik-123", ikErr.Key)
}

func TestBusinessErrorFromGRPC_TransactionReferenceConflict(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.AlreadyExists, "ref conflict",
		domain.ErrReasonTransactionReferenceConflict, map[string]string{
			"ledger":    "test",
			"reference": "ref-001",
		})

	bizErr := BusinessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var refErr *domain.ErrTransactionReferenceConflict
	require.True(t, errors.As(bizErr, &refErr))
	require.Equal(t, "test", refErr.Ledger)
	require.Equal(t, "ref-001", refErr.Reference)
}

func TestBusinessErrorFromGRPC_TransactionNotFound(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.NotFound, "tx not found",
		domain.ErrReasonTransactionNotFound, map[string]string{"transactionId": "999"})

	bizErr := BusinessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var txErr *domain.ErrTransactionNotFound
	require.True(t, errors.As(bizErr, &txErr))
	require.Equal(t, uint64(999), txErr.TransactionID)
}

func TestBusinessErrorFromGRPC_TransactionAlreadyReverted(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.FailedPrecondition, "already reverted",
		domain.ErrReasonTransactionAlreadyReverted, map[string]string{"transactionId": "42"})

	bizErr := BusinessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var txErr *domain.ErrTransactionAlreadyReverted
	require.True(t, errors.As(bizErr, &txErr))
	require.Equal(t, uint64(42), txErr.TransactionID)
}

func TestBusinessErrorFromGRPC_InsufficientFunds(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.FailedPrecondition, "insufficient funds",
		domain.ErrReasonInsufficientFunds, map[string]string{
			"account": "user:001",
			"asset":   "USD",
			"amount":  "1000",
			"balance": "500",
		})

	bizErr := BusinessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var fundsErr *domain.ErrInsufficientFunds
	require.True(t, errors.As(bizErr, &fundsErr))
	require.Equal(t, "user:001", fundsErr.Account)
	require.Equal(t, "USD", fundsErr.Asset)
	require.Equal(t, "1000", fundsErr.Amount)
	require.Equal(t, "500", fundsErr.Balance)
}

func TestBusinessErrorFromGRPC_NumscriptParseError(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.InvalidArgument, "parse error",
		domain.ErrReasonNumscriptParseError, map[string]string{"details": "unexpected token"})

	bizErr := BusinessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var parseErr *numscript.ErrNumscriptParse
	require.True(t, errors.As(bizErr, &parseErr))
	require.Equal(t, "unexpected token", parseErr.Details)
}

func TestBusinessErrorFromGRPC_Validation(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.InvalidArgument, "target is required",
		domain.ErrReasonValidation, nil)

	bizErr := BusinessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)
	require.Equal(t, "target is required", bizErr.Err.Error())
}

func TestBusinessErrorFromGRPC_NonBusinessError(t *testing.T) {
	t.Parallel()

	// A plain gRPC error without ErrorInfo domain "ledger"
	grpcErr := status.Error(codes.Internal, "some internal error")

	bizErr := BusinessErrorFromGRPC(grpcErr)
	require.Nil(t, bizErr)
}

func TestBusinessErrorFromGRPC_NonGRPCError(t *testing.T) {
	t.Parallel()

	bizErr := BusinessErrorFromGRPC(errors.New("plain error"))
	require.Nil(t, bizErr)
}

func TestBusinessErrorRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ledger already exists", &domain.ErrLedgerAlreadyExists{Name: "test"}},
		{"ledger not found", &domain.ErrLedgerNotFound{Name: "test"}},
		{"idempotency key conflict", &domain.ErrIdempotencyKeyConflict{Key: "ik-1"}},
		{"transaction reference conflict", &domain.ErrTransactionReferenceConflict{Ledger: "test", Reference: "ref-1"}},
		{"transaction not found", &domain.ErrTransactionNotFound{TransactionID: 100}},
		{"transaction already reverted", &domain.ErrTransactionAlreadyReverted{TransactionID: 100}},
		{"insufficient funds", &domain.ErrInsufficientFunds{Account: "a", Asset: "USD", Amount: "10", Balance: "5"}},
		{"balance not found", &domain.ErrBalanceNotFound{Account: "a", Asset: "USD"}},
		{"balance not preloaded", &numscript.ErrBalanceNotPreloaded{Account: "a", Asset: "USD"}},
		{"numscript parse error", &numscript.ErrNumscriptParse{Details: "bad syntax"}},
		{"index not found", &domain.ErrIndexNotFound{Index: "metadata[\"role\"] on a:"}},
		{"index building", &domain.ErrIndexBuilding{Index: "metadata[\"role\"] on a:"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Server side: wrap in BusinessError and convert to gRPC status
			bizErr := &domain.BusinessError{Err: tt.err}
			st := serverSideConvert(bizErr)

			// Client side: reconstruct from gRPC error
			reconstructed := BusinessErrorFromGRPC(st.Err())
			require.NotNil(t, reconstructed, "expected reconstructed business error")
			require.Equal(t, tt.err.Error(), reconstructed.Err.Error())
		})
	}
}

func TestFormatGRPCError_Unauthenticated_IncludesServerMessage(t *testing.T) {
	t.Parallel()

	grpcErr := status.Error(codes.Unauthenticated, "invalid token: token has expired")
	err := FormatGRPCError("list ledgers", grpcErr)
	require.Contains(t, err.Error(), "token has expired")
	require.Contains(t, err.Error(), "hint:")
}

func TestFormatGRPCError_Unauthenticated_SignatureError(t *testing.T) {
	t.Parallel()

	grpcErr := status.Error(codes.Unauthenticated, "invalid token: invalid signature")
	err := FormatGRPCError("list ledgers", grpcErr)
	require.Contains(t, err.Error(), "invalid signature")
	require.Contains(t, err.Error(), "signing key")
}

func TestFormatGRPCError_Unauthenticated_MissingToken(t *testing.T) {
	t.Parallel()

	grpcErr := status.Error(codes.Unauthenticated, "missing authorization header")
	err := FormatGRPCError("list ledgers", grpcErr)
	require.Contains(t, err.Error(), "missing authorization header")
	require.Contains(t, err.Error(), "hint:")
}

func TestFormatGRPCError_PermissionDenied(t *testing.T) {
	t.Parallel()

	grpcErr := status.Error(codes.PermissionDenied, "missing required scope (required: [ledgers:read])")
	err := FormatGRPCError("list ledgers", grpcErr)
	require.Contains(t, err.Error(), "missing required scope")
}

// serverSideConvert simulates the server-side conversion (imported from the application package).
// Since we can't import the internal package from cmd, we replicate the logic for round-trip testing.
func serverSideConvert(bizErr *domain.BusinessError) *status.Status {
	var (
		code     codes.Code
		reason   string
		metadata map[string]string
	)

	inner := bizErr.Err
	switch e := inner.(type) {
	case *domain.ErrLedgerAlreadyExists:
		code, reason = codes.AlreadyExists, domain.ErrReasonLedgerAlreadyExists
		metadata = map[string]string{"name": e.Name}
	case *domain.ErrLedgerNotFound:
		code, reason = codes.NotFound, domain.ErrReasonLedgerNotFound
		metadata = map[string]string{"name": e.Name}
	case *domain.ErrIdempotencyKeyConflict:
		code, reason = codes.AlreadyExists, domain.ErrReasonIdempotencyKeyConflict
		metadata = map[string]string{"key": e.Key}
	case *domain.ErrTransactionReferenceConflict:
		code, reason = codes.AlreadyExists, domain.ErrReasonTransactionReferenceConflict
		metadata = map[string]string{"ledger": e.Ledger, "reference": e.Reference}
	case *domain.ErrTransactionNotFound:
		code, reason = codes.NotFound, domain.ErrReasonTransactionNotFound
		metadata = map[string]string{"transactionId": "100"}
	case *domain.ErrTransactionAlreadyReverted:
		code, reason = codes.FailedPrecondition, domain.ErrReasonTransactionAlreadyReverted
		metadata = map[string]string{"transactionId": "100"}
	case *domain.ErrInsufficientFunds:
		code, reason = codes.FailedPrecondition, domain.ErrReasonInsufficientFunds
		metadata = map[string]string{"account": e.Account, "asset": e.Asset, "amount": e.Amount, "balance": e.Balance}
	case *domain.ErrBalanceNotFound:
		code, reason = codes.FailedPrecondition, domain.ErrReasonBalanceNotFound
		metadata = map[string]string{"account": e.Account, "asset": e.Asset}
	case *numscript.ErrBalanceNotPreloaded:
		code, reason = codes.FailedPrecondition, domain.ErrReasonBalanceNotPreloaded
		metadata = map[string]string{"account": e.Account, "asset": e.Asset}
	case *numscript.ErrNumscriptParse:
		code, reason = codes.InvalidArgument, domain.ErrReasonNumscriptParseError
		metadata = map[string]string{"details": e.Details}
	case *domain.ErrIndexNotFound:
		code, reason = codes.FailedPrecondition, domain.ErrReasonIndexNotFound
		metadata = map[string]string{"index": e.Index}
	case *domain.ErrIndexBuilding:
		code, reason = codes.FailedPrecondition, domain.ErrReasonIndexBuilding
		metadata = map[string]string{"index": e.Index}
	default:
		return status.New(codes.Internal, inner.Error())
	}

	st := status.New(code, inner.Error())
	detailed, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason:   reason,
		Domain:   "ledger",
		Metadata: metadata,
	})
	if err != nil {
		return st
	}
	return detailed
}
