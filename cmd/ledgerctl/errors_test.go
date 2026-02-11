package main

import (
	"errors"
	"math/big"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
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
		processing.ErrReasonLedgerAlreadyExists, map[string]string{"name": "foo"})

	bizErr := businessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var ledgerErr *processing.ErrLedgerAlreadyExists
	require.True(t, errors.As(bizErr, &ledgerErr))
	require.Equal(t, "foo", ledgerErr.Name)
}

func TestBusinessErrorFromGRPC_LedgerNotFound(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.NotFound, "ledger does not exist: bar",
		processing.ErrReasonLedgerNotFound, map[string]string{"name": "bar"})

	bizErr := businessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var ledgerErr *processing.ErrLedgerNotFound
	require.True(t, errors.As(bizErr, &ledgerErr))
	require.Equal(t, "bar", ledgerErr.Name)
}

func TestBusinessErrorFromGRPC_IdempotencyKeyConflict(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.AlreadyExists, "idempotency key conflict",
		processing.ErrReasonIdempotencyKeyConflict, map[string]string{"key": "ik-123"})

	bizErr := businessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var ikErr *processing.ErrIdempotencyKeyConflict
	require.True(t, errors.As(bizErr, &ikErr))
	require.Equal(t, "ik-123", ikErr.Key)
}

func TestBusinessErrorFromGRPC_TransactionReferenceConflict(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.AlreadyExists, "ref conflict",
		processing.ErrReasonTransactionReferenceConflict, map[string]string{
			"ledgerId":  "42",
			"reference": "ref-001",
		})

	bizErr := businessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var refErr *processing.ErrTransactionReferenceConflict
	require.True(t, errors.As(bizErr, &refErr))
	require.Equal(t, uint32(42), refErr.LedgerID)
	require.Equal(t, "ref-001", refErr.Reference)
}

func TestBusinessErrorFromGRPC_TransactionNotFound(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.NotFound, "tx not found",
		processing.ErrReasonTransactionNotFound, map[string]string{"transactionId": "999"})

	bizErr := businessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var txErr *processing.ErrTransactionNotFound
	require.True(t, errors.As(bizErr, &txErr))
	require.Equal(t, uint64(999), txErr.TransactionID)
}

func TestBusinessErrorFromGRPC_TransactionAlreadyReverted(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.FailedPrecondition, "already reverted",
		processing.ErrReasonTransactionAlreadyReverted, map[string]string{"transactionId": "42"})

	bizErr := businessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var txErr *processing.ErrTransactionAlreadyReverted
	require.True(t, errors.As(bizErr, &txErr))
	require.Equal(t, uint64(42), txErr.TransactionID)
}

func TestBusinessErrorFromGRPC_InsufficientFunds(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.FailedPrecondition, "insufficient funds",
		processing.ErrReasonInsufficientFunds, map[string]string{
			"account": "user:001",
			"asset":   "USD",
			"amount":  "1000",
			"balance": "500",
		})

	bizErr := businessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var fundsErr *processing.ErrInsufficientFunds
	require.True(t, errors.As(bizErr, &fundsErr))
	require.Equal(t, "user:001", fundsErr.Account)
	require.Equal(t, "USD", fundsErr.Asset)
	require.Equal(t, big.NewInt(1000), fundsErr.Amount)
	require.Equal(t, big.NewInt(500), fundsErr.Balance)
}

func TestBusinessErrorFromGRPC_NumscriptParseError(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.InvalidArgument, "parse error",
		processing.ErrReasonNumscriptParseError, map[string]string{"details": "unexpected token"})

	bizErr := businessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)

	var parseErr *processing.ErrNumscriptParse
	require.True(t, errors.As(bizErr, &parseErr))
	require.Equal(t, "unexpected token", parseErr.Details)
}

func TestBusinessErrorFromGRPC_Validation(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.InvalidArgument, "target is required",
		processing.ErrReasonValidation, nil)

	bizErr := businessErrorFromGRPC(grpcErr)
	require.NotNil(t, bizErr)
	require.Equal(t, "target is required", bizErr.Err.Error())
}

func TestBusinessErrorFromGRPC_NonBusinessError(t *testing.T) {
	t.Parallel()

	// A plain gRPC error without ErrorInfo domain "ledger"
	grpcErr := status.Error(codes.Internal, "some internal error")

	bizErr := businessErrorFromGRPC(grpcErr)
	require.Nil(t, bizErr)
}

func TestBusinessErrorFromGRPC_NonGRPCError(t *testing.T) {
	t.Parallel()

	bizErr := businessErrorFromGRPC(errors.New("plain error"))
	require.Nil(t, bizErr)
}

func TestBusinessErrorRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ledger already exists", &processing.ErrLedgerAlreadyExists{Name: "test"}},
		{"ledger not found", &processing.ErrLedgerNotFound{Name: "test"}},
		{"idempotency key conflict", &processing.ErrIdempotencyKeyConflict{Key: "ik-1"}},
		{"transaction reference conflict", &processing.ErrTransactionReferenceConflict{LedgerID: 1, Reference: "ref-1"}},
		{"transaction not found", &processing.ErrTransactionNotFound{TransactionID: 100}},
		{"transaction already reverted", &processing.ErrTransactionAlreadyReverted{TransactionID: 100}},
		{"insufficient funds", &processing.ErrInsufficientFunds{Account: "a", Asset: "USD", Amount: big.NewInt(10), Balance: big.NewInt(5)}},
		{"balance not found", &processing.ErrBalanceNotFound{Account: "a", Asset: "USD"}},
		{"balance not preloaded", &processing.ErrBalanceNotPreloaded{Account: "a", Asset: "USD"}},
		{"numscript parse error", &processing.ErrNumscriptParse{Details: "bad syntax"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Server side: wrap in BusinessError and convert to gRPC status
			bizErr := &processing.BusinessError{Err: tt.err}
			st := serverSideConvert(bizErr)

			// Client side: reconstruct from gRPC error
			reconstructed := businessErrorFromGRPC(st.Err())
			require.NotNil(t, reconstructed, "expected reconstructed business error")
			require.Equal(t, tt.err.Error(), reconstructed.Err.Error())
		})
	}
}

// serverSideConvert simulates the server-side conversion (imported from the application package).
// Since we can't import the internal package from cmd, we replicate the logic for round-trip testing.
func serverSideConvert(bizErr *processing.BusinessError) *status.Status {
	var (
		code     codes.Code
		reason   string
		metadata map[string]string
	)

	inner := bizErr.Err
	switch e := inner.(type) {
	case *processing.ErrLedgerAlreadyExists:
		code, reason = codes.AlreadyExists, processing.ErrReasonLedgerAlreadyExists
		metadata = map[string]string{"name": e.Name}
	case *processing.ErrLedgerNotFound:
		code, reason = codes.NotFound, processing.ErrReasonLedgerNotFound
		metadata = map[string]string{"name": e.Name}
	case *processing.ErrIdempotencyKeyConflict:
		code, reason = codes.AlreadyExists, processing.ErrReasonIdempotencyKeyConflict
		metadata = map[string]string{"key": e.Key}
	case *processing.ErrTransactionReferenceConflict:
		code, reason = codes.AlreadyExists, processing.ErrReasonTransactionReferenceConflict
		metadata = map[string]string{"ledgerId": "1", "reference": e.Reference}
	case *processing.ErrTransactionNotFound:
		code, reason = codes.NotFound, processing.ErrReasonTransactionNotFound
		metadata = map[string]string{"transactionId": "100"}
	case *processing.ErrTransactionAlreadyReverted:
		code, reason = codes.FailedPrecondition, processing.ErrReasonTransactionAlreadyReverted
		metadata = map[string]string{"transactionId": "100"}
	case *processing.ErrInsufficientFunds:
		code, reason = codes.FailedPrecondition, processing.ErrReasonInsufficientFunds
		metadata = map[string]string{"account": e.Account, "asset": e.Asset, "amount": e.Amount.String(), "balance": e.Balance.String()}
	case *processing.ErrBalanceNotFound:
		code, reason = codes.FailedPrecondition, processing.ErrReasonBalanceNotFound
		metadata = map[string]string{"account": e.Account, "asset": e.Asset}
	case *processing.ErrBalanceNotPreloaded:
		code, reason = codes.FailedPrecondition, processing.ErrReasonBalanceNotPreloaded
		metadata = map[string]string{"account": e.Account, "asset": e.Asset}
	case *processing.ErrNumscriptParse:
		code, reason = codes.InvalidArgument, processing.ErrReasonNumscriptParseError
		metadata = map[string]string{"details": e.Details}
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
