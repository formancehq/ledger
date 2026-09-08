package cmdutil

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
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

	grpcErr := status.Error(codes.PermissionDenied, "missing required scope (required: [ledger:LedgerRead])")
	err := FormatGRPCError("list ledgers", grpcErr)
	require.Contains(t, err.Error(), "missing required scope")
}

func TestDisplayed_NilReturnsNil(t *testing.T) {
	t.Parallel()
	require.NoError(t, Displayed(nil))
}

func TestDisplayed_WrapsError(t *testing.T) {
	t.Parallel()

	inner := errors.New("something failed")
	err := Displayed(inner)
	require.Error(t, err)
	require.Equal(t, "something failed", err.Error())

	var cliErr *CLIError
	require.ErrorAs(t, err, &cliErr)
	require.Equal(t, inner, cliErr.Unwrap())
}

func TestFormatGRPCError_ReturnsDisplayedError(t *testing.T) {
	t.Parallel()

	grpcErr := status.Error(codes.Unavailable, "connection refused")
	err := FormatGRPCError("create ledger", grpcErr)

	var cliErr *CLIError
	require.ErrorAs(t, err, &cliErr, "FormatGRPCError should return a Displayed error")
	require.Contains(t, err.Error(), "connection refused")
}

func TestFormatGRPCError_BusinessError_ReturnsDisplayed(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.FailedPrecondition, "index not found: address",
		domain.ErrReasonIndexNotFound, map[string]string{"index": "address"})

	err := FormatGRPCError("list accounts", grpcErr)

	var cliErr *CLIError
	require.ErrorAs(t, err, &cliErr, "FormatGRPCError should return a Displayed error for business errors")
	require.Contains(t, err.Error(), "index not found")
}
