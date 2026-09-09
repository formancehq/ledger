package cmdutil

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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

// TestFormatGRPCError_BusinessError_UnknownReasonStillFormatted pins the
// forward-compatibility half on the CLI surface. A reason from a newer server
// is absent from this build's ErrorReason enum, but the wire contract —
// message and metadata — is still what the operator needs to see, so it must
// be formatted as a business error rather than falling through to the generic
// status message.
func TestFormatGRPCError_BusinessError_UnknownReasonStillFormatted(t *testing.T) {
	t.Parallel()

	require.Equal(t, commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED,
		domain.ReasonCode("SOME_REASON_FROM_A_NEWER_SERVER"),
		"precondition: the reason must be unknown to this build")

	grpcErr := buildGRPCError(t, codes.AlreadyExists, "something conflicted",
		"SOME_REASON_FROM_A_NEWER_SERVER", map[string]string{"name": "foo"})

	err := FormatGRPCError("create ledger", grpcErr)

	var cliErr *CLIError
	require.ErrorAs(t, err, &cliErr)
	require.Equal(t, "create ledger: something conflicted", err.Error(),
		"the business-error path formats context + the server's message, with no status prefix")
}

// TestFormatGRPCError_InvalidWirePairIsNotABusinessError covers the mismatch
// policy on the CLI surface. LEDGER_DELETED is KindConflict, which this build
// only sends as codes.FailedPrecondition; arriving as codes.Unavailable is a
// protocol fault, so the payload is not trusted as a business outcome.
//
// codes.Unavailable is chosen deliberately: friendlyMessage gives it a
// distinguishing "server unavailable: " prefix, so a fall-through to the
// status-code path is observable. That fall-through is the defect this pins —
// it formatted the peer's own status message, echoing untrusted free-form text
// as though this build had produced it. The invalid-pair rendering names the
// reason and the codes, which are this build's enum values, and nothing else.
func TestFormatGRPCError_InvalidWirePairIsNotABusinessError(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.Unavailable, "ledger deleted: secret-ledger",
		domain.ErrReasonLedgerDeleted, map[string]string{"name": "secret-ledger"})

	err := FormatGRPCError("delete ledger", grpcErr)

	var cliErr *CLIError
	require.ErrorAs(t, err, &cliErr)

	require.NotContains(t, err.Error(), "secret-ledger",
		"neither the peer's message nor its metadata may be echoed to the operator")
	require.NotContains(t, err.Error(), "server unavailable",
		"an invalid pair must not fall through to the status-code path, which formats the peer's message")
	require.Contains(t, err.Error(), "delete ledger: invalid wire error",
		"the operator is told the response was not a valid business outcome")
	require.Contains(t, err.Error(), domain.ErrReasonLedgerDeleted,
		"the reason and codes are this build's own enum values and stay for diagnosis")
}
