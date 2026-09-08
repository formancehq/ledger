package grpcerr

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// buildGRPCError creates a gRPC status error with an ErrorInfo detail,
// simulating what the server sends.
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

// assertRemote asserts that bizErr.Err is a *domain.RemoteError with the
// expected Reason and metadata-subset. Replaces the per-type ErrorAs checks
// from before the Describable refactor: client-side code no longer ties to
// the specific server Go types.
func assertRemote(t *testing.T, bizErr *domain.BusinessError, reason string, meta map[string]string) {
	t.Helper()

	require.NotNil(t, bizErr)

	var remote *domain.RemoteError
	require.ErrorAs(t, bizErr, &remote)
	require.Equal(t, reason, remote.Reason())

	for k, v := range meta {
		require.Equal(t, v, remote.Metadata()[k], "metadata key %q", k)
	}
}

func TestBusinessErrorFromGRPC_LedgerAlreadyExists(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.AlreadyExists, "ledger already exists: foo",
		domain.ErrReasonLedgerAlreadyExists, map[string]string{"name": "foo"})

	assertRemote(t, BusinessErrorFromGRPC(grpcErr), domain.ErrReasonLedgerAlreadyExists,
		map[string]string{"name": "foo"})
}

func TestBusinessErrorFromGRPC_LedgerNotFound(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.NotFound, "ledger does not exist: bar",
		domain.ErrReasonLedgerNotFound, map[string]string{"name": "bar"})

	assertRemote(t, BusinessErrorFromGRPC(grpcErr), domain.ErrReasonLedgerNotFound,
		map[string]string{"name": "bar"})
}

func TestBusinessErrorFromGRPC_IdempotencyKeyConflict(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.AlreadyExists, "idempotency key conflict",
		domain.ErrReasonIdempotencyKeyConflict, map[string]string{"key": "ik-123"})

	assertRemote(t, BusinessErrorFromGRPC(grpcErr), domain.ErrReasonIdempotencyKeyConflict,
		map[string]string{"key": "ik-123"})
}

func TestBusinessErrorFromGRPC_TransactionReferenceConflict(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.AlreadyExists, "ref conflict",
		domain.ErrReasonTransactionReferenceConflict, map[string]string{
			"ledger":    "test",
			"reference": "ref-001",
		})

	assertRemote(t, BusinessErrorFromGRPC(grpcErr), domain.ErrReasonTransactionReferenceConflict,
		map[string]string{"ledger": "test", "reference": "ref-001"})
}

func TestBusinessErrorFromGRPC_TransactionNotFound(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.NotFound, "tx not found",
		domain.ErrReasonTransactionNotFound, map[string]string{"transactionId": "999"})

	assertRemote(t, BusinessErrorFromGRPC(grpcErr), domain.ErrReasonTransactionNotFound,
		map[string]string{"transactionId": "999"})
}

func TestBusinessErrorFromGRPC_TransactionAlreadyReverted(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.FailedPrecondition, "already reverted",
		domain.ErrReasonTransactionAlreadyReverted, map[string]string{"transactionId": "42"})

	assertRemote(t, BusinessErrorFromGRPC(grpcErr), domain.ErrReasonTransactionAlreadyReverted,
		map[string]string{"transactionId": "42"})
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

	assertRemote(t, BusinessErrorFromGRPC(grpcErr), domain.ErrReasonInsufficientFunds,
		map[string]string{"account": "user:001", "asset": "USD", "amount": "1000", "balance": "500"})
}

func TestBusinessErrorFromGRPC_NumscriptParseError(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.InvalidArgument, "parse error",
		domain.ErrReasonNumscriptParseError, map[string]string{"details": "unexpected token"})

	assertRemote(t, BusinessErrorFromGRPC(grpcErr), domain.ErrReasonNumscriptParseError,
		map[string]string{"details": "unexpected token"})
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

func TestKindForCode_ResourceExhausted(t *testing.T) {
	t.Parallel()

	// Round-trip symmetry with the server-side kindToGRPCCode: a disk-full /
	// clock-skew write rejection (KindResourceExhausted) is sent as
	// codes.ResourceExhausted and must reconstruct to the same Kind client-side.
	require.Equal(t, domain.KindResourceExhausted, kindForCode(codes.ResourceExhausted))
}

func TestBusinessErrorRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  domain.Describable
	}{
		{"ledger already exists", &domain.ErrLedgerAlreadyExists{Name: "test"}},
		{"ledger not found", &domain.ErrLedgerNotFound{Name: "test"}},
		{"idempotency key conflict", &domain.ErrIdempotencyKeyConflict{Key: "ik-1"}},
		{"transaction reference conflict", &domain.ErrTransactionReferenceConflict{Ledger: "test", Reference: "ref-1"}},
		{"transaction not found", &domain.ErrTransactionNotFound{TransactionID: 100}},
		{"transaction already reverted", &domain.ErrTransactionAlreadyReverted{TransactionID: 100}},
		{"insufficient funds", &domain.ErrInsufficientFunds{Account: "a", Asset: "USD", Amount: "10", Balance: "5"}},
		{"balance not preloaded", &domain.ErrBalanceNotPreloaded{Account: "a", Asset: "USD"}},
		{"numscript parse error", &domain.ErrNumscriptParse{Details: "bad syntax"}},
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

// serverSideConvert simulates the server-side conversion. The real converter,
// internal/adapter/grpc.describableToGRPCStatus, is unexported, so the mapping
// is replicated here. Replicating it is what makes the round trip meaningful:
// the codes below were chosen independently of kindForCode, so a test passing
// proves the two directions agree rather than restating one of them.
func serverSideConvert(bizErr *domain.BusinessError) *status.Status {
	var (
		code     codes.Code
		reason   string
		metadata map[string]string
	)

	inner := bizErr.Err
	{
		var (
			e   *domain.ErrLedgerAlreadyExists
			e1  *domain.ErrLedgerNotFound
			e2  *domain.ErrIdempotencyKeyConflict
			e3  *domain.ErrTransactionReferenceConflict
			e4  *domain.ErrTransactionNotFound
			e5  *domain.ErrTransactionAlreadyReverted
			e6  *domain.ErrInsufficientFunds
			e8  *domain.ErrBalanceNotPreloaded
			e9  *domain.ErrNumscriptParse
			e10 *domain.ErrIndexNotFound
			e11 *domain.ErrIndexBuilding
		)

		switch {
		case errors.As(inner, &e):
			code, reason = codes.AlreadyExists, domain.ErrReasonLedgerAlreadyExists
			metadata = map[string]string{"name": e.Name}
		case errors.As(inner, &e1):
			code, reason = codes.NotFound, domain.ErrReasonLedgerNotFound
			metadata = map[string]string{"name": e1.Name}
		case errors.As(inner, &e2):
			code, reason = codes.AlreadyExists, domain.ErrReasonIdempotencyKeyConflict
			metadata = map[string]string{"key": e2.Key}
		case errors.As(inner, &e3):
			code, reason = codes.AlreadyExists, domain.ErrReasonTransactionReferenceConflict
			metadata = map[string]string{"ledger": e3.Ledger, "reference": e3.Reference}
		case errors.As(inner, &e4):
			code, reason = codes.NotFound, domain.ErrReasonTransactionNotFound
			metadata = map[string]string{"transactionId": "100"}
		case errors.As(inner, &e5):
			code, reason = codes.FailedPrecondition, domain.ErrReasonTransactionAlreadyReverted
			metadata = map[string]string{"transactionId": "100"}
		case errors.As(inner, &e6):
			code, reason = codes.FailedPrecondition, domain.ErrReasonInsufficientFunds
			metadata = map[string]string{"account": e6.Account, "asset": e6.Asset, "amount": e6.Amount, "balance": e6.Balance}
		case errors.As(inner, &e8):
			code, reason = codes.Unavailable, domain.ErrReasonBalanceNotPreloaded
			metadata = map[string]string{"account": e8.Account, "asset": e8.Asset}
		case errors.As(inner, &e9):
			code, reason = codes.InvalidArgument, domain.ErrReasonNumscriptParseError
			metadata = map[string]string{"details": e9.Details}
		case errors.As(inner, &e10):
			code, reason = codes.FailedPrecondition, domain.ErrReasonIndexNotFound
			metadata = map[string]string{"index": e10.Index}
		case errors.As(inner, &e11):
			code, reason = codes.Unavailable, domain.ErrReasonIndexBuilding
			metadata = map[string]string{"index": e11.Index}
		default:
			return status.New(codes.Internal, inner.Error())
		}
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

// TestKindForWire_ReasonFirst covers one reason per ErrorKind. The
// KindConflict row is the one that matters most: kindToGRPCCode sends both
// KindConflict and KindPrecondition as codes.FailedPrecondition, so deriving
// the kind from the code alone answers KindPrecondition (400) where the leader
// answered 409. Reason-first recovers the distinction.
//
// A KindAlreadyExists reason would NOT catch that regression —
// codes.AlreadyExists is collision-free and round-trips either way — so
// ledger-already-exists, idempotency-key conflict and reference conflict are
// deliberately not used as the 409 case here.
func TestKindForWire_ReasonFirst(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		reason   string
		code     codes.Code
		wantKind domain.ErrorKind
	}{
		{"validation", domain.ErrReasonValidation, codes.InvalidArgument, domain.KindValidation},
		{"not found", domain.ErrReasonLedgerNotFound, codes.NotFound, domain.KindNotFound},
		{"already exists", domain.ErrReasonLedgerAlreadyExists, codes.AlreadyExists, domain.KindAlreadyExists},
		{
			"conflict survives the FailedPrecondition collapse",
			domain.ErrReasonLedgerDeleted, codes.FailedPrecondition, domain.KindConflict,
		},
		{"precondition", domain.ErrReasonInsufficientFunds, codes.FailedPrecondition, domain.KindPrecondition},
		{"unavailable", domain.ErrReasonIndexBuilding, codes.Unavailable, domain.KindUnavailable},
		{"resource exhausted", domain.ErrReasonWritesBlockedDiskFull, codes.ResourceExhausted, domain.KindResourceExhausted},
		{"internal", domain.ErrReasonCoverageMiss, codes.Internal, domain.KindInternal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.wantKind, kindForWire(tt.reason, tt.code))
		})
	}
}

// TestKindForWire_UnknownReasonFallsBackToCode pins the forward-compatibility
// half of the derivation. A client older than the server receives a reason its
// enum does not know; ReasonCode yields UNSPECIFIED, and reason-only derivation
// would collapse the error to KindInternal and answer 500 for what the server
// classified as a caller error.
func TestKindForWire_UnknownReasonFallsBackToCode(t *testing.T) {
	t.Parallel()

	require.Equal(t, commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED,
		domain.ReasonCode("SOME_REASON_FROM_A_NEWER_SERVER"),
		"precondition: the reason must be unknown to this build for the test to mean anything")

	require.Equal(t, domain.KindNotFound, kindForWire("SOME_REASON_FROM_A_NEWER_SERVER", codes.NotFound))
	require.Equal(t, domain.KindAlreadyExists, kindForWire("SOME_REASON_FROM_A_NEWER_SERVER", codes.AlreadyExists))
}

// TestFromStatusError_BusinessErrorBecomesDescribable is the core of EN-1636:
// a business error forwarded from the leader must reach the consumer as a
// Describable carrying the leader's kind, while still answering the leader's
// status code.
func TestFromStatusError_BusinessErrorBecomesDescribable(t *testing.T) {
	t.Parallel()

	// The reproduction case from the ticket: a metadata field absent from the
	// schema. KindPrecondition, which the HTTP layer answers as 400.
	grpcErr := buildGRPCError(t, codes.FailedPrecondition,
		"metadata field not declared in schema: TARGET_TYPE_ACCOUNT/type",
		domain.ErrReasonMetadataFieldNotInSchema, map[string]string{"field": "type"})

	converted := FromStatusError(grpcErr)

	d, ok := errors.AsType[domain.Describable](converted)
	require.True(t, ok, "the reconstructed error must satisfy the contract handleError dispatches on")
	require.Equal(t, domain.ErrReasonMetadataFieldNotInSchema, d.Reason())
	require.Equal(t, domain.KindPrecondition, domain.Kind(d))
	require.Equal(t, "type", d.Metadata()["field"])

	// The message must be the leader's, with no transport wrapper in front of
	// it: it becomes the client-visible errorMessage.
	require.Equal(t, "metadata field not declared in schema: TARGET_TYPE_ACCOUNT/type", converted.Error())
}

// TestFromStatusError_PreservesStatusCode pins the contract
// internal/adapter/grpc/cursor.go depends on: it keys end-of-stream detection
// off status.Code(err), so reconstruction must not hide the code. It is also
// what lets convertToGRPCError re-derive the same status on a second hop.
func TestFromStatusError_PreservesStatusCode(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.FailedPrecondition, "ledger deleted: foo",
		domain.ErrReasonLedgerDeleted, nil)

	converted := FromStatusError(grpcErr)

	require.Equal(t, codes.FailedPrecondition, status.Code(converted))

	st, ok := status.FromError(converted)
	require.True(t, ok)
	require.Equal(t, "ledger deleted: foo", st.Message())
}

// TestFromStatusError_ConflictReachesKindConflict is the end-to-end form of the
// collapse regression: the wire carries FailedPrecondition, and the value the
// HTTP layer inspects must still report KindConflict (409), not
// KindPrecondition (400).
func TestFromStatusError_ConflictReachesKindConflict(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.FailedPrecondition, "ledger deleted: foo",
		domain.ErrReasonLedgerDeleted, nil)

	d, ok := errors.AsType[domain.Describable](FromStatusError(grpcErr))
	require.True(t, ok)
	require.Equal(t, domain.KindConflict, domain.Kind(d))
}

// TestFromStatusError_ReconstructedErrorIsNotItselfDescribable pins the shape
// that makes the kind override work.
//
// domain.Kind checks for kindOverride first, then for *BusinessError. If the
// carrier implemented Describable itself, errors.AsType would stop at the
// carrier, domain.Kind would match neither branch, and it would silently
// re-derive the kind from the reason — discarding the override that exists
// precisely so an unknown reason keeps the status the wire carried.
func TestFromStatusError_ReconstructedErrorIsNotItselfDescribable(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.NotFound, "ledger not found: foo",
		domain.ErrReasonLedgerNotFound, nil)

	converted := FromStatusError(grpcErr)

	// A direct type assertion is the point here, not an oversight: the
	// invariant is that the TOP-LEVEL carrier does not satisfy Describable.
	// errors.As would walk the chain and find the BusinessError inside, which
	// is what the next assertion checks and the opposite of what this one does.
	//nolint:errorlint // deliberate: asserts on the carrier, not the chain.
	_, isDescribable := converted.(domain.Describable)
	require.False(t, isDescribable, "the carrier must not satisfy Describable directly")

	d, ok := errors.AsType[domain.Describable](converted)
	require.True(t, ok)
	require.IsType(t, &domain.BusinessError{}, d, "errors.AsType must unwrap to the BusinessError")
}

// TestFromStatusError_BareNotFound covers the ~20 commonpb.NewNotFoundError
// sites, which send a bare codes.NotFound with no ErrorInfo. Without this the
// forwarded lookup miss degrades to a 500.
func TestFromStatusError_BareNotFound(t *testing.T) {
	t.Parallel()

	converted := FromStatusError(status.Error(codes.NotFound, "ledger foo not found"))

	notFound, ok := errors.AsType[*commonpb.NotFoundError](converted)
	require.True(t, ok, "handleError dispatches on *commonpb.NotFoundError for its 404 branch")
	require.Equal(t, "ledger foo not found", notFound.Error())
	require.Equal(t, codes.NotFound, status.Code(converted), "the code must survive")
}

// TestFromStatusError_PassthroughCodes covers the statuses left deliberately
// raw. codes.Canceled is the load-bearing one: cursor.go normalises it into
// io.EOF to end a page, so wrapping it would break pagination.
func TestFromStatusError_PassthroughCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code codes.Code
		why  string
	}{
		{"canceled", codes.Canceled, "cursor.go turns this into io.EOF to end a page"},
		{"unavailable", codes.Unavailable, "handleError already answers 503 + Retry-After for this code"},
		{"internal", codes.Internal, "a genuine server fault, nothing typed to restore"},
		{"unknown", codes.Unknown, "the sanitised fallthrough; must keep its correlation ID"},
		{"deadline exceeded", codes.DeadlineExceeded, "a transport condition, not a business outcome"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := status.Error(tt.code, "message")
			require.Equal(t, original, FromStatusError(original), tt.why)
		})
	}
}

// TestFromStatusError_NonStatusErrors: a local failure or a stream end is not
// a status and must come back untouched. io.EOF in particular is how every
// cursor terminates.
func TestFromStatusError_NonStatusErrors(t *testing.T) {
	t.Parallel()

	require.NoError(t, FromStatusError(nil))

	require.Equal(t, io.EOF, FromStatusError(io.EOF))
	require.ErrorIs(t, FromStatusError(io.EOF), io.EOF, "cursors terminate on io.EOF")

	plain := errors.New("dial tcp: connection refused")
	require.Equal(t, plain, FromStatusError(plain))
}

// TestFromStatusError_ForeignErrorDomain: an ErrorInfo stamped by another
// service is not ours to reinterpret.
func TestFromStatusError_ForeignErrorDomain(t *testing.T) {
	t.Parallel()

	st := status.New(codes.FailedPrecondition, "some other service said no")
	detailed, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: "SOMETHING",
		Domain: "not-ledger",
	})
	require.NoError(t, err)

	original := detailed.Err()
	require.Equal(t, original, FromStatusError(original))
}
