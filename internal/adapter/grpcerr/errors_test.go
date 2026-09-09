package grpcerr

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/adapter/apierr"
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

// assertRemote asserts that decoded is an *apierr.Remote with the expected
// Reason and metadata-subset. Replaces the per-type ErrorAs checks from before
// the Describable refactor: a receiver no longer ties to the specific server
// Go types.
func assertRemote(t *testing.T, decoded error, reason string, meta map[string]string) {
	t.Helper()

	remote := remoteFrom(t, decoded)
	require.Equal(t, reason, remote.Reason())

	for k, v := range meta {
		require.Equal(t, v, remote.Metadata()[k], "metadata key %q", k)
	}
}

// remoteFrom is assertRemote's extraction half, for the assertions that read
// the decoded kind rather than only the wire contract.
func remoteFrom(t *testing.T, decoded error) *apierr.Remote {
	t.Helper()

	remote, ok := errors.AsType[*apierr.Remote](decoded)
	require.True(t, ok, "expected an *apierr.Remote, got %T", decoded)

	return remote
}

func TestDecode_LedgerAlreadyExists(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.AlreadyExists, "ledger already exists: foo",
		domain.ErrReasonLedgerAlreadyExists, map[string]string{"name": "foo"})

	assertRemote(t, Decode(grpcErr), domain.ErrReasonLedgerAlreadyExists,
		map[string]string{"name": "foo"})
}

func TestDecode_LedgerNotFound(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.NotFound, "ledger does not exist: bar",
		domain.ErrReasonLedgerNotFound, map[string]string{"name": "bar"})

	assertRemote(t, Decode(grpcErr), domain.ErrReasonLedgerNotFound,
		map[string]string{"name": "bar"})
}

func TestDecode_IdempotencyKeyConflict(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.AlreadyExists, "idempotency key conflict",
		domain.ErrReasonIdempotencyKeyConflict, map[string]string{"key": "ik-123"})

	assertRemote(t, Decode(grpcErr), domain.ErrReasonIdempotencyKeyConflict,
		map[string]string{"key": "ik-123"})
}

func TestDecode_TransactionReferenceConflict(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.AlreadyExists, "ref conflict",
		domain.ErrReasonTransactionReferenceConflict, map[string]string{
			"ledger":    "test",
			"reference": "ref-001",
		})

	assertRemote(t, Decode(grpcErr), domain.ErrReasonTransactionReferenceConflict,
		map[string]string{"ledger": "test", "reference": "ref-001"})
}

func TestDecode_TransactionNotFound(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.NotFound, "tx not found",
		domain.ErrReasonTransactionNotFound, map[string]string{"transactionId": "999"})

	assertRemote(t, Decode(grpcErr), domain.ErrReasonTransactionNotFound,
		map[string]string{"transactionId": "999"})
}

func TestDecode_TransactionAlreadyReverted(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.FailedPrecondition, "already reverted",
		domain.ErrReasonTransactionAlreadyReverted, map[string]string{"transactionId": "42"})

	assertRemote(t, Decode(grpcErr), domain.ErrReasonTransactionAlreadyReverted,
		map[string]string{"transactionId": "42"})
}

func TestDecode_InsufficientFunds(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.FailedPrecondition, "insufficient funds",
		domain.ErrReasonInsufficientFunds, map[string]string{
			"account": "user:001",
			"asset":   "USD",
			"amount":  "1000",
			"balance": "500",
		})

	assertRemote(t, Decode(grpcErr), domain.ErrReasonInsufficientFunds,
		map[string]string{"account": "user:001", "asset": "USD", "amount": "1000", "balance": "500"})
}

func TestDecode_NumscriptParseError(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.InvalidArgument, "parse error",
		domain.ErrReasonNumscriptParseError, map[string]string{"details": "unexpected token"})

	assertRemote(t, Decode(grpcErr), domain.ErrReasonNumscriptParseError,
		map[string]string{"details": "unexpected token"})
}

func TestDecode_Validation(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.InvalidArgument, "target is required",
		domain.ErrReasonValidation, nil)

	assertRemote(t, Decode(grpcErr), domain.ErrReasonValidation, nil)
	require.Equal(t, "target is required", remoteFrom(t, Decode(grpcErr)).Error())
}

func TestDecode_NonBusinessError(t *testing.T) {
	t.Parallel()

	// A plain gRPC error without ErrorInfo domain "ledger"
	grpcErr := status.Error(codes.Internal, "some internal error")

	require.NoError(t, Decode(grpcErr), "no ledger ErrorInfo, nothing to decode")
}

func TestDecode_NonGRPCError(t *testing.T) {
	t.Parallel()

	require.NoError(t, Decode(errors.New("plain error")))
}

func TestKindForCode_ResourceExhausted(t *testing.T) {
	t.Parallel()

	// Round-trip symmetry with CodeForKind: a disk-full / clock-skew write
	// rejection (KindResourceExhausted) is sent as codes.ResourceExhausted and
	// must decode back to the same Kind.
	require.Equal(t, codes.ResourceExhausted, CodeForKind(domain.KindResourceExhausted))
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

			// Receiving side: decode the boundary view back out.
			remote, ok := errors.AsType[*apierr.Remote](Decode(st.Err()))
			require.True(t, ok, "expected a decoded remote failure")
			require.Equal(t, tt.err.Error(), remote.Error())
			require.Equal(t, tt.err.Reason(), remote.Reason())
			require.Equal(t, domain.Kind(tt.err), remote.KindValue,
				"the decoded kind must equal the kind the server classified")
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

// TestDecode_ReasonFirst covers one reason per ErrorKind through the real
// decoder. The KindConflict row is the one that matters most: CodeForKind
// sends both KindConflict and KindPrecondition as codes.FailedPrecondition, so
// deriving the kind from the code alone answers KindPrecondition (400) where
// the leader answered 409. Reason-first recovers the distinction.
//
// A KindAlreadyExists reason would NOT catch that regression —
// codes.AlreadyExists is collision-free and round-trips either way — so
// ledger-already-exists, idempotency-key conflict and reference conflict are
// deliberately not used as the 409 case here.
// TestDecode_EveryEnumReasonRoundTrips widens this to the whole enum.
func TestDecode_ReasonFirst(t *testing.T) {
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

			decoded := Decode(buildGRPCError(t, tt.code, "message", tt.reason, nil))
			assertRemote(t, decoded, tt.reason, nil)
			require.Equal(t, tt.wantKind, remoteFrom(t, decoded).KindValue)
		})
	}
}

// TestDecode_UnknownReasonFallsBackToCode pins the forward-compatibility half
// of the derivation. A receiver older than the sender gets a reason its enum
// does not know; ReasonCode yields UNSPECIFIED, and reason-only derivation
// would collapse the error to KindInternal and answer 500 for what the sender
// classified as a caller error.
//
// The AlreadyExists row is the EN-1980 acceptance case: an unknown reason
// whose code does have an ErrorKind mapping keeps the code-derived
// classification (409), rather than collapsing to KindInternal (500).
func TestDecode_UnknownReasonFallsBackToCode(t *testing.T) {
	t.Parallel()

	require.Equal(t, commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED,
		domain.ReasonCode(unknownReason),
		"precondition: the reason must be unknown to this build for the test to mean anything")

	tests := []struct {
		code     codes.Code
		wantKind domain.ErrorKind
	}{
		{codes.NotFound, domain.KindNotFound},
		{codes.AlreadyExists, domain.KindAlreadyExists},
		{codes.InvalidArgument, domain.KindValidation},
		{codes.FailedPrecondition, domain.KindPrecondition},
		{codes.ResourceExhausted, domain.KindResourceExhausted},
		{codes.Unavailable, domain.KindUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			t.Parallel()

			decoded := Decode(buildGRPCError(t, tt.code, "message", unknownReason,
				map[string]string{"k": "v"}))
			assertRemote(t, decoded, unknownReason, map[string]string{"k": "v"})

			remote := remoteFrom(t, decoded)
			require.Equal(t, tt.wantKind, remote.KindValue)
			require.Equal(t, "message", remote.Error(),
				"the sender's message is preserved verbatim for a reason we cannot validate")
		})
	}
}

// unknownReason stands for a reason from a newer server: absent from this
// build's ErrorReason enum, so it can be neither classified nor validated
// against its code.
const unknownReason = "SOME_REASON_FROM_A_NEWER_SERVER"

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

	d, ok := apierr.Describe(FromStatusError(grpcErr))
	require.True(t, ok)
	require.Equal(t, domain.KindConflict, d.Kind)
}

// TestFromStatusError_ReconstructedErrorIsNotItselfDescribable pins the shape
// that makes the decoded kind reachable.
//
// apierr.Describe looks for an *apierr.Remote in the chain and reads the kind
// it carries. If the carrier satisfied domain.Describable itself, a consumer
// walking the chain could stop at the carrier and re-derive the kind from the
// reason — which for a reason this build does not know collapses to
// KindInternal, discarding exactly what decoding recovered.
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
	require.IsType(t, &apierr.Remote{}, d, "errors.AsType must unwrap to the decoded remote failure")
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
//
// codes.NotFound is the row that matters. The bare-NotFound fallback exists
// for the ~20 commonpb.NewNotFoundError sites, which carry no detail at all;
// applied to a foreign typed failure it rewrote that service's contract into a
// ledger *commonpb.NotFoundError, which is exactly what the domain check
// upstream of it declined to do.
func TestFromStatusError_ForeignErrorDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code codes.Code
	}{
		{
			name: "failed precondition",
			code: codes.FailedPrecondition,
		},
		{
			name: "not found — must not take the bare-NotFound fallback",
			code: codes.NotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := status.New(tc.code, "some other service said no")
			detailed, err := st.WithDetails(&errdetails.ErrorInfo{
				Reason: "SOMETHING",
				Domain: "not-ledger",
			})
			require.NoError(t, err)

			original := detailed.Err()
			require.Equal(t, original, FromStatusError(original))

			_, isNotFound := errors.AsType[*commonpb.NotFoundError](FromStatusError(original))
			require.False(t, isNotFound,
				"a foreign service's typed failure must not become a ledger NotFoundError")
		})
	}
}

// TestFromStatusError_CanceledCarryingLedgerReasonIsValidated is the
// end-of-stream half of the mismatch policy.
//
// No ErrorKind maps to codes.Canceled, so every reason this build knows is a
// contradiction under it — and the passthrough that keeps cursor termination
// working used to answer the status before the detail was ever decoded. That
// exempted the one code with no legitimate reason from the validation the
// policy exists for, and handed the peer's message to the surfaces that render
// an unrecognised error.
func TestFromStatusError_CanceledCarryingLedgerReasonIsValidated(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.Canceled, "ledger deleted: secret-ledger",
		domain.ErrReasonLedgerDeleted, map[string]string{"name": "secret-ledger"})

	converted := FromStatusError(grpcErr)

	var invalid *apierr.InvalidWireError
	require.ErrorAs(t, converted, &invalid)
	require.Equal(t, codes.Canceled, invalid.Code)
	require.Equal(t, []codes.Code{codes.FailedPrecondition}, invalid.Expected)

	_, isDescribable := apierr.Describe(converted)
	require.False(t, isDescribable, "a protocol fault is not a business outcome")

	require.NotContains(t, converted.Error(), "secret-ledger",
		"the received message must not survive into any rendered text")
}

// TestFromStatusError_CanceledWithUnknownReasonKeepsItsCode: an unknown reason
// cannot be validated, so it decodes down the forward-compatibility path like
// any other code. Reconstruction is still safe for cursor.go, because the
// carrier answers GRPCStatus() with the received Canceled status.
func TestFromStatusError_CanceledWithUnknownReasonKeepsItsCode(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.Canceled, "canceled upstream", unknownReason, nil)

	converted := FromStatusError(grpcErr)

	require.Equal(t, codes.Canceled, status.Code(converted),
		"cursor.go reads the code to normalise a page end into io.EOF")

	d, ok := apierr.Describe(converted)
	require.True(t, ok)
	require.Equal(t, unknownReason, d.Reason, "the sender's reason is preserved verbatim")
}

// enumReasons returns every reason this build's ErrorReason enum knows,
// excluding UNSPECIFIED (which is the "unknown reason" signal, not a reason).
func enumReasons(t *testing.T) map[commonpb.ErrorReason]string {
	t.Helper()

	reasons := make(map[commonpb.ErrorReason]string, len(commonpb.ErrorReason_name))

	for value := range commonpb.ErrorReason_name {
		code := commonpb.ErrorReason(value)
		if code == commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED {
			continue
		}

		reasons[code] = domain.ReasonString(code)
	}

	require.NotEmpty(t, reasons, "the enum scan found nothing — the scan is broken, not the decoder")

	return reasons
}

// TestDecode_EveryEnumReasonRoundTrips is the exhaustive form of the
// conversion table: every reason in the enum, under every code this build's
// server may legitimately send it under, must decode back to the semantic kind
// the sender classified.
//
// One reason per ErrorKind is not enough coverage (EN-1980). Kinds are not
// one-to-one with wire codes — KindConflict and KindPrecondition share
// codes.FailedPrecondition — so a single sampled reason per kind can pass
// while a sibling reason of the same kind decodes wrong. Driving the table off
// the enum itself also means a reason added tomorrow is covered without
// touching this test, and cannot be added with a kind the decoder disagrees
// about.
func TestDecode_EveryEnumReasonRoundTrips(t *testing.T) {
	t.Parallel()

	for rc, reason := range enumReasons(t) {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()

			allowed := allowedWireCodes(rc)
			require.NotEmpty(t, allowed)

			for _, code := range allowed {
				decoded := Decode(buildGRPCError(t, code, "message", reason,
					map[string]string{"k": "v"}))
				assertRemote(t, decoded, reason, map[string]string{"k": "v"})

				require.Equal(t, domain.KindForReason(rc), remoteFrom(t, decoded).KindValue,
					"reason %s sent as %s must decode to its semantic kind", reason, code)
			}
		})
	}
}

// TestDecode_ConflictAndPreconditionShareOneWireCode is the structural reason
// the exhaustive test above must be exhaustive, pinned directly: two distinct
// semantic kinds travel under the same status code, so no amount of
// code-derived classification can separate them and every reason of both kinds
// has to be checked from the reason side.
func TestDecode_ConflictAndPreconditionShareOneWireCode(t *testing.T) {
	t.Parallel()

	require.Equal(t, codes.FailedPrecondition, CodeForKind(domain.KindConflict))
	require.Equal(t, codes.FailedPrecondition, CodeForKind(domain.KindPrecondition))

	counts := map[domain.ErrorKind]int{}

	for rc, reason := range enumReasons(t) {
		kind := domain.KindForReason(rc)
		if kind != domain.KindConflict && kind != domain.KindPrecondition {
			continue
		}

		counts[kind]++

		decoded := Decode(buildGRPCError(t, codes.FailedPrecondition, "message", reason, nil))
		assertRemote(t, decoded, reason, nil)
		require.Equal(t, kind, remoteFrom(t, decoded).KindValue,
			"reason %s collapsed onto the wrong side of the FailedPrecondition collapse", reason)
	}

	require.Greater(t, counts[domain.KindConflict], 1,
		"at least two KindConflict reasons must exist for this test to prove anything")
	require.Greater(t, counts[domain.KindPrecondition], 1,
		"at least two KindPrecondition reasons must exist for this test to prove anything")
}

// TestDecode_InvalidReasonCodePairIsRejected covers the mismatch policy. A
// reason this build knows is a reason whose legitimate codes it knows too, so
// a contradiction is a protocol fault rather than a business outcome: nothing
// received is answered to the client.
func TestDecode_InvalidReasonCodePairIsRejected(t *testing.T) {
	t.Parallel()

	// LEDGER_NOT_FOUND is KindNotFound, which this build only ever sends as
	// codes.NotFound. Arriving as AlreadyExists means the sender is not a
	// ledger of this contract.
	grpcErr := buildGRPCError(t, codes.AlreadyExists, "ledger not found: foo",
		domain.ErrReasonLedgerNotFound, map[string]string{"name": "foo"})

	invalid, ok := errors.AsType[*apierr.InvalidWireError](Decode(grpcErr))
	require.True(t, ok, "a contradicting pair must not decode to a business outcome")
	require.Equal(t, domain.ErrReasonLedgerNotFound, invalid.ReasonValue)
	require.Equal(t, codes.AlreadyExists, invalid.Code)
	require.Equal(t, []codes.Code{codes.NotFound}, invalid.Expected)

	require.NotContains(t, invalid.Error(), "ledger not found: foo",
		"the received message is untrusted and must not be carried forward")
	require.NotContains(t, invalid.Error(), "foo",
		"the received metadata is untrusted and must not be carried forward")
}

// TestFromStatusError_InvalidPairReachesTheSanitizer pins where a rejected
// pair ends up. It must satisfy neither the boundary contract nor GRPCStatus,
// so HTTP falls through to writeInternalServerError (500 + correlation ID +
// server-side log) and convertToGRPCError sanitises it to codes.Unknown —
// rather than being answered as the code it arrived under, which for
// codes.InvalidArgument would have echoed the untrusted message as a 400.
func TestFromStatusError_InvalidPairReachesTheSanitizer(t *testing.T) {
	t.Parallel()

	grpcErr := buildGRPCError(t, codes.InvalidArgument, "ledger deleted: foo",
		domain.ErrReasonLedgerDeleted, nil)

	converted := FromStatusError(grpcErr)

	_, isDescribable := apierr.Describe(converted)
	require.False(t, isDescribable, "a protocol fault is not a business outcome")

	_, hasStatus := status.FromError(converted)
	require.False(t, hasStatus,
		"keeping the status would let handleError answer InvalidArgument as a 400 with the untrusted message")

	require.ErrorAs(t, converted, new(*apierr.InvalidWireError))
}

// TestFromStatusError_UnknownReasonPreservesExactCode is the second EN-1980
// forward-compatibility case: a reason this build does not know, carried by a
// code no ErrorKind maps to. The classification degrades to KindInternal — the
// receiver genuinely cannot do better — but the exact upstream code must still
// cross a second hop, because a code reconstructed from KindInternal would be
// codes.Internal and would lose what the sender said.
func TestFromStatusError_UnknownReasonPreservesExactCode(t *testing.T) {
	t.Parallel()

	require.Equal(t, domain.KindInternal, kindForCode(codes.Aborted),
		"precondition: codes.Aborted has no ErrorKind of its own")

	grpcErr := buildGRPCError(t, codes.Aborted, "aborted upstream", unknownReason,
		map[string]string{"k": "v"})

	converted := FromStatusError(grpcErr)

	require.Equal(t, codes.Aborted, status.Code(converted),
		"the exact upstream code must survive to the next hop")

	d, ok := apierr.Describe(converted)
	require.True(t, ok)
	require.Equal(t, domain.KindInternal, d.Kind)
	require.Equal(t, unknownReason, d.Reason, "the sender's reason is preserved verbatim")
	require.Equal(t, "aborted upstream", d.Message)
	require.Equal(t, map[string]string{"k": "v"}, d.Metadata)
}
