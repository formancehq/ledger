package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/adapter/grpcerr"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// leaderStatus builds the error a leader sends for a business rejection:
// the kind's status code plus the reason and metadata in an ErrorInfo. This
// mirrors internal/adapter/grpc.describableToGRPCStatus.
func leaderStatus(t *testing.T, code codes.Code, message, reason string) error {
	t.Helper()

	st := status.New(code, message)
	detailed, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: "ledger",
	})
	require.NoError(t, err)

	return detailed.Err()
}

// TestHandleErrorForwardedFromLeader is the regression for EN-1636.
//
// A REST write that lands on a follower is forwarded to the leader over gRPC.
// The leader's typed business error arrives as a *status.Error, which is not a
// domain.Describable, so before grpcerr.NewConn the handler fell through to the
// 500 sanitizer and the client got INTERNAL_ERROR with a correlation ID for
// what is a plain 4xx. Each row feeds handleError exactly what the decorated
// connection now produces and asserts the client-visible outcome.
//
// The table covers one reason per ErrorKind that has an enum reason. The
// remaining two kinds, KindUnauthenticated and KindPermissionDenied, have no
// reason codes: HTTP auth is enforced by the middleware at the entry node
// before any forwarding, so an auth status arriving from the leader denotes a
// cluster-secret failure, which is a genuine server fault and correctly stays
// a 500.
func TestHandleErrorForwardedFromLeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		leaderErr      error
		expectedStatus int
		expectedCode   string
		checkRetry     bool
	}{
		{
			// The exact reproduction from the ticket: creating an index on a
			// metadata field never declared via SetMetadataFieldType. Observed
			// as 500 INTERNAL_ERROR on a follower, 400 on the leader.
			name: "metadata field not in schema — the ticket's repro",
			leaderErr: leaderStatus(t, codes.FailedPrecondition,
				"metadata field not declared in schema: TARGET_TYPE_ACCOUNT/type",
				domain.ErrReasonMetadataFieldNotInSchema),
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "METADATA_FIELD_NOT_IN_SCHEMA",
		},
		{
			// The collapse case. Both KindConflict and KindPrecondition are sent
			// as codes.FailedPrecondition, so deriving the kind from the code
			// alone answers 400 here where the leader answers 409.
			name: "ledger deleted — KindConflict must not collapse to 400",
			leaderErr: leaderStatus(t, codes.FailedPrecondition, "ledger deleted: foo",
				domain.ErrReasonLedgerDeleted),
			expectedStatus: http.StatusConflict,
			expectedCode:   "LEDGER_DELETED",
		},
		{
			name: "validation",
			leaderErr: leaderStatus(t, codes.InvalidArgument, "target is required",
				domain.ErrReasonValidation),
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "VALIDATION",
		},
		{
			name: "ledger not found",
			leaderErr: leaderStatus(t, codes.NotFound, "ledger not found: foo",
				domain.ErrReasonLedgerNotFound),
			expectedStatus: http.StatusNotFound,
			expectedCode:   "LEDGER_NOT_FOUND",
		},
		{
			name: "ledger already exists",
			leaderErr: leaderStatus(t, codes.AlreadyExists, "ledger already exists: foo",
				domain.ErrReasonLedgerAlreadyExists),
			expectedStatus: http.StatusConflict,
			expectedCode:   "LEDGER_ALREADY_EXISTS",
		},
		{
			name: "insufficient funds",
			leaderErr: leaderStatus(t, codes.FailedPrecondition, "insufficient funds",
				domain.ErrReasonInsufficientFunds),
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "INSUFFICIENT_FUNDS",
		},
		{
			name: "index building — retryable",
			leaderErr: leaderStatus(t, codes.Unavailable, `index building: metadata["role"] on a:`,
				domain.ErrReasonIndexBuilding),
			expectedStatus: http.StatusServiceUnavailable,
			expectedCode:   "INDEX_BUILDING",
			checkRetry:     true,
		},
		{
			name: "writes blocked, disk full",
			leaderErr: leaderStatus(t, codes.ResourceExhausted, "writes blocked: disk full",
				domain.ErrReasonWritesBlockedDiskFull),
			expectedStatus: http.StatusTooManyRequests,
			expectedCode:   "WRITES_BLOCKED_DISK_FULL",
		},
		{
			// A KindInternal reason is a real server fault and must keep its 500,
			// but it should still carry its own reason rather than the generic
			// sanitized code: the leader already decided this is safe to name.
			name: "coverage miss — a genuine 500, but a named one",
			leaderErr: leaderStatus(t, codes.Internal, "coverage miss",
				domain.ErrReasonCoverageMiss),
			expectedStatus: http.StatusInternalServerError,
			expectedCode:   "COVERAGE_MISS",
		},
		{
			// The ~20 commonpb.NewNotFoundError sites send no ErrorInfo.
			name:           "bare NotFound with no ErrorInfo",
			leaderErr:      status.Error(codes.NotFound, "ledger foo not found"),
			expectedStatus: http.StatusNotFound,
			expectedCode:   "NOT_FOUND",
		},
		{
			// Forward compatibility: a reason this build's enum does not know
			// must keep the status the wire carried rather than collapsing to a
			// 500 through KindInternal.
			name: "reason from a newer server keeps the wire status",
			leaderErr: leaderStatus(t, codes.AlreadyExists, "something conflicted",
				"SOME_REASON_FROM_A_NEWER_SERVER"),
			expectedStatus: http.StatusConflict,
			expectedCode:   "SOME_REASON_FROM_A_NEWER_SERVER",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/", nil)

			// Exactly what the decorated leader connection hands the handler.
			handleError(w, r, grpcerr.FromStatusError(tc.leaderErr))

			require.Equal(t, tc.expectedStatus, w.Code)

			var resp ErrorResponse
			require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))

			require.Equal(t, tc.expectedCode, resp.ErrorCode)
			require.NotEqual(t, "INTERNAL_ERROR", resp.ErrorCode,
				"a forwarded business error must never be sanitized into INTERNAL_ERROR")
			require.NotContains(t, resp.ErrorMessage, "correlation ID",
				"the sanitizer must not have run")
			require.NotContains(t, resp.ErrorMessage, "gRPC call failed",
				"no transport wrapper may leak into the client-visible message")

			if tc.checkRetry {
				require.Equal(t, "1", w.Header().Get("Retry-After"))
			}
		})
	}
}

// leaderStatusWithMetadata is leaderStatus with a metadata payload, for the
// rows that assert what does and does not reach the client body.
func leaderStatusWithMetadata(t *testing.T, code codes.Code, message, reason string,
	metadata map[string]string,
) error {
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

// TestHandleErrorForwardedInvalidWirePairIsSanitized covers the mismatch
// policy (EN-1980). A reason this build knows, carried under a code this build
// would never send it under, is a protocol fault: the peer is not speaking this
// contract, so nothing it sent may be answered as a business outcome.
//
// The pair below would be answered as a 400 with the peer's own message if the
// original status survived reconstruction — which is precisely why the decoder
// returns the rejection without a GRPCStatus.
func TestHandleErrorForwardedInvalidWirePairIsSanitized(t *testing.T) {
	t.Parallel()

	leaderErr := leaderStatusWithMetadata(t, codes.InvalidArgument,
		"ledger deleted: secret-ledger", domain.ErrReasonLedgerDeleted,
		map[string]string{"name": "secret-ledger"})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil)

	handleError(w, r, grpcerr.FromStatusError(leaderErr))

	require.Equal(t, http.StatusInternalServerError, w.Code,
		"a contradicting reason/code pair is a server-side fault, not a caller error")

	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))

	require.Equal(t, "INTERNAL_ERROR", resp.ErrorCode)
	require.Contains(t, resp.ErrorMessage, "correlation ID",
		"the fault must be recorded server-side and correlatable")
	require.NotContains(t, resp.ErrorCode, domain.ErrReasonLedgerDeleted,
		"the received reason must not be presented as a trusted business code")
	require.NotContains(t, resp.ErrorMessage, "secret-ledger",
		"neither the received message nor its metadata may reach the client")
}

// TestHandleErrorForwardedBareAuthStatusStaysInternal pins the passthrough
// rule for the two kinds with no enum reason. HTTP auth is enforced by the
// middleware at the entry node before any forwarding, so an auth status
// arriving from the leader denotes a cluster-secret failure between nodes, not
// a caller credential problem — surfacing it as 401/403 would tell the caller
// to fix something it does not control.
func TestHandleErrorForwardedBareAuthStatusStaysInternal(t *testing.T) {
	t.Parallel()

	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied} {
		t.Run(code.String(), func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/", nil)

			handleError(w, r, grpcerr.FromStatusError(status.Error(code, "cluster secret mismatch")))

			require.Equal(t, http.StatusInternalServerError, w.Code)

			var resp ErrorResponse
			require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
			require.Equal(t, "INTERNAL_ERROR", resp.ErrorCode)
		})
	}
}

// TestBulkPerElementForwardedFromLeader pins the second consumer of the
// boundary contract. The bulk per-element mapper is a separate dispatch site
// from handleError, and it was broken by the same cause and repaired by the
// same change; a forwarded element rejection must roll up under its own status
// and code rather than the generic 500 / "ERROR" fallback.
func TestBulkPerElementForwardedFromLeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		leaderErr    error
		expectStatus int
		expectCode   string
	}{
		{
			name: "insufficient funds",
			leaderErr: leaderStatus(t, codes.FailedPrecondition, "insufficient funds",
				domain.ErrReasonInsufficientFunds),
			expectStatus: http.StatusBadRequest,
			expectCode:   domain.ErrReasonInsufficientFunds,
		},
		{
			name: "conflict survives the FailedPrecondition collapse",
			leaderErr: leaderStatus(t, codes.FailedPrecondition, "ledger deleted: foo",
				domain.ErrReasonLedgerDeleted),
			expectStatus: http.StatusConflict,
			expectCode:   domain.ErrReasonLedgerDeleted,
		},
		{
			name: "reason from a newer server",
			leaderErr: leaderStatus(t, codes.AlreadyExists, "something conflicted",
				"SOME_REASON_FROM_A_NEWER_SERVER"),
			expectStatus: http.StatusConflict,
			expectCode:   "SOME_REASON_FROM_A_NEWER_SERVER",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			forwarded := grpcerr.FromStatusError(tc.leaderErr)

			require.Equal(t, tc.expectStatus, perElementStatus(forwarded))
			require.Equal(t, tc.expectCode, bulkErrorCode(forwarded))
			require.NotEqual(t, "ERROR", bulkErrorCode(forwarded),
				"a forwarded element rejection must not fall back to the generic code")
		})
	}
}

// TestBulkPerElementForwardedInvalidWirePairIsSanitized is the bulk half of the
// sanitization contract TestHandleErrorForwardedInvalidWirePairIsSanitized pins
// on the unitary path.
//
// perElementStatus and bulkErrorCode already withhold the claimed reason, but
// they are not what the client reads: the description is, and rendering
// result.err.Error() there put the received reason and code into the response
// body. The two consumers of the boundary contract must answer an invalid pair
// identically, so this asserts the client-visible element body directly rather
// than the two mapping helpers.
func TestBulkPerElementForwardedInvalidWirePairIsSanitized(t *testing.T) {
	t.Parallel()

	// LEDGER_DELETED is KindConflict, which this build only ever sends as
	// codes.FailedPrecondition. Arriving as codes.InvalidArgument contradicts
	// it, so nothing in the payload may be answered as a business outcome.
	leaderErr := leaderStatusWithMetadata(t, codes.InvalidArgument,
		"ledger deleted: secret-ledger", domain.ErrReasonLedgerDeleted,
		map[string]string{"name": "secret-ledger"})

	elements := []*servicepb.BulkElement{{Action: &servicepb.LedgerAction{
		Data: &servicepb.LedgerAction_CreateTransaction{
			CreateTransaction: &servicepb.CreateTransactionPayload{},
		},
	}}}

	w := httptest.NewRecorder()
	writeBulkResponse(w, testBulkRequest(), elements,
		[]bulkResult{{err: grpcerr.FromStatusError(leaderErr)}}, true)

	require.Equal(t, http.StatusInternalServerError, w.Code,
		"a contradicting reason/code pair is a server-side fault, not a caller error")

	var resp bulkResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.Len(t, resp.Data, 1)

	element := resp.Data[0]

	require.Equal(t, "ERROR", element.ResponseType)
	require.Equal(t, "INTERNAL_ERROR", element.ErrorCode)
	require.Contains(t, element.ErrorDescription, "correlation ID",
		"the fault must be recorded server-side and correlatable")

	require.NotContains(t, element.ErrorDescription, "secret-ledger",
		"neither the received message nor its metadata may reach the client")
	require.NotContains(t, element.ErrorDescription, domain.ErrReasonLedgerDeleted,
		"the received reason must not be echoed in the element description")
	require.NotContains(t, element.ErrorCode, domain.ErrReasonLedgerDeleted,
		"the received reason must not be presented as a trusted business code")
	require.NotContains(t, element.ErrorDescription, "invalid wire error",
		"the internal representation is log material, not client-visible text")
}
