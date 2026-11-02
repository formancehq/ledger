package domain

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestPublicErrorDetails(t *testing.T) {
	t.Parallel()

	private := &ErrIndexInconsistent{Index: "private-index", Detail: "private storage detail"}
	ordinary := &ErrLedgerNotFound{Name: "caller-ledger"}
	outer := &ErrStorageOperation{Operation: "loading index", Cause: private}
	for _, tc := range []struct {
		name       string
		err        Describable
		overridden bool
		// wantMetadata is the context PublicErrorDetails must publish. It is
		// spelled out rather than derived from tc.err so the row for `outer`
		// genuinely pins the no-unwrap rule: a helper that walked the chain
		// would answer the nested private error's context here and the
		// assertion would still hold if both sides walked.
		wantMetadata map[string]string
	}{
		{"private", private, true, nil},
		{"business private", &BusinessError{Err: private}, true, nil},
		{"nested business private", &BusinessError{Err: &BusinessError{Err: private}}, true, nil},
		{"ordinary", ordinary, true, map[string]string{"name": "caller-ledger"}},
		{"business ordinary", &BusinessError{Err: ordinary}, true, map[string]string{"name": "caller-ledger"}},
		{"distinct outer reason", outer, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			message, metadata, overridden := PublicErrorDetails(tc.err)
			require.Equal(t, tc.overridden, overridden)
			if tc.err.Reason() == ErrReasonIndexInconsistent {
				require.Equal(t, "index is inconsistent", message)
				require.Empty(t, metadata)
				require.Equal(t, "index private-index is inconsistent: private storage detail", tc.err.Error())
				// Diagnostic identity remains available through the typed error.
				require.Equal(t, "private-index", private.Index)
				require.Equal(t, "private storage detail", private.Detail)
			} else {
				require.True(t, overridden)
				if tc.err.Reason() == ErrReasonLedgerNotFound {
					require.Equal(t, "ledger not found: name=\"caller-ledger\"", message)
				} else {
					require.Equal(t, "storage operation failed", message)
				}
				require.Equal(t, tc.wantMetadata, metadata)
			}
		})
	}
}

func TestNonAuditedErrorsKeepPublicFacts(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		err     Describable
		message string
		facts   map[string]string
	}{
		{"maintenance", ErrMaintenanceMode, "maintenance mode", map[string]string{}},
		{"idempotency lookup", &ErrIdempotencyCheckFailed{Cause: errors.New("private path")}, "idempotency check failed", map[string]string{}},
		{"plan too large", &ErrExecutionPlanTooLarge{Size: 4096, Limit: 1024}, "execution plan too large: limit=\"1024\", size=\"4096\"", map[string]string{"size": "4096", "limit": "1024"}},
		{"discovery opaque", &ErrDependencyDiscoveryFailed{Cause: errors.New("private cause")}, "dependency discovery failed", map[string]string{}},
		{"discovery typed cause", &ErrDependencyDiscoveryFailed{Cause: &ErrLedgerNotFound{Name: "ledger"}}, "ledger not found: name=\"ledger\"", map[string]string{"name": "ledger"}},
		{"discovery public-only cause", &ErrDependencyDiscoveryFailed{Cause: ErrMaintenanceMode}, "maintenance mode", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, serialized := tc.err.(SerializableError)
			require.False(t, serialized)
			message, facts, overridden := PublicErrorDetails(tc.err)
			require.True(t, overridden)
			require.Equal(t, tc.message, message)
			require.Equal(t, tc.facts, facts)
		})
	}
}

func TestBusinessErrorStructuredPublicFailureRetainsCause(t *testing.T) {
	t.Parallel()

	cause := &ErrLedgerNotFound{Name: "caller-ledger"}
	facts := FailureFactsOf(cause)
	business := &BusinessError{Err: cause, PublicFailure: &facts}
	message, metadata, overridden := PublicErrorDetails(business)
	wantMessage, wantMetadata := RenderFailureFacts(facts)
	require.True(t, overridden)
	require.Equal(t, wantMessage, message)
	require.Equal(t, wantMetadata, metadata)
	require.ErrorIs(t, business, cause)
}

func TestPublicErrorDetailsRejectMalformedFacts(t *testing.T) {
	t.Parallel()

	invalid := &ErrNumscriptExecution{
		Detail:  "private diagnostic",
		Details: commonpb.MustFailureDetails("NUMSCRIPT_COMPILE_INVALID_UNCAPPED_SOURCE", nil),
	}
	message, metadata, overridden := PublicErrorDetails(invalid)
	require.True(t, overridden)
	require.Equal(t, "numscript execution error", message)
	require.Empty(t, metadata)

	facts := FailureFactsOf(invalid)
	message, metadata, overridden = PublicErrorDetails(&BusinessError{Err: invalid, PublicFailure: &facts})
	require.True(t, overridden)
	require.Equal(t, "numscript execution error", message)
	require.Empty(t, metadata)
}

// TestPublicErrorDetailsDoesNotWalkTheChain pins the selection rule: an outer
// storage error cannot publish facts or diagnostic text from its inner cause.
func TestPublicErrorDetailsDoesNotWalkTheChain(t *testing.T) {
	t.Parallel()

	inner := &ErrIndexInconsistent{Index: "private-index", Detail: "private storage detail"}
	message, facts, overridden := PublicErrorDetails(&ErrStorageOperation{Operation: "loading index", Cause: inner})
	require.True(t, overridden)
	require.Equal(t, "storage operation failed", message)
	require.Empty(t, facts)
}
