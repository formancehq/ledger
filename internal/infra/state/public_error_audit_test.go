package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// Public diagnostics are independent of the stable facts in the audit chain.
func TestPublicErrorAuditFactsExcludeDiagnosticText(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		err   domain.SerializableError
		code  string
		facts map[string]string
	}{
		{
			name:  "coverage",
			err:   &ErrCoverageMiss{Attribute: "volumes", CanonicalHex: "deadbeef", IDHex: "0102", RaftIndex: 42},
			code:  "COVERAGE_MISS",
			facts: map[string]string{"attribute": "volumes", "canonicalHex": "deadbeef", "idHex": "0102", "raftIndex": "42"},
		},
		{
			name:  "index",
			err:   &domain.ErrIndexInconsistent{Index: "private-index", Detail: "reading /private/pebble: secret failure"},
			code:  "INDEX_INCONSISTENT",
			facts: map[string]string{"index": "private-index"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, metadata, overridden := domain.PublicErrorDetails(tc.err)
			require.True(t, overridden)
			require.Empty(t, metadata)
			failure := buildAuditFailure(tc.err)
			require.Equal(t, tc.code, failure.GetCode())
			require.Equal(t, tc.facts, failure.GetFacts())
			require.Equal(t, tc.err.Reason(), domain.ReasonString(failure.GetReason()))
		})
	}
}

func TestNumscriptExecutionAuditIgnoresDiagnosticWording(t *testing.T) {
	t.Parallel()
	first := &domain.ErrNumscriptExecution{Detail: "old wording", Code: "NUMSCRIPT_NEGATIVE_AMOUNT", Facts: map[string]string{"amount": "-4"}}
	second := &domain.ErrNumscriptExecution{Detail: "new wording", Code: "NUMSCRIPT_NEGATIVE_AMOUNT", Facts: map[string]string{"amount": "-4"}}
	firstFailure := buildAuditFailure(first)
	secondFailure := buildAuditFailure(second)
	require.Equal(t, firstFailure, secondFailure)
	require.NotContains(t, firstFailure.GetFacts(), "detail")
	require.Equal(t, "NUMSCRIPT_NEGATIVE_AMOUNT", firstFailure.GetCode())
}
