package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// Read-only index failures retain a safe public presentation but have no
// audited failure details.
func TestPublicIndexFailureExcludesDiagnosticText(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  domain.Describable
	}{
		{
			name: "index",
			err:  &domain.ErrIndexInconsistent{Index: "private-index", Detail: "reading /private/pebble: secret failure"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			message, metadata, overridden := domain.PublicErrorDetails(tc.err)
			require.True(t, overridden)
			require.Equal(t, "index is inconsistent", message)
			require.Empty(t, metadata)
		})
	}
}

func TestNumscriptExecutionAuditIgnoresDiagnosticWording(t *testing.T) {
	t.Parallel()
	first := &domain.ErrNumscriptExecution{Detail: "old wording", Details: ledgerpb.MustFailureDetails("NUMSCRIPT_NEGATIVE_AMOUNT", map[string]string{"amount": "-4"})}
	second := &domain.ErrNumscriptExecution{Detail: "new wording", Details: ledgerpb.MustFailureDetails("NUMSCRIPT_NEGATIVE_AMOUNT", map[string]string{"amount": "-4"})}
	firstFailure := buildAuditFailure(first)
	secondFailure := buildAuditFailure(second)
	require.Equal(t, firstFailure, secondFailure)
	require.NotContains(t, firstFailure.GetFacts(), "detail")
	require.Equal(t, "NUMSCRIPT_NEGATIVE_AMOUNT", firstFailure.GetCode())
}
