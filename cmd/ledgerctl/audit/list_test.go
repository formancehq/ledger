package audit

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// The --ledger shorthand must carry the raw ledger name through to the proto
// filter. Building the proto directly (rather than round-tripping through the
// filterexpr DSL string) is what lets names containing characters the no-escape
// String grammar cannot represent — quotes and backslashes, both valid per
// domain.ValidateLedgerName — survive verbatim.
func TestBuildAuditFilter_LedgerShorthandPreservesSpecialChars(t *testing.T) {
	t.Parallel()

	for _, ledger := range []string{`a"b`, `a\b`, `plain`, `a"b\c`} {
		filter, err := buildAuditFilter("", false, ledger)
		require.NoError(t, err, "ledger: %q", ledger)

		audit := filter.GetAudit()
		require.NotNil(t, audit, "ledger: %q", ledger)
		require.Equal(t, commonpb.AuditField_AUDIT_FIELD_LEDGER, audit.GetField(), "ledger: %q", ledger)
		require.Equal(t, ledger, audit.GetStringCond().GetHardcoded(), "ledger: %q", ledger)
	}
}

func TestBuildAuditFilter_Combinations(t *testing.T) {
	t.Parallel()

	t.Run("empty returns nil", func(t *testing.T) {
		t.Parallel()
		filter, err := buildAuditFilter("", false, "")
		require.NoError(t, err)
		require.Nil(t, filter)
	})

	t.Run("ledger and failures-only AND-combine", func(t *testing.T) {
		t.Parallel()
		filter, err := buildAuditFilter("", true, "main")
		require.NoError(t, err)
		require.Len(t, filter.GetAnd().GetFilters(), 2)
	})

	t.Run("invalid expr surfaces error", func(t *testing.T) {
		t.Parallel()
		_, err := buildAuditFilter("this is not valid !!", false, "")
		require.Error(t, err)
	})
}
