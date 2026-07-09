package filterexpr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestParseAudit_OutcomeEquality(t *testing.T) {
	t.Parallel()

	filter, err := Parse("audit[outcome] == failure")
	require.NoError(t, err)

	ac := filter.GetAudit()
	require.NotNil(t, ac)
	assert.Equal(t, commonpb.AuditField_AUDIT_FIELD_OUTCOME, ac.GetField())
	assert.Equal(t, "failure", ac.GetStringCond().GetHardcoded())
}

func TestParseAudit_LedgerKeyword(t *testing.T) {
	t.Parallel()

	// "ledger" is a keyword; the audit field key must still parse.
	filter, err := Parse("audit[ledger] == main")
	require.NoError(t, err)

	ac := filter.GetAudit()
	require.NotNil(t, ac)
	assert.Equal(t, commonpb.AuditField_AUDIT_FIELD_LEDGER, ac.GetField())
	assert.Equal(t, "main", ac.GetStringCond().GetHardcoded())
}

func TestParseAudit_CallerSubjectQuoted(t *testing.T) {
	t.Parallel()

	filter, err := Parse(`audit[caller.subject] == "svc:payments"`)
	require.NoError(t, err)

	ac := filter.GetAudit()
	require.NotNil(t, ac)
	assert.Equal(t, commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT, ac.GetField())
	assert.Equal(t, "svc:payments", ac.GetStringCond().GetHardcoded())
}

func TestParseAudit_OrderTypeIn(t *testing.T) {
	t.Parallel()

	filter, err := Parse("audit[order_type] in (create_transaction, revert_transaction)")
	require.NoError(t, err)

	or := filter.GetOr()
	require.NotNil(t, or)
	require.Len(t, or.GetFilters(), 2)
	assert.Equal(t, "create_transaction", or.GetFilters()[0].GetAudit().GetStringCond().GetHardcoded())
	assert.Equal(t, "revert_transaction", or.GetFilters()[1].GetAudit().GetStringCond().GetHardcoded())
}

func TestParseAudit_SeqBetween(t *testing.T) {
	t.Parallel()

	filter, err := Parse("audit[seq] between 1000 and 2000")
	require.NoError(t, err)

	ac := filter.GetAudit()
	require.NotNil(t, ac)
	assert.Equal(t, commonpb.AuditField_AUDIT_FIELD_SEQUENCE, ac.GetField())
	uc := ac.GetUintCond()
	require.NotNil(t, uc)
	assert.Equal(t, uint64(1000), uc.GetMin())
	assert.Equal(t, uint64(2000), uc.GetMax())
}

func TestParseAudit_ProposalIDEquality(t *testing.T) {
	t.Parallel()

	filter, err := Parse("audit[proposal_id] == 42")
	require.NoError(t, err)

	ac := filter.GetAudit()
	require.NotNil(t, ac)
	assert.Equal(t, commonpb.AuditField_AUDIT_FIELD_PROPOSAL_ID, ac.GetField())
	uc := ac.GetUintCond()
	require.Equal(t, uint64(42), uc.GetMin())
	assert.Equal(t, uint64(42), uc.GetMax())
}

func TestParseAudit_TimestampGte(t *testing.T) {
	t.Parallel()

	filter, err := Parse("audit[timestamp] >= 1700000000000000")
	require.NoError(t, err)

	uc := filter.GetAudit().GetUintCond()
	require.NotNil(t, uc)
	assert.Equal(t, uint64(1700000000000000), uc.GetMin())
	assert.False(t, uc.GetMinExclusive())
}

func TestParseAudit_Composition(t *testing.T) {
	t.Parallel()

	filter, err := Parse("audit[outcome] == failure and audit[ledger] == main")
	require.NoError(t, err)

	and := filter.GetAnd()
	require.NotNil(t, and)
	require.Len(t, and.GetFilters(), 2)
	assert.Equal(t, commonpb.AuditField_AUDIT_FIELD_OUTCOME, and.GetFilters()[0].GetAudit().GetField())
	assert.Equal(t, commonpb.AuditField_AUDIT_FIELD_LEDGER, and.GetFilters()[1].GetAudit().GetField())
}

func TestParseAudit_UnknownField(t *testing.T) {
	t.Parallel()

	_, err := Parse("audit[bogus] == x")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown audit field")
}

func TestParseAudit_UintFieldRejectsNonNumeric(t *testing.T) {
	t.Parallel()

	_, err := Parse("audit[seq] == notanumber")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsigned integer")
}

func TestParseAudit_StringFieldRejectsNotEqual(t *testing.T) {
	t.Parallel()

	// != would require a NOT wrapper that the audit access path cannot serve.
	_, err := Parse("audit[outcome] != failure")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "== and in only")
}

// TestParseAudit_KeywordAsBareValue guards that introducing the `audit`
// keyword (and the pre-existing field keywords) does not stop them being used
// as unquoted right-hand-side values, while structural operators stay reserved.
func TestParseAudit_KeywordAsBareValue(t *testing.T) {
	t.Parallel()

	// Reserved "noun" keywords must still parse as bare values.
	for _, kw := range []string{"audit", "ledger", "source", "destination", "metadata", "address", "exists"} {
		f, err := Parse("metadata[type] == " + kw)
		require.NoError(t, err, "metadata[type] == %s should parse", kw)
		assert.Equal(t, kw, f.GetField().GetStringCond().GetHardcoded())
	}

	// Structural operators must NOT be swallowed as values.
	for _, op := range []string{"and", "or", "not", "in", "between"} {
		_, err := Parse("metadata[type] == " + op)
		require.Error(t, err, "metadata[type] == %s must not parse (structural keyword)", op)
	}

	// Composition still works after the value change.
	f, err := Parse("metadata[a] == audit and metadata[b] == ledger")
	require.NoError(t, err)
	require.NotNil(t, f.GetAnd())
	require.Len(t, f.GetAnd().GetFilters(), 2)
}

func TestFormatAudit_RoundTrip(t *testing.T) {
	t.Parallel()

	for _, in := range []string{
		"audit[outcome] == failure",
		"audit[ledger] == main",
		"audit[caller.subject] == svc:payments",
		"audit[seq] == 42",
		"audit[seq] between 1000 and 2000",
		"audit[timestamp] >= 1700000000000000",
		"audit[proposal_id] < 100",
		"audit[outcome] == failure and audit[ledger] == main",
	} {
		f, err := Parse(in)
		require.NoError(t, err, in)
		assert.Equal(t, in, Format(f), "round-trip for %q", in)
	}
}
