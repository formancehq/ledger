package filterexpr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// audit is the query target under which the bare audit fields resolve to the
// AuditCondition arm (EN-1549).
const audit = commonpb.QueryTarget_QUERY_TARGET_AUDIT

func TestParseAudit_OutcomeEquality(t *testing.T) {
	t.Parallel()

	filter, err := Parse("outcome == failure", audit)
	require.NoError(t, err)

	ac := filter.GetAudit()
	require.NotNil(t, ac)
	assert.Equal(t, commonpb.AuditField_AUDIT_FIELD_OUTCOME, ac.GetField())
	assert.Equal(t, "failure", ac.GetStringCond().GetHardcoded())
}

func TestParseAudit_LedgerKeyword(t *testing.T) {
	t.Parallel()

	// "ledger" is a keyword; the bare audit field must still resolve on the audit
	// target (to the audit ledger arm, not the LedgerCondition arm).
	filter, err := Parse("ledger == main", audit)
	require.NoError(t, err)

	ac := filter.GetAudit()
	require.NotNil(t, ac)
	assert.Equal(t, commonpb.AuditField_AUDIT_FIELD_LEDGER, ac.GetField())
	assert.Equal(t, "main", ac.GetStringCond().GetHardcoded())
}

func TestParseAudit_CallerSubjectQuoted(t *testing.T) {
	t.Parallel()

	filter, err := Parse(`caller_subject == "svc:payments"`, audit)
	require.NoError(t, err)

	ac := filter.GetAudit()
	require.NotNil(t, ac)
	assert.Equal(t, commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT, ac.GetField())
	assert.Equal(t, "svc:payments", ac.GetStringCond().GetHardcoded())
}

func TestParseAudit_TimestampRFC3339(t *testing.T) {
	t.Parallel()

	// 2023-11-14T22:13:20Z == 1_700_000_000 s == 1_700_000_000_000_000 µs.
	const wantMicros = uint64(1_700_000_000_000_000)

	// RFC3339 (quoted) and raw microseconds must both parse to the same bound.
	for _, in := range []string{
		`timestamp >= "2023-11-14T22:13:20Z"`,
		"timestamp >= 1700000000000000",
	} {
		filter, err := Parse(in, audit)
		require.NoError(t, err, in)

		ac := filter.GetAudit()
		require.NotNil(t, ac, in)
		assert.Equal(t, commonpb.AuditField_AUDIT_FIELD_TIMESTAMP, ac.GetField(), in)
		assert.Equal(t, wantMicros, ac.GetUintCond().GetMin(), in)
	}
}

func TestParseAudit_TimestampRejectsPreEpoch(t *testing.T) {
	t.Parallel()

	_, err := Parse(`timestamp >= "1969-12-31T00:00:00Z"`, audit)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Unix epoch")
}

func TestParseAudit_OrderTypeIn(t *testing.T) {
	t.Parallel()

	filter, err := Parse("order_type in (create_transaction, revert_transaction)", audit)
	require.NoError(t, err)

	or := filter.GetOr()
	require.NotNil(t, or)
	require.Len(t, or.GetFilters(), 2)
	assert.Equal(t, "create_transaction", or.GetFilters()[0].GetAudit().GetStringCond().GetHardcoded())
	assert.Equal(t, "revert_transaction", or.GetFilters()[1].GetAudit().GetStringCond().GetHardcoded())
}

func TestParseAudit_SeqBetween(t *testing.T) {
	t.Parallel()

	filter, err := Parse("seq between 1000 and 2000", audit)
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

	filter, err := Parse("proposal_id == 42", audit)
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

	filter, err := Parse("timestamp >= 1700000000000000", audit)
	require.NoError(t, err)

	uc := filter.GetAudit().GetUintCond()
	require.NotNil(t, uc)
	assert.Equal(t, uint64(1700000000000000), uc.GetMin())
	assert.False(t, uc.GetMinExclusive())
}

func TestParseAudit_Composition(t *testing.T) {
	t.Parallel()

	filter, err := Parse("outcome == failure and ledger == main", audit)
	require.NoError(t, err)

	and := filter.GetAnd()
	require.NotNil(t, and)
	require.Len(t, and.GetFilters(), 2)
	assert.Equal(t, commonpb.AuditField_AUDIT_FIELD_OUTCOME, and.GetFilters()[0].GetAudit().GetField())
	assert.Equal(t, commonpb.AuditField_AUDIT_FIELD_LEDGER, and.GetFilters()[1].GetAudit().GetField())
}

func TestParseAudit_UnknownField(t *testing.T) {
	t.Parallel()

	_, err := Parse("bogus == x", audit)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown audit field")
}

func TestParseAudit_UintFieldRejectsNonNumeric(t *testing.T) {
	t.Parallel()

	_, err := Parse("seq == notanumber", audit)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsigned integer")
}

func TestParseAudit_StringFieldRejectsNotEqual(t *testing.T) {
	t.Parallel()

	// != would require a NOT wrapper that the audit access path cannot serve.
	_, err := Parse("outcome != failure", audit)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "== and in only")
}

// TestParseAudit_AuditFieldsRejectedOffAuditTarget guards that the audit-only
// field names carry no meaning on a non-audit target: they must be rejected
// rather than silently resolving to nothing (EN-1549).
func TestParseAudit_AuditFieldsRejectedOffAuditTarget(t *testing.T) {
	t.Parallel()

	for _, in := range []string{
		"outcome == failure",
		"seq between 1 and 2",
		"proposal_id == 42",
		"log_seq == 500",
		"caller_subject == svc",
		"order_type == create_transaction",
	} {
		for _, target := range []commonpb.QueryTarget{
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			commonpb.QueryTarget_QUERY_TARGET_LOGS,
		} {
			_, err := Parse(in, target)
			require.Error(t, err, "%q must be rejected on %s", in, target)
			assert.Contains(t, err.Error(), "unknown field", in)
		}
	}
}

// TestParseAudit_KeywordAsBareValue guards that the field keywords can still be
// used as unquoted right-hand-side values, while structural operators stay
// reserved. (`audit` is no longer a keyword after EN-1549.)
func TestParseAudit_KeywordAsBareValue(t *testing.T) {
	t.Parallel()

	// Reserved "noun" keywords must still parse as bare values.
	for _, kw := range []string{"ledger", "source", "destination", "metadata", "address", "exists"} {
		f, err := Parse("metadata[type] == "+kw, audit)
		require.NoError(t, err, "metadata[type] == %s should parse", kw)
		assert.Equal(t, kw, f.GetField().GetStringCond().GetHardcoded())
	}

	// Structural operators must NOT be swallowed as values.
	for _, op := range []string{"and", "or", "not", "in", "between"} {
		_, err := Parse("metadata[type] == "+op, audit)
		require.Error(t, err, "metadata[type] == %s must not parse (structural keyword)", op)
	}

	// `audit` is now an ordinary identifier and parses as a bare value.
	f, err := Parse("metadata[a] == audit and metadata[b] == ledger", audit)
	require.NoError(t, err)
	require.NotNil(t, f.GetAnd())
	require.Len(t, f.GetAnd().GetFilters(), 2)
}

func TestFormatAudit_RoundTrip(t *testing.T) {
	t.Parallel()

	for _, in := range []string{
		"outcome == failure",
		"ledger == main",
		// A value with a special char (`:`) must be quoted (EN-1547); Format emits
		// and Parse reads the quoted form.
		`caller_subject == "svc:payments"`,
		"seq == 42",
		"seq between 1000 and 2000",
		`timestamp >= "2023-11-14T22:13:20Z"`,
		"proposal_id < 100",
		"outcome == failure and ledger == main",
	} {
		f, err := Parse(in, audit)
		require.NoError(t, err, in)
		assert.Equal(t, in, Format(f), "round-trip for %q", in)
	}
}
