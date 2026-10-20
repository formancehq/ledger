package filterexpr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

const logs = commonpb.QueryTarget_QUERY_TARGET_LOGS

// TestDualFormatFieldParity locks the field-coverage parity between the two
// filter serializations: every server-valid condition listed here must be
// expressible in BOTH the textual filterexpr grammar and the structured JSON
// DSL, decode to the identical proto, and pass the per-target validity gate.
// A field reachable through only one form is a contract hole (a prepared query
// created in one form cannot be exported, edited or re-applied in the other).
func TestDualFormatFieldParity(t *testing.T) {
	t.Parallel()

	const rfc = "2023-11-14T22:13:20Z"

	cases := []struct {
		target  commonpb.QueryTarget
		textual string
		json    string
	}{
		// Transactions: reference / reverted.
		{tx, `reference == "ref-1"`, `{"$match":{"reference":"ref-1"}}`},
		{tx, `reference == $ref`, `{"$match":{"reference":{"$param":"ref"}}}`},
		{tx, `reference in (a, "b-2")`, `{"$or":[{"$match":{"reference":"a"}},{"$match":{"reference":"b-2"}}]}`},
		{tx, `reverted == true`, `{"$match":{"reverted":true}}`},
		{tx, `reverted == false`, `{"$match":{"reverted":false}}`},
		// Transactions: builtin uint ranges.
		{tx, `id == 3`, `{"$match":{"id":3}}`},
		{tx, `id == 3`, `{"$match":{"id":"3"}}`},
		{tx, `id >= 3`, `{"$gte":{"id":3}}`},
		{tx, `id between 3 and 9`, `{"$and":[{"$gte":{"id":3}},{"$lte":{"id":9}}]}`},
		{tx, `id > 3 and id < 9`, `{"$and":[{"$gt":{"id":3}},{"$lt":{"id":9}}]}`},
		// An exclusive closed range nested in a wider `and` must stay one
		// condition through Format/Parse (parenthesized, so it still folds).
		{tx, `metadata[a] == x and (id > 3 and id < 9)`, `{"$and":[{"$match":{"metadata[a]":"x"}},{"$and":[{"$gt":{"id":3}},{"$lt":{"id":9}}]}]}`},
		{tx, `insertedAt >= "` + rfc + `"`, `{"$gte":{"insertedAt":"` + rfc + `"}}`},
		{tx, `revertedAt < 1700000000000000`, `{"$lt":{"revertedAt":1700000000000000}}`},
		{tx, `timestamp == "` + rfc + `"`, `{"$match":{"timestamp":"` + rfc + `"}}`},
		// Parameterized range bounds: a prepared query resolves them at
		// execution. Format must emit them as $name, never drop one side of a
		// mixed literal/parameter range or emit an unparseable placeholder.
		{tx, `id == $n`, `{"$match":{"id":{"$param":"n"}}}`},
		{tx, `id >= $n`, `{"$gte":{"id":{"$param":"n"}}}`},
		{tx, `id >= 3 and id <= $hi`, `{"$and":[{"$gte":{"id":3}},{"$lte":{"id":{"$param":"hi"}}}]}`},
		{tx, `id between $lo and $hi`, `{"$and":[{"$gte":{"id":{"$param":"lo"}}},{"$lte":{"id":{"$param":"hi"}}}]}`},
		{tx, `id > $lo and id < 9`, `{"$and":[{"$gt":{"id":{"$param":"lo"}}},{"$lt":{"id":9}}]}`},
		{tx, `timestamp > $from`, `{"$gt":{"timestamp":{"$param":"from"}}}`},
		{tx, `insertedAt >= "` + rfc + `" and insertedAt < $to`, `{"$and":[{"$gte":{"insertedAt":"` + rfc + `"}},{"$lt":{"insertedAt":{"$param":"to"}}}]}`},
		{tx, `revertedAt <= $to`, `{"$lte":{"revertedAt":{"$param":"to"}}}`},
		// Logs: logId / date.
		{logs, `logId == 7`, `{"$match":{"logId":7}}`},
		{logs, `logId > 7 and logId <= 9`, `{"$and":[{"$gt":{"logId":7}},{"$lte":{"logId":9}}]}`},
		{logs, `date == "` + rfc + `"`, `{"$match":{"date":"` + rfc + `"}}`},
		{logs, `logId >= $n`, `{"$gte":{"logId":{"$param":"n"}}}`},
		{logs, `date <= $to`, `{"$lte":{"date":{"$param":"to"}}}`},
		// Audit: the bare audit fields resolve to the audit arm in BOTH forms.
		{audit, `outcome == failure`, `{"$match":{"outcome":"failure"}}`},
		{audit, `idempotency_key == "payment-42"`, `{"$match":{"idempotency_key":"payment-42"}}`},
		{audit, `ledger == main`, `{"$match":{"ledger":"main"}}`},
		{audit, `caller_subject == "svc:payments"`, `{"$match":{"caller_subject":"svc:payments"}}`},
		{audit, `order_type == create_transaction`, `{"$match":{"order_type":"create_transaction"}}`},
		{audit, `timestamp >= "` + rfc + `"`, `{"$gte":{"timestamp":"` + rfc + `"}}`},
		{audit, `seq == 42`, `{"$match":{"seq":42}}`},
		{audit, `seq between 1 and 9`, `{"$and":[{"$gte":{"seq":1}},{"$lte":{"seq":9}}]}`},
		{audit, `seq > 1 and seq < 9`, `{"$and":[{"$gt":{"seq":1}},{"$lt":{"seq":9}}]}`},
		{audit, `outcome == failure and (seq >= 1 and seq < 9)`, `{"$and":[{"$match":{"outcome":"failure"}},{"$and":[{"$gte":{"seq":1}},{"$lt":{"seq":9}}]}]}`},
		{audit, `proposal_id < 100`, `{"$lt":{"proposal_id":100}}`},
		{audit, `log_seq > 5`, `{"$gt":{"log_seq":5}}`},
		{audit, `outcome in (success, failure)`, `{"$or":[{"$match":{"outcome":"success"}},{"$match":{"outcome":"failure"}}]}`},
		{audit, `outcome == failure and ledger == main`, `{"$and":[{"$match":{"outcome":"failure"}},{"$match":{"ledger":"main"}}]}`},
	}

	for _, tc := range cases {
		t.Run(tc.target.String()+"/"+tc.textual, func(t *testing.T) {
			t.Parallel()

			fromText, err := DecodeDualFormat([]byte(tc.textual), tc.target)
			require.NoError(t, err, "textual form must decode and pass the target gate")

			fromJSON, err := DecodeDualFormat([]byte(tc.json), tc.target)
			require.NoError(t, err, "JSON form must decode and pass the target gate")

			require.True(t, proto.Equal(fromText, fromJSON),
				"both forms must decode to the same proto\n textual: %v\n json:    %v", fromText, fromJSON)

			// Format is the canonical textual export (ledgerctl config export,
			// queries list): it must emit a string that re-parses to the same
			// proto on the same target.
			formatted := Format(fromText)
			reparsed, err := Parse(formatted, tc.target)
			require.NoError(t, err, "Format output %q must re-parse", formatted)
			require.True(t, proto.Equal(fromText, reparsed),
				"Format/Parse round-trip changed the filter\n formatted: %s\n before: %v\n after:  %v", formatted, fromText, reparsed)
		})
	}
}

// TestParseIntrinsicFields_Shapes pins the proto each new bare intrinsic field
// resolves to on a non-audit target.
func TestParseIntrinsicFields_Shapes(t *testing.T) {
	t.Parallel()

	f, err := Parse(`reference == "ref-1"`, tx)
	require.NoError(t, err)
	assert.Equal(t, "ref-1", f.GetReference().GetCond().GetHardcoded())

	f, err = Parse(`reference == $ref`, tx)
	require.NoError(t, err)
	assert.Equal(t, "ref", f.GetReference().GetCond().GetParam())

	f, err = Parse(`reverted == true`, tx)
	require.NoError(t, err)
	require.NotNil(t, f.GetReverted())
	assert.True(t, f.GetReverted().GetValue())

	for field, want := range map[string]commonpb.TransactionBuiltinIndex{
		"id":         commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID,
		"insertedAt": commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT,
		"revertedAt": commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT,
	} {
		f, err = Parse(field+" >= 10", tx)
		require.NoError(t, err, field)
		require.NotNil(t, f.GetBuiltinUint(), field)
		assert.Equal(t, want, f.GetBuiltinUint().GetField(), field)
		assert.Equal(t, uint64(10), f.GetBuiltinUint().GetCond().GetMin(), field)
	}

	f, err = Parse(`logId == 7`, logs)
	require.NoError(t, err)
	require.NotNil(t, f.GetLogId())
	assert.Equal(t, uint64(7), f.GetLogId().GetCond().GetMin())
	assert.Equal(t, uint64(7), f.GetLogId().GetCond().GetMax())
}

// TestParseIntrinsicFields_Rejections covers the operator/value shapes the new
// bare intrinsic fields refuse, and their rejection on the audit target.
func TestParseIntrinsicFields_Rejections(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in      string
		target  commonpb.QueryTarget
		wantErr string
	}{
		{`reference > x`, tx, "reference field supports == and in only"},
		{`reference ^= x`, tx, "reference field supports == and in only"},
		{`reverted == $r`, tx, "reverted field does not support parameters"},
		{`reverted == maybe`, tx, "reverted field requires true or false"},
		{`reverted == "true"`, tx, "reverted field requires true or false"},
		{`reverted != true`, tx, "reverted field supports == only"},
		{`id == -1`, tx, "unsigned integer"},
		{`id == abc`, tx, "unsigned integer"},
		{`id == "2023-11-14T22:13:20Z"`, tx, "unsigned integer"},
		{`id ^= 1`, tx, "prefix operator"},
		// Audit ranges have no parameter-resolution context.
		{`seq >= $from`, audit, "does not support parameters"},
		{`timestamp between $a and $b`, audit, "does not support parameters"},
		{`logId == x`, logs, "unsigned integer"},
		{`insertedAt >= "not-a-date"`, tx, "RFC3339"},
		// Intrinsic-only names carry no meaning on the audit target.
		{`reference == x`, audit, "unknown audit field"},
		{`id == 1`, audit, "unknown audit field"},
		{`logId == 1`, audit, "unknown audit field"},
	}

	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(tc.in, tc.target)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestDecodeDualFormat_NewFieldsGatedPerTarget confirms the new bare fields go
// through the same per-target validity gate as their JSON counterparts: a
// transaction-only field is rejected on accounts/logs, a log-only field on
// transactions.
func TestDecodeDualFormat_NewFieldsGatedPerTarget(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		raw    string
		target commonpb.QueryTarget
	}{
		{`reference == x`, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS},
		{`reverted == true`, logs},
		{`id == 1`, logs},
		{`logId == 1`, tx},
		{`{"$match":{"logId":1}}`, tx},
		{`{"$match":{"id":1}}`, logs},
	} {
		_, err := DecodeDualFormat([]byte(tc.raw), tc.target)
		require.Error(t, err, "%s on %s", tc.raw, tc.target)
		assert.Contains(t, err.Error(), "not valid on", tc.raw)
	}
}

// TestDecodeDualFormat_AuditJSONRejections covers the structured audit shapes
// that stay rejected: parameters (audit reads have no parameter context) and
// audit-only names off the audit target.
func TestDecodeDualFormat_AuditJSONRejections(t *testing.T) {
	t.Parallel()

	cases := []struct {
		raw     string
		target  commonpb.QueryTarget
		wantErr string
	}{
		{`{"$match":{"outcome":{"$param":"o"}}}`, audit, "does not support parameters"},
		{`{"$gte":{"seq":{"$param":"s"}}}`, audit, "does not support parameters"},
		{`{"$match":{"seq":"x"}}`, audit, "expected uint value"},
		{`{"$gt":{"outcome":"failure"}}`, audit, "supports $match only"},
		{`{"$exists":{"outcome":"failure"}}`, audit, "unsupported field"},
		// A non-audit condition is still gated off the audit target.
		{`{"$match":{"reference":"x"}}`, audit, "not valid on"},
		// Audit-only names carry no meaning off the audit target.
		{`{"$match":{"outcome":"failure"}}`, tx, "unsupported field"},
		{`{"$match":{"idempotency_key":"k"}}`, tx, "unsupported field"},
	}

	for _, tc := range cases {
		t.Run(tc.target.String()+"/"+tc.raw, func(t *testing.T) {
			t.Parallel()

			_, err := DecodeDualFormat([]byte(tc.raw), tc.target)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestDecodeDualFormatStructuralOnly_NonAuditResolutionIsTargetIndependent
// locks the assumption the prepared-query update paths (ledgerctl queries
// update, PATCH prepared query) rely on when they decode with a hard-coded
// TRANSACTIONS target: every non-audit target resolves every bare intrinsic
// field to the same proto, in both forms, so the stored target (applied by the
// FSM gate) is never needed for resolution.
func TestDecodeDualFormatStructuralOnly_NonAuditResolutionIsTargetIndependent(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		`id == 1`, `timestamp >= 1`, `insertedAt < 2`, `revertedAt > 3`,
		`reference == r`, `reverted == true`, `logId between 1 and 2`, `date <= 4`,
		`ledger == main`, `metadata[k] == v`, `address ^= "users:"`, `has asset USD/2`,
		`{"$match":{"id":1}}`, `{"$match":{"reference":"r"}}`, `{"$match":{"ledger":"main"}}`,
		`{"$gte":{"timestamp":1}}`, `{"$match":{"logId":1}}`,
	} {
		want, err := DecodeDualFormatStructuralOnly([]byte(raw), tx)
		require.NoError(t, err, raw)

		for _, target := range []commonpb.QueryTarget{commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, logs} {
			got, err := DecodeDualFormatStructuralOnly([]byte(raw), target)
			require.NoError(t, err, "%s on %s", raw, target)
			require.True(t, proto.Equal(want, got), "%s resolves differently on %s", raw, target)
		}
	}
}
