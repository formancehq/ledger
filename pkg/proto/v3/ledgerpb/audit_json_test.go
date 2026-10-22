package ledgerpb_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
)

func TestAuditEntryMarshalJSONKeepsPublicShape(t *testing.T) {
	t.Parallel()

	entry := &ledgerpb.AuditEntry{
		Sequence:    7,
		Timestamp:   &ledgerpb.Timestamp{Data: 1700000000000000},
		ProposalId:  42,
		OrderCount:  1,
		Ledgers:     []string{"main"},
		HashVersion: 3,
		Hash:        []byte{0xde, 0xad, 0xbe, 0xef},
		Outcome: &ledgerpb.AuditEntry_Success{Success: &ledgerpb.AuditSuccess{
			MinLogSequence: 10,
			MaxLogSequence: 12,
		}},
		Items: []*ledgerpb.AuditItem{{OrderIndex: 3, SerializedOrder: []byte{0x01, 0x02}, LogSequence: 11}},
	}

	data, err := entry.MarshalJSON()
	require.NoError(t, err)
	out := string(data)
	for _, field := range []string{"sequence", "proposalId", "orderCount", "hashVersion", "minLogSequence", "maxLogSequence", "orderIndex", "logSequence"} {
		require.Contains(t, out, `"`+field+`"`)
	}
	require.Contains(t, out, `"hash":"deadbeef"`)
	require.Contains(t, out, `"serializedOrder":"0102"`)
	for _, snake := range []string{"proposal_id", "order_count", "hash_version", "min_log_sequence", "max_log_sequence", "order_index", "log_sequence", "serialized_order"} {
		require.NotContains(t, out, snake)
	}
}

func TestAuditEntryMarshalJSONPreservesMeaningfulZeros(t *testing.T) {
	t.Parallel()

	entry := &ledgerpb.AuditEntry{
		Outcome: &ledgerpb.AuditEntry_Success{Success: &ledgerpb.AuditSuccess{}},
		Items:   []*ledgerpb.AuditItem{{}},
	}
	data, err := entry.MarshalJSON()
	require.NoError(t, err)
	for _, field := range []string{"sequence", "proposalId", "orderCount", "hashVersion", "minLogSequence", "maxLogSequence", "orderIndex", "logSequence"} {
		require.Contains(t, string(data), `"`+field+`":0`)
	}
}

func TestAuditEntryMarshalJSONPropagatesSubmessageError(t *testing.T) {
	t.Parallel()

	entry := &ledgerpb.AuditEntry{
		Sequence: 7,
		CallerSnapshot: &ledgerpb.CallerSnapshot{Principal: &ledgerpb.CallerSnapshot_Anonymous{
			Anonymous: &ledgerpb.AnonymousCaller{Scopes: []string{"\xff\xfe"}},
		}},
	}
	_, err := entry.MarshalJSON()
	require.Error(t, err)
	require.Contains(t, err.Error(), "callerSnapshot")
}

func TestAuditFailureMarshalJSONRendersStableFacts(t *testing.T) {
	t.Parallel()

	failure := &ledgerpb.AuditFailure{Details: ledgerpb.MustFailureDetails("INSUFFICIENT_FUNDS", map[string]string{
		"account": "bank", "asset": "USD", "amount": "10", "balance": "0", "color": "",
	})}
	data, err := failure.MarshalJSON()
	require.NoError(t, err)
	out := string(data)
	require.Contains(t, out, `"reason":"ERROR_REASON_INSUFFICIENT_FUNDS"`)
	require.Contains(t, out, `"code":"INSUFFICIENT_FUNDS"`)
	require.Contains(t, out, `"account":"bank"`)
	require.NotContains(t, out, `"message"`)
	require.False(t, strings.Contains(out, "failure_details"))
}
