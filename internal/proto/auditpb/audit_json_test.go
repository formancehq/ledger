package auditpb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestAuditEntry_MarshalJSON_CamelCase guards the camelCase JSON invariant on
// the HTTP audit surface: the default sonic struct-tag encoding would emit
// snake_case (proposal_id, order_count, hash_version, log_sequence, …), which
// this hand-written codec must override.
func TestAuditEntry_MarshalJSON_CamelCase(t *testing.T) {
	t.Parallel()

	entry := &AuditEntry{
		Sequence:    7,
		Timestamp:   &commonpb.Timestamp{Data: 1700000000000000},
		ProposalId:  42,
		OrderCount:  1,
		Ledgers:     []string{"main"},
		HashVersion: 3,
		Hash:        []byte{0xde, 0xad, 0xbe, 0xef},
		Outcome: &AuditEntry_Success{Success: &AuditSuccess{
			MinLogSequence: 10,
			MaxLogSequence: 12,
		}},
		Items: []*AuditItem{
			{OrderIndex: 3, SerializedOrder: []byte{0x01, 0x02}, LogSequence: 11},
		},
	}

	data, err := entry.MarshalJSON()
	require.NoError(t, err)

	out := string(data)

	// camelCase keys present
	require.Contains(t, out, `"sequence":7`)
	require.Contains(t, out, `"proposalId":42`)
	require.Contains(t, out, `"orderCount":1`)
	require.Contains(t, out, `"hashVersion":3`)
	require.Contains(t, out, `"minLogSequence":10`)
	require.Contains(t, out, `"maxLogSequence":12`)
	require.Contains(t, out, `"orderIndex":3`)
	require.Contains(t, out, `"logSequence":11`)
	// typed-nil sub-messages are omitted, not rendered as "{}"
	require.NotContains(t, out, `"callerSnapshot"`)
	require.NotContains(t, out, `"idempotency"`)
	require.NotContains(t, out, `"signature"`)
	// hash + serialized order are hex-encoded
	require.Contains(t, out, `"hash":"deadbeef"`)
	require.Contains(t, out, `"serializedOrder":"0102"`)

	// no snake_case leakage
	for _, snake := range []string{"proposal_id", "order_count", "hash_version", "min_log_sequence", "max_log_sequence", "order_index", "log_sequence", "serialized_order"} {
		require.Falsef(t, strings.Contains(out, snake), "must use camelCase, found %q", snake)
	}
}

// TestAuditFailure_MarshalJSON renders the reason enum as its string name.
func TestAuditFailure_MarshalJSON(t *testing.T) {
	t.Parallel()

	f := &AuditFailure{
		Reason:  commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
		Message: "boom",
		Context: map[string]string{"k": "v"},
	}

	data, err := f.MarshalJSON()
	require.NoError(t, err)

	out := string(data)
	require.Contains(t, out, `"reason":"ERROR_REASON_INSUFFICIENT_FUNDS"`)
	require.Contains(t, out, `"message":"boom"`)
	require.Contains(t, out, `"context":{"k":"v"}`)
}
