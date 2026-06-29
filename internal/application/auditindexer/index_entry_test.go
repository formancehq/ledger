package auditindexer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func mustOrderBytes(t *testing.T, o *raftcmdpb.Order) []byte {
	t.Helper()
	b, err := proto.Marshal(o)
	require.NoError(t, err)
	return b
}

func TestAppendEntryKeys(t *testing.T) {
	t.Parallel()

	entry := &auditpb.AuditEntry{
		Sequence:       9,
		ProposalId:     3,
		Timestamp:      &commonpb.Timestamp{Data: 1_000_000}, // 1 second in HLC micros
		Outcome:        &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
		Ledgers:        []string{"a", "b"},
		CallerSnapshot: &commonpb.CallerSnapshot{Identity: &commonpb.CallerIdentity{Subject: "alice"}},
	}
	createTx := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
		LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{}}}}}}
	items := []*auditpb.AuditItem{
		{OrderIndex: 0, LogSequence: 100, SerializedOrder: mustOrderBytes(t, createTx)},
		{OrderIndex: 1, LogSequence: 0},
	}

	var keys [][]byte
	emit := func(k []byte) error { keys = append(keys, append([]byte{}, k...)); return nil }

	require.NoError(t, appendEntryKeys(dal.NewKeyBuilder(), emit, entry, items))

	require.Len(t, keys, 8) // outcome + 2 ledgers + caller + order_type + timestamp + proposal_id + log_seq(100)

	for _, k := range keys {
		require.Equal(t, readstore.PrefixInternal, k[0])
		require.Equal(t, readstore.SubInternalAuditIndex, k[1])
	}

	var orderTypeKeys int
	for _, k := range keys {
		if k[2] == readstore.AuditFieldOrderType {
			orderTypeKeys++
		}
	}
	require.Equal(t, 1, orderTypeKeys)
}

func TestAppendEntryKeysFailureNilCaller(t *testing.T) {
	t.Parallel()

	entry := &auditpb.AuditEntry{
		Sequence:   2,
		ProposalId: 1,
		Timestamp:  &commonpb.Timestamp{Data: 1_000_000},
		Outcome:    &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{}},
		Ledgers:    []string{"x"},
	}

	var keys [][]byte
	emit := func(k []byte) error { keys = append(keys, append([]byte{}, k...)); return nil }
	require.NoError(t, appendEntryKeys(dal.NewKeyBuilder(), emit, entry, nil))

	require.Len(t, keys, 4) // outcome + ledger("x") + timestamp + proposal_id
	for _, k := range keys {
		require.NotEqual(t, readstore.AuditFieldCallerSubject, k[2])
	}
}
