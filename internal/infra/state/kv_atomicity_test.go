package state

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func putKVOrder(ledger, key string, value []byte) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: ledger,
		Payload: &raftcmdpb.LedgerScopedOrder_PutKv{PutKv: &raftcmdpb.PutKVOrder{
			Key: key, Value: value,
		}},
	}}}
}

func deleteKVOrder(ledger, key string) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: ledger,
		Payload: &raftcmdpb.LedgerScopedOrder_DeleteKv{DeleteKv: &raftcmdpb.DeleteKVOrder{
			Key: key,
		}},
	}}}
}

func persistedKV(t *testing.T, store *dal.Store, ledger, key string) ([]byte, bool) {
	t.Helper()
	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	value, closer, err := handle.Get(dal.LedgerKVKey(ledger, key))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false
	}
	require.NoError(t, err)
	defer func() { require.NoError(t, closer.Close()) }()

	return bytes.Clone(value), true
}

func TestLedgerKV_FailedProposalDoesNotLeakIntoNextProposal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		staged  func() *raftcmdpb.Order
		initial []byte
	}{
		{name: "put", staged: func() *raftcmdpb.Order { return putKVOrder("ledger", "key", []byte("rejected")) }},
		{name: "delete", staged: func() *raftcmdpb.Order { return deleteKVOrder("ledger", "key") }, initial: []byte("retained")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			machine, store, _ := newTestMachine(t)
			result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, makeProposal(1, createLedgerOrder("ledger"))))
			require.NoError(t, err)
			require.NoError(t, result.Results[0].Error)
			if tc.initial != nil {
				result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 2, makeProposal(2, putKVOrder("ledger", "key", tc.initial))))
				require.NoError(t, err)
				require.NoError(t, result.Results[0].Error)
			}
			failedIndex := uint64(2)
			if tc.initial != nil {
				failedIndex = 3
			}
			result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, failedIndex, makeProposal(failedIndex,
				tc.staged(), deleteLedgerOrder("missing"))))
			require.NoError(t, err)
			require.Error(t, result.Results[0].Error, "the second order must reject the whole proposal")
			require.Empty(t, result.Results[0].Logs)

			// The next proposal flushes the same WriteSet. A stale KV mutation would
			// persist here despite the preceding audit failure.
			nextIndex := failedIndex + 1
			result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, nextIndex, makeProposal(nextIndex, createLedgerOrder("other"))))
			require.NoError(t, err)
			require.NoError(t, result.Results[0].Error)
			value, exists := persistedKV(t, store, "ledger", "key")
			require.Equal(t, tc.initial != nil, exists)
			require.Equal(t, tc.initial, value)
		})
	}
}

func TestLedgerKV_DeletedLedgerDoesNotResurrectAfterNextProposal(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	for index, order := range []*raftcmdpb.Order{
		createLedgerOrder("ledger"),
		putKVOrder("ledger", "key", []byte("before deletion")),
		deleteLedgerOrder("ledger"),
		createLedgerOrder("other"),
	} {
		sequence := uint64(index + 1)
		result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, sequence, makeProposal(sequence, order)))
		require.NoError(t, err)
		require.NoError(t, result.Results[0].Error)
		if sequence >= 3 {
			_, exists := persistedKV(t, store, "ledger", "key")
			require.False(t, exists, "a later proposal must not flush the deleted ledger's old KV mutation")
		}
	}
}
