package ledgerpb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
)

func TestAuditEntryReaderDoesNotExposeMutableStorage(t *testing.T) {
	t.Parallel()

	ledgers := []string{"main", "staging", "audit"}
	entry := &ledgerpb.AuditEntry{Ledgers: ledgers, Hash: []byte{1, 2, 3, 4}}
	reader := entry.AsReader()
	reader.GetLedgers()[0] = "MUTATED"
	reader.GetHash()[0] = 0xff
	require.Equal(t, []string{"main", "staging", "audit"}, entry.GetLedgers())
	require.Equal(t, []byte{1, 2, 3, 4}, entry.GetHash())
}

func TestAuditEntryReaderListProtectsElements(t *testing.T) {
	t.Parallel()

	item := &ledgerpb.AuditItem{OrderIndex: 1, LogSequence: 42}
	entry := &ledgerpb.AuditEntry{Items: []*ledgerpb.AuditItem{item}}
	list := entry.AsReader().GetItems()
	clone := list.Get(0).Mutate()
	clone.OrderIndex = 999
	clone.LogSequence = 888
	require.Equal(t, uint32(1), item.GetOrderIndex())
	require.Equal(t, uint64(42), item.GetLogSequence())
}

func TestAuditEntryReaderRangeYieldsReaderViews(t *testing.T) {
	t.Parallel()

	entry := &ledgerpb.AuditEntry{Items: []*ledgerpb.AuditItem{{OrderIndex: 1}, {OrderIndex: 2}, nil}}
	var indices []int
	var orders []uint32
	entry.AsReader().GetItems().Range(func(i int, item ledgerpb.AuditItemReader) bool {
		indices = append(indices, i)
		if item == nil {
			orders = append(orders, 0)
			return true
		}
		orders = append(orders, item.GetOrderIndex())
		return true
	})
	require.Equal(t, []int{0, 1, 2}, indices)
	require.Equal(t, []uint32{1, 2, 0}, orders)
}
