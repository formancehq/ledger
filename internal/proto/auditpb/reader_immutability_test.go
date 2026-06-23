package auditpb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
)

// These tests pin the immutability contract of protoc-gen-reader: a Reader view
// must never expose a mutable handle on the underlying message. They guard the
// invariant from CLAUDE.md that cache/FSM state must be identical on every node.

func TestAuditEntryReader_GetLedgers_ReturnsIndependentSlice(t *testing.T) {
	t.Parallel()

	original := []string{"main", "staging", "audit"}
	entry := &auditpb.AuditEntry{Ledgers: original}
	r := entry.AsReader()

	got := r.GetLedgers()
	got[0] = "MUTATED"

	require.Equal(t, []string{"main", "staging", "audit"}, entry.GetLedgers())
	require.Equal(t, []string{"main", "staging", "audit"}, original)
}

func TestAuditEntryReader_GetHash_ReturnsIndependentBytes(t *testing.T) {
	t.Parallel()

	entry := &auditpb.AuditEntry{Hash: []byte{0x01, 0x02, 0x03, 0x04}}
	r := entry.AsReader()

	got := r.GetHash()
	got[0] = 0xFF

	require.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, entry.GetHash())
}

func TestAuditEntryReader_GetItems_ListReaderProtectsElements(t *testing.T) {
	t.Parallel()

	item := &auditpb.AuditItem{OrderIndex: 1, LogSequence: 42}
	entry := &auditpb.AuditEntry{Items: []*auditpb.AuditItem{item}}
	r := entry.AsReader()

	list := r.GetItems()
	require.Equal(t, 1, list.Len())

	// The only way to mutate is through Mutate(), which deep-clones — the
	// original AuditItem must stay untouched.
	clone := list.Get(0).Mutate()
	clone.OrderIndex = 999
	clone.LogSequence = 888

	require.Equal(t, uint32(1), item.GetOrderIndex())
	require.Equal(t, uint64(42), item.GetLogSequence())
}

func TestAuditEntryReader_GetItems_RangeYieldsReaderViews(t *testing.T) {
	t.Parallel()

	entry := &auditpb.AuditEntry{
		Items: []*auditpb.AuditItem{
			{OrderIndex: 1},
			{OrderIndex: 2},
			nil,
		},
	}
	r := entry.AsReader()

	var indices []int
	var orders []uint32
	r.GetItems().Range(func(i int, item auditpb.AuditItemReader) bool {
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

func TestAuditEntryReader_GetLedgers_NilStaysNil(t *testing.T) {
	t.Parallel()

	entry := &auditpb.AuditEntry{}
	r := entry.AsReader()

	require.Nil(t, r.GetLedgers())
	require.Nil(t, r.GetHash())
}
