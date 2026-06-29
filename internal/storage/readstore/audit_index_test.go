package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(t.TempDir(), logging.NopZap(), DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

func TestAuditIndexKeysAreInternalNamespaced(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()
	key := AuditIndexStringKey(kb, AuditFieldLedger, "ledger-a", 42)

	require.Equal(t, PrefixInternal, key[0])
	require.Equal(t, SubInternalAuditIndex, key[1])
	require.Equal(t, AuditFieldLedger, key[2])
	require.Equal(t, len(key), readStoreSplit(key), "audit-index key must not be split")
}

func TestAuditIndexUint64KeyOrdersByValueThenSeq(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()
	k1 := AuditIndexUint64Key(kb, AuditFieldProposalID, 5, 100)
	k2 := AuditIndexUint64Key(kb, AuditFieldProposalID, 5, 101)
	k3 := AuditIndexUint64Key(kb, AuditFieldProposalID, 6, 1)

	require.Negative(t, bytesCompare(k1, k2), "same value: lower seq sorts first")
	require.Negative(t, bytesCompare(k2, k3), "higher value sorts after lower value")
}

func bytesCompare(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}

			return 1
		}
	}

	return len(a) - len(b)
}

func TestAuditProgressRoundTrip(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	got, err := s.ReadAuditProgress()
	require.NoError(t, err)
	require.Zero(t, got, "missing cursor reads as 0")

	batch := s.NewBatch()
	require.NoError(t, s.WriteAuditProgress(batch, 7))
	require.NoError(t, batch.Commit())

	got, err = s.ReadAuditProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(7), got)
}

func TestSeekAuditEqualityAndRangeAndDrop(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()

	const highSeq = uint64(0xFF00000000000001)
	batch := s.NewBatch()
	require.NoError(t, batch.SetBytes(AuditIndexStringKey(kb, AuditFieldLedger, "a", 1), nil))
	require.NoError(t, batch.SetBytes(AuditIndexStringKey(kb, AuditFieldLedger, "a", 4), nil))
	require.NoError(t, batch.SetBytes(AuditIndexStringKey(kb, AuditFieldLedger, "a", highSeq), nil))
	require.NoError(t, batch.SetBytes(AuditIndexStringKey(kb, AuditFieldLedger, "b", 2), nil))
	require.NoError(t, batch.SetBytes(AuditIndexUint64Key(kb, AuditFieldProposalID, 10, 1), nil))
	require.NoError(t, batch.SetBytes(AuditIndexUint64Key(kb, AuditFieldProposalID, 20, 5), nil))
	require.NoError(t, batch.Commit())

	// A sequence whose top byte is 0xFF must still be returned: the exclusive
	// upper bound has to be computed as a true prefix successor, not by
	// appending 0xFF to the value prefix (which would silently drop this key).
	seqs, err := s.AuditSeqsByString(AuditFieldLedger, "a")
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 4, highSeq}, seqs)

	seqs, err = s.AuditSeqsByUint64Range(AuditFieldProposalID, 10, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, seqs)

	require.NoError(t, s.DropAuditIndex())

	seqs, err = s.AuditSeqsByString(AuditFieldLedger, "a")
	require.NoError(t, err)
	require.Empty(t, seqs)
}
