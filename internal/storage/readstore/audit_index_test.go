package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestAuditIndexKeysAreInternalNamespaced(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()
	key := AuditIndexStringKey(kb, AuditFieldLedger, "ledger-a", 42)

	require.Equal(t, PrefixInternal, key[0])
	require.Equal(t, SubInternalAuditIndex, key[1])
	require.Equal(t, byte(AuditFieldLedger), key[2])
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
