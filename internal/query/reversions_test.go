package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// seedReversions writes reverted transaction IDs into a ledger's reversion
// bitset using the exact FSM key layout (see state.saveReversionWord).
func seedReversions(t *testing.T, s *dal.Store, ledger string, ids ...uint64) {
	t.Helper()

	bs := &bitset.Bitset{}
	for _, id := range ids {
		bs.Set(id)
	}

	sess := s.OpenWriteSession()
	for wordIndex, word := range bs.Words() {
		if word == 0 {
			continue
		}

		sess.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLReversions).
			PutLedgerNameFixed(ledger).
			PutUint64(uint64(wordIndex))

		require.NoError(t, sess.SetBytes(sess.KeyBuilder.Consume(), bitset.MarshalWord(word)))
	}

	require.NoError(t, sess.Commit())
}

func TestReadReversionBitset(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	seedReversions(t, s, "ledgerA", 1, 5, 130)
	seedReversions(t, s, "ledgerB", 2) // must not leak into ledgerA's read

	handle, err := s.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	bsA, err := query.ReadReversionBitset(handle, "ledgerA")
	require.NoError(t, err)
	require.True(t, bsA.Test(1))
	require.True(t, bsA.Test(5))
	require.True(t, bsA.Test(130))
	require.False(t, bsA.Test(2), "ledgerB's reversion must not appear in ledgerA")
	require.False(t, bsA.Test(3))

	// A ledger with no reversions yields a non-nil empty bitset.
	bsEmpty, err := query.ReadReversionBitset(handle, "ledgerC")
	require.NoError(t, err)
	require.NotNil(t, bsEmpty)
	require.False(t, bsEmpty.Test(1))
}
