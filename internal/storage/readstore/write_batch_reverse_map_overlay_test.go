package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestWriteBatchReverseMapRangeOverlayHonorsOperationOrder(t *testing.T) {
	t.Parallel()

	const (
		ledger = "test"
		field  = "status"
		entity = "users:1"
	)

	store := newTestStore(t)
	kb := dal.NewKeyBuilder()
	reverseKey := AccountReverseMapKeyV(kb, ledger, entity, field, 1)
	otherKey := AccountReverseMapKeyV(kb, ledger, entity, "team", 1)
	encoded := EncodeMetadataValue(nil, commonpb.NewStringValue("open"))

	seed := store.NewBatch()
	require.NoError(t, seed.SetBytes(reverseKey, encoded))
	require.NoError(t, seed.Commit())

	wb := NewWriteBatch()
	session := store.NewBatch()
	wb.Init(session)
	t.Cleanup(func() {
		// Best-effort teardown; assertion failures remain the primary signal.
		_ = session.Cancel()
	})

	start := ReverseMapFieldPrefix(dal.NewKeyBuilder(), ledger, NamespaceAccount, field)
	require.NoError(t, wb.DeleteReverseMapRange(start, IncrementBytes(start)))

	got, ok := wb.ReverseMapOverlay(reverseKey)
	require.True(t, ok, "a committed row covered only by the range tombstone must not fall through to Pebble")
	require.Nil(t, got)
	_, ok = wb.ReverseMapOverlay(otherKey)
	require.False(t, ok, "an unrelated field must remain outside the tombstone")

	wb.SetEventSequence(2)
	require.NoError(t, wb.ReplaceMetadataIndexV(
		dal.NewKeyBuilder(), reverseKey, ledger, NamespaceAccount, field, 1,
		encoded, nil, []byte(entity),
	))
	got, ok = wb.ReverseMapOverlay(reverseKey)
	require.True(t, ok)
	require.Equal(t, encoded, got, "a later exact write must supersede the earlier range tombstone")

	start = ReverseMapFieldPrefix(dal.NewKeyBuilder(), ledger, NamespaceAccount, field)
	require.NoError(t, wb.DeleteReverseMapRange(start, IncrementBytes(start)))
	got, ok = wb.ReverseMapOverlay(reverseKey)
	require.True(t, ok)
	require.Nil(t, got, "a later range tombstone must supersede the exact overlay write")
}
