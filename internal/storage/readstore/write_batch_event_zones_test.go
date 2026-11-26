package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestWriteBatchEventZonesTrackActualEventPuts(t *testing.T) {
	t.Parallel()

	const (
		ledger = "test"
		key    = "status"
		entity = "accounts:1"
	)

	newBatch := func(t *testing.T) (*WriteBatch, *dal.WriteSession) {
		t.Helper()

		store := newTestStore(t)
		session := store.NewBatch()
		wb := NewWriteBatch()
		wb.Init(session)
		wb.SetEventSequence(10)
		t.Cleanup(func() { _ = session.Cancel() })

		return wb, session
	}

	t.Run("fresh non-null value marks both zones", func(t *testing.T) {
		t.Parallel()

		wb, _ := newBatch(t)
		encoded := EncodeMetadataValue(nil, commonpb.NewStringValue("open"))
		reverseKey := AccountReverseMapKeyV(dal.NewKeyBuilder(), ledger, entity, key, 1)

		require.NoError(t, wb.ReplaceMetadataIndexV(
			dal.NewKeyBuilder(), reverseKey, ledger, NamespaceAccount, key, 1,
			encoded, nil, []byte(entity),
		))
		require.True(t, wb.EventZones().Has(PrefixMetadataIndex))
		require.True(t, wb.EventZones().Has(PrefixEntityExists))
	})

	t.Run("same null class marks metadata only", func(t *testing.T) {
		t.Parallel()

		wb, _ := newBatch(t)
		oldEncoded := EncodeMetadataValue(nil, commonpb.NewStringValue("open"))
		newEncoded := EncodeMetadataValue(nil, commonpb.NewStringValue("closed"))
		reverseKey := AccountReverseMapKeyV(dal.NewKeyBuilder(), ledger, entity, key, 1)

		require.NoError(t, wb.ReplaceMetadataIndexV(
			dal.NewKeyBuilder(), reverseKey, ledger, NamespaceAccount, key, 1,
			newEncoded, oldEncoded, []byte(entity),
		))
		require.True(t, wb.EventZones().Has(PrefixMetadataIndex))
		require.False(t, wb.EventZones().Has(PrefixEntityExists))
	})

	t.Run("equal replacement marks no zone", func(t *testing.T) {
		t.Parallel()

		wb, _ := newBatch(t)
		encoded := EncodeMetadataValue(nil, commonpb.NewStringValue("open"))
		reverseKey := AccountReverseMapKeyV(dal.NewKeyBuilder(), ledger, entity, key, 1)

		require.NoError(t, wb.ReplaceMetadataIndexV(
			dal.NewKeyBuilder(), reverseKey, ledger, NamespaceAccount, key, 1,
			encoded, encoded, []byte(entity),
		))
		require.False(t, wb.EventZones().Has(PrefixMetadataIndex))
		require.False(t, wb.EventZones().Has(PrefixEntityExists))
	})

	t.Run("reset and flush clear the snapshot", func(t *testing.T) {
		t.Parallel()

		wb, _ := newBatch(t)
		encoded := EncodeMetadataValue(nil, commonpb.NewStringValue("open"))
		reverseKey := AccountReverseMapKeyV(dal.NewKeyBuilder(), ledger, entity, key, 1)
		require.NoError(t, wb.ReplaceMetadataIndexV(
			dal.NewKeyBuilder(), reverseKey, ledger, NamespaceAccount, key, 1,
			encoded, nil, []byte(entity),
		))

		require.NoError(t, wb.Flush())
		require.False(t, wb.EventZones().Has(PrefixMetadataIndex))
		require.False(t, wb.EventZones().Has(PrefixEntityExists))

		store := newTestStore(t)
		resetSession := store.NewBatch()
		wb.Init(resetSession)
		wb.SetEventSequence(11)
		require.NoError(t, wb.ReplaceMetadataIndexV(
			dal.NewKeyBuilder(), reverseKey, ledger, NamespaceAccount, key, 1,
			encoded, nil, []byte(entity),
		))
		require.NoError(t, resetSession.Cancel())
		wb.Reset()
		require.False(t, wb.EventZones().Has(PrefixMetadataIndex))
		require.False(t, wb.EventZones().Has(PrefixEntityExists))
	})
}
