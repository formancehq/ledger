package incarnationmigration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestAccountSeenGrowsWithoutFalseNegatives(t *testing.T) {
	t.Parallel()
	seen := newAccountSeenWithCapacity(4)
	for i := range 1000 {
		seen.add([]byte(fmt.Sprintf("account:%d", i)))
	}
	for i := range 1000 {
		require.True(t, seen.mayContain([]byte(fmt.Sprintf("account:%d", i))))
	}
	require.Greater(t, len(seen.layers), 1)
}

func TestAccountSeenKeepsFalsePositivesBounded(t *testing.T) {
	t.Parallel()
	seen := newAccountSeenWithCapacity(1 << 17)
	for i := range 100_000 {
		seen.add([]byte(fmt.Sprintf("ledger/src:%d", i)))
	}
	falsePositives := 0
	for i := 100_000; i < 200_000; i++ {
		if seen.mayContain([]byte(fmt.Sprintf("ledger/src:%d", i))) {
			falsePositives++
		}
	}
	require.Less(t, falsePositives, 10)
}

func TestFoldKeepsFirstDateAcrossCommittedBatches(t *testing.T) {
	store := migrationStore(t)
	key := domain.AccountKey{LedgerName: "ledger", Account: "alice"}
	fold := &folder{current: store, attrs: attributes.New(), rows: make(map[domain.AccountKey]*commonpb.AccountIncarnation), deletes: make(map[string]struct{}), seen: newAccountSeen()}
	require.NoError(t, fold.touch(key, &commonpb.Timestamp{Data: 200}))
	require.NoError(t, fold.commit(store, Marker{}))
	require.NoError(t, fold.touch(key, &commonpb.Timestamp{Data: 300}))
	require.NoError(t, fold.commit(store, Marker{}))
	row, err := fold.attrs.AccountIncarnation.Get(store, key.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(200), row.GetInsertedAt().GetData())
	require.Equal(t, uint64(300), row.GetUpdatedAt().GetData())
}

func TestExistingIncarnationIsIncludedInMigrationFilter(t *testing.T) {
	store := migrationStore(t)
	key := domain.AccountKey{LedgerName: "ledger", Account: "alice"}
	attrs := attributes.New()
	batch := store.OpenWriteSession()
	_, err := attrs.AccountIncarnation.Set(batch, key.Bytes(), &commonpb.AccountIncarnation{InsertedAt: &commonpb.Timestamp{Data: 700}, UpdatedAt: &commonpb.Timestamp{Data: 800}})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	seen, err := readExistingIncarnations(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.True(t, seen.mayContain(key.Bytes()))
	fold := &folder{current: store, attrs: attrs, rows: make(map[domain.AccountKey]*commonpb.AccountIncarnation), deletes: make(map[string]struct{}), seen: seen}
	require.NoError(t, fold.touch(key, &commonpb.Timestamp{Data: 200}))
	require.NoError(t, fold.commit(store, Marker{}))
	row, err := attrs.AccountIncarnation.Get(store, key.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(200), row.GetInsertedAt().GetData())
	require.Equal(t, uint64(800), row.GetUpdatedAt().GetData())
}

func TestTransientDrainClosesStoredEphemeralIncarnation(t *testing.T) {
	store := migrationStore(t)
	attrs := attributes.New()
	fold := &folder{current: store, attrs: attrs, rows: make(map[domain.AccountKey]*commonpb.AccountIncarnation), deletes: make(map[string]struct{}), seen: newAccountSeen()}
	ephemeral := domain.AccountKey{LedgerName: "ledger", Account: "ephemeral"}
	normal := domain.AccountKey{LedgerName: "ledger", Account: "normal"}
	require.NoError(t, fold.touchWithVolumeDelta(ephemeral, &commonpb.Timestamp{Data: 100}, commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL, 1, true))
	require.NoError(t, fold.touchWithVolumeDelta(normal, &commonpb.Timestamp{Data: 100}, commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL, 1, true))
	require.NoError(t, fold.commit(store, Marker{}))

	// After reclassification as transient, the old incarnation is not touched,
	// but a purged retained volume must still close an ephemeral row.
	require.NoError(t, fold.adjustExistingVolumeCount(ephemeral, -1))
	require.NoError(t, fold.adjustExistingVolumeCount(normal, -1))
	require.NoError(t, fold.commit(store, Marker{}))
	closed, err := attrs.AccountIncarnation.Get(store, ephemeral.Bytes())
	require.NoError(t, err)
	require.Nil(t, closed)
	retained, err := attrs.AccountIncarnation.Get(store, normal.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(0), retained.GetVolumeCount())

	require.NoError(t, fold.touchWithVolumeDelta(ephemeral, &commonpb.Timestamp{Data: 300}, commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL, 1, true))
	require.NoError(t, fold.commit(store, Marker{}))
	reopened, err := attrs.AccountIncarnation.Get(store, ephemeral.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(300), reopened.GetInsertedAt().GetData())
	require.Equal(t, uint64(1), reopened.GetVolumeCount())
}
