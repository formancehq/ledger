package incarnationmigration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestAuditItemsReaderStreamsAcrossSequences(t *testing.T) {
	store := migrationStore(t)
	appendFixture(t, store, 1, 100, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger"}}})
	appendFixture(t, store, 2, 200, &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{Name: "ledger"}}})
	batch := store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAuditItem).PutUint64(2).PutUint32(1).Build()
	require.NoError(t, batch.SetProto(key, &auditpb.AuditItem{OrderIndex: 1, LogSequence: 2}))
	require.NoError(t, batch.Commit())

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	items, err := newAuditItemsReader(reader)
	require.NoError(t, err)
	defer func() { require.NoError(t, items.close()) }()
	first, err := items.read(t.Context(), 1)
	require.NoError(t, err)
	require.Len(t, first, 1)
	second, err := items.read(t.Context(), 2)
	require.NoError(t, err)
	require.Len(t, second, 2)
	require.Equal(t, uint32(1), second[1].GetOrderIndex())
	missing, err := items.read(t.Context(), 3)
	require.NoError(t, err)
	require.Empty(t, missing)
	_, err = items.read(t.Context(), 2)
	require.ErrorContains(t, err, "not advancing")
}
