package incarnationmigration

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func migrationStore(t *testing.T) *dal.Store {
	t.Helper()
	store, err := dal.NewStore(t.TempDir(), logging.Testing(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func appendFixture(t *testing.T, store *dal.Store, sequence, timestamp uint64, payload *commonpb.LogPayload) {
	t.Helper()
	batch := store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(sequence).Build()
	require.NoError(t, batch.SetProto(key, &commonpb.Log{Sequence: sequence, Payload: payload}))
	key = dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(sequence).Build()
	require.NoError(t, batch.SetProto(key, &auditpb.AuditEntry{
		Sequence: sequence, Timestamp: &commonpb.Timestamp{Data: timestamp}, OrderCount: 1,
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: sequence, MaxLogSequence: sequence}},
	}))
	key = dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAuditItem).PutUint64(sequence).PutUint32(0).Build()
	require.NoError(t, batch.SetProto(key, &auditpb.AuditItem{OrderIndex: 0, LogSequence: sequence}))
	require.NoError(t, batch.Commit())
}

func activateWrites(t *testing.T, store *dal.Store) {
	t.Helper()
	batch := store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy}, &commonpb.ClusterPolicy{
		Revision:              1,
		CapabilityActivations: []*commonpb.CapabilityActivation{{Id: uint32(capabilities.AccountIncarnationWritesV1), Active: true}},
	}))
	var index [8]byte
	binary.BigEndian.PutUint64(index[:], 7)
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneClusterPersistent, dal.SubGlobLastAppliedIndex}, index[:]))
	require.NoError(t, batch.Commit())
}

func TestRunBackfillsDatesAndMergesNewWrites(t *testing.T) {
	store := migrationStore(t)
	appendFixture(t, store, 1, 100, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger"}}})
	appendFixture(t, store, 2, 200, &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: "ledger", Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Postings: []*commonpb.Posting{{Source: "alice", Destination: "bob"}}}},
		}}},
	}}})
	appendFixture(t, store, 3, 300, &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: "ledger", Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_SavedMetadata{
			SavedMetadata: &commonpb.SavedMetadata{Target: &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: "alice"}}}, Metadata: map[string]*commonpb.MetadataValue{"a": commonpb.NewStringValue("b")}},
		}}},
	}}})
	attrs := attributes.New()
	key := domain.AccountKey{LedgerName: "ledger", Account: "alice"}
	batch := store.OpenWriteSession()
	_, err := attrs.AccountIncarnation.Set(batch, key.Bytes(), &commonpb.AccountIncarnation{
		Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL,
		InsertedAt:  &commonpb.Timestamp{Data: 700}, UpdatedAt: &commonpb.Timestamp{Data: 800},
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// The dormant binary must not backfill before the first activation.
	require.NoError(t, Run(t.Context(), store))
	marker, err := ReadMarker(store)
	require.NoError(t, err)
	require.False(t, marker.Complete)
	ids, err := AdvertisedIDs(store)
	require.NoError(t, err)
	require.Equal(t, []uint32{uint32(capabilities.AccountIncarnationWritesV1)}, ids)

	activateWrites(t, store)
	require.NoError(t, Run(t.Context(), store))
	marker, err = ReadMarker(store)
	require.NoError(t, err)
	require.True(t, marker.Complete)
	ids, err = AdvertisedIDs(store)
	require.NoError(t, err)
	require.Equal(t, []uint32{uint32(capabilities.AccountIncarnationWritesV1), uint32(capabilities.AccountIncarnationHistoryCompleteV1)}, ids)
	require.Equal(t, uint64(3), marker.Cursor)
	require.Equal(t, uint64(7), marker.AppliedIndex)
	row, err := attrs.AccountIncarnation.Get(store, key.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(200), row.GetInsertedAt().GetData())
	require.Equal(t, uint64(800), row.GetUpdatedAt().GetData())
	require.NoError(t, Run(t.Context(), store))
}

func TestRunRejectsAuditGap(t *testing.T) {
	store := migrationStore(t)
	appendFixture(t, store, 2, 200, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger"}}})
	activateWrites(t, store)
	require.ErrorContains(t, Run(t.Context(), store), "complete audit history")
	marker, err := ReadMarker(store)
	require.NoError(t, err)
	require.False(t, marker.Complete)
}

func TestVerifyComparesIncarnationsInBothDirections(t *testing.T) {
	store := migrationStore(t)
	appendFixture(t, store, 1, 100, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger"}}})
	appendFixture(t, store, 2, 200, &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: "ledger", Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Postings: []*commonpb.Posting{{Source: "alice", Destination: "bob"}}}},
		}}},
	}}})
	activateWrites(t, store)
	require.NoError(t, Run(t.Context(), store))
	policy := &commonpb.ClusterPolicy{Revision: 2, CapabilityActivations: []*commonpb.CapabilityActivation{
		{Id: uint32(capabilities.AccountIncarnationWritesV1), Active: true},
		{Id: uint32(capabilities.AccountIncarnationHistoryCompleteV1), Active: true},
	}}
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy}, policy))
	require.NoError(t, batch.Commit())
	check := func() error {
		reader, err := store.NewReadHandle()
		if err != nil {
			return err
		}
		defer func() { _ = reader.Close() }()
		return Verify(t.Context(), reader, logging.Testing())
	}
	require.NoError(t, check())
	batch = store.OpenWriteSession()
	_, err := attributes.New().AccountIncarnation.Set(batch, (domain.AccountKey{LedgerName: "ledger", Account: "orphan"}).Bytes(), &commonpb.AccountIncarnation{InsertedAt: &commonpb.Timestamp{Data: 200}, UpdatedAt: &commonpb.Timestamp{Data: 200}})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
	require.ErrorContains(t, check(), "differs from audited history")
}
