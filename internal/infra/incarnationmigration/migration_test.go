package incarnationmigration

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestMigrateBeforeRaftRequiresAppliedDrainAndResumes(t *testing.T) {
	store := migrationStore(t)
	appendFixture(t, store, 1, 100, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger"}}})
	activateWrites(t, store)
	require.ErrorContains(t, MigrateBeforeRaft(t.Context(), store), "requires an active cluster drain")

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneClusterTransient, dal.SubTransientDrainState}, &raftcmdpb.DrainState{Epoch: "upgrade", Index: 8, Members: []uint64{1}}))
	require.NoError(t, batch.Commit())
	require.ErrorContains(t, MigrateBeforeRaft(t.Context(), store), "barrier at index 8")

	batch = store.OpenWriteSession()
	var applied [8]byte
	binary.BigEndian.PutUint64(applied[:], 8)
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneClusterPersistent, dal.SubGlobLastAppliedIndex}, applied[:]))
	require.NoError(t, batch.Commit())
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, MigrateBeforeRaft(canceled, store), context.Canceled)
	marker, err := ReadMarker(store)
	require.NoError(t, err)
	require.False(t, marker.Complete)

	require.NoError(t, MigrateBeforeRaft(t.Context(), store))
	marker, err = ReadMarker(store)
	require.NoError(t, err)
	require.True(t, marker.Complete)
	require.Equal(t, uint64(1), marker.Cursor)
	require.NoError(t, MigrateBeforeRaft(t.Context(), store))
}

func TestMigrateBeforeRaftConvertsDormantHistoryUnderDrain(t *testing.T) {
	store := migrationStore(t)
	appendFixture(t, store, 1, 100, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger"}}})
	appendFixture(t, store, 2, 200, &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: "ledger", Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Postings: []*commonpb.Posting{{Source: "world", Destination: "alice"}}}},
		}}},
	}}})
	// The source RC has no incarnation capability. Its drain still makes the
	// historical store immutable while the target binary migrates at startup.
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneClusterTransient, dal.SubTransientDrainState}, &raftcmdpb.DrainState{Epoch: "upgrade", Index: 7, Members: []uint64{1}}))
	var applied [8]byte
	binary.BigEndian.PutUint64(applied[:], 7)
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneClusterPersistent, dal.SubGlobLastAppliedIndex}, applied[:]))
	require.NoError(t, batch.Commit())
	require.NoError(t, MigrateBeforeRaft(t.Context(), store))
	marker, err := ReadMarker(store)
	require.NoError(t, err)
	require.True(t, marker.Complete)
	require.Equal(t, uint64(2), marker.Cursor)
	row, err := attributes.New().AccountIncarnation.Get(store, (domain.AccountKey{LedgerName: "ledger", Account: "alice"}).Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(200), row.GetInsertedAt().GetData())
	require.NoError(t, MigrateBeforeRaft(t.Context(), store))
}

func TestMigrateBeforeRaftSealsEmptyStore(t *testing.T) {
	store := migrationStore(t)
	require.NoError(t, MigrateBeforeRaft(t.Context(), store))
	marker, err := ReadMarker(store)
	require.NoError(t, err)
	require.True(t, marker.Complete)
	require.Zero(t, marker.Cursor)
}

func TestMigrateBeforeRaftResumesCommittedCursorAfterCancellation(t *testing.T) {
	store := migrationStore(t)
	for sequence := uint64(1); sequence <= 257; sequence++ {
		appendFixture(t, store, sequence, sequence*100, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: fmt.Sprintf("ledger-%d", sequence)},
		}})
	}
	activateWrites(t, store)
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneClusterTransient, dal.SubTransientDrainState}, &raftcmdpb.DrainState{Epoch: "upgrade", Index: 7, Members: []uint64{1}}))
	require.NoError(t, batch.Commit())
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- RunWithPause(ctx, store, time.Hour) }()
	require.Eventually(t, func() bool {
		marker, err := ReadMarker(store)

		return err == nil && marker.Cursor == 256
	}, 10*time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.NoError(t, MigrateBeforeRaft(t.Context(), store))
	marker, err := ReadMarker(store)
	require.NoError(t, err)
	require.True(t, marker.Complete)
	require.Equal(t, uint64(257), marker.Cursor)
}

func migrationStore(t *testing.T) *dal.Store {
	t.Helper()
	store, err := dal.NewStore(t.TempDir(), logging.Testing(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	return store
}

func TestEmptyBootstrapLegacyWritesRequireMigration(t *testing.T) {
	store := migrationStore(t)
	marker, err := ReadMarker(store)
	require.NoError(t, err)
	require.False(t, marker.Complete)
	appendFixture(t, store, 1, 100, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger"}}})
	appendFixture(t, store, 2, 200, &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: "ledger", Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Postings: []*commonpb.Posting{{Source: "world", Destination: "alice"}}}},
		}}},
	}}})
	require.NoError(t, Run(t.Context(), store)) // Dormant migration must not seal history.
	marker, err = ReadMarker(store)
	require.NoError(t, err)
	require.False(t, marker.Complete)
	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	require.NoError(t, Verify(t.Context(), reader, logging.Testing()))
	require.NoError(t, reader.Close())

	activateWrites(t, store)
	require.ErrorContains(t, RequireReady(store), "migrate-incarnations")
	ids, err := AdvertisedIDs(store)
	require.NoError(t, err)
	require.Equal(t, []uint32{uint32(capabilities.AccountIncarnationWritesV1)}, ids)
	require.NoError(t, Run(t.Context(), store))
	require.NoError(t, RequireReady(store))
	marker, err = ReadMarker(store)
	require.NoError(t, err)
	require.True(t, marker.Complete)
	require.Equal(t, uint64(2), marker.Cursor)
	row, err := attributes.New().AccountIncarnation.Get(store, (domain.AccountKey{LedgerName: "ledger", Account: "alice"}).Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(200), row.GetInsertedAt().GetData())
	require.Equal(t, uint64(200), row.GetUpdatedAt().GetData())
	ids, err = AdvertisedIDs(store)
	require.NoError(t, err)
	require.Equal(t, []uint32{uint32(capabilities.AccountIncarnationWritesV1), uint32(capabilities.AccountIncarnationHistoryCompleteV1)}, ids)
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

func TestRunBackfillsDatesFromAuditInsteadOfExistingRows(t *testing.T) {
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
	require.Equal(t, uint64(300), row.GetUpdatedAt().GetData())
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
	preActivationReader, err := store.NewReadHandle()
	require.NoError(t, err)
	require.NoError(t, Verify(t.Context(), preActivationReader, logging.Testing()))
	require.NoError(t, preActivationReader.Close())
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
	_, err = attributes.New().AccountIncarnation.Set(batch, (domain.AccountKey{LedgerName: "ledger", Account: "orphan"}).Bytes(), &commonpb.AccountIncarnation{InsertedAt: &commonpb.Timestamp{Data: 200}, UpdatedAt: &commonpb.Timestamp{Data: 200}})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
	require.ErrorContains(t, check(), "differs from audited history")
}

func TestVerifySkipsUnreadableActivationState(t *testing.T) {
	for _, test := range []struct {
		name string
		key  []byte
	}{
		{name: "policy", key: []byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy}},
		{name: "capabilities", key: []byte{dal.ZoneGlobal, dal.SubGlobCapabilityState}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := migrationStore(t)
			batch := store.OpenWriteSession()
			require.NoError(t, putMarker(batch, Marker{Complete: true}))
			require.NoError(t, batch.SetBytes(test.key, []byte{0xff}))
			require.NoError(t, batch.Commit())
			reader, err := store.NewReadHandle()
			require.NoError(t, err)
			defer func() { require.NoError(t, reader.Close()) }()
			require.NoError(t, Verify(t.Context(), reader, logging.Testing()))
		})
	}
}

func TestIncomingCheckpointMigratesBeforeInstallation(t *testing.T) {
	store := migrationStore(t)
	appendFixture(t, store, 1, 100, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger"}}})
	appendFixture(t, store, 2, 200, &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: "ledger", Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Postings: []*commonpb.Posting{{Source: "world", Destination: "alice"}}}},
		}}},
	}}})
	activateWrites(t, store)
	checkpointDir := filepath.Join(t.TempDir(), "incoming")
	require.NoError(t, store.Checkpoint(checkpointDir))
	incoming, err := dal.OpenDirect(checkpointDir, logging.Testing())
	require.NoError(t, err)
	require.NoError(t, Run(t.Context(), incoming))
	marker, err := ReadMarker(incoming)
	require.NoError(t, err)
	require.True(t, marker.Complete)
	row, err := attributes.New().AccountIncarnation.Get(incoming, (domain.AccountKey{LedgerName: "ledger", Account: "alice"}).Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(200), row.GetInsertedAt().GetData())
	require.NoError(t, incoming.Close())
	sourceMarker, err := ReadMarker(store)
	require.NoError(t, err)
	require.False(t, sourceMarker.Complete)
}

func TestBackfillResetsAccountDatesAcrossLedgerDeletion(t *testing.T) {
	store := migrationStore(t)
	create := &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger"}}}
	transaction := &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: "ledger", Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Postings: []*commonpb.Posting{{Source: "world", Destination: "alice"}}}},
		}}},
	}}}
	appendFixture(t, store, 1, 100, create)
	appendFixture(t, store, 2, 200, transaction)
	appendFixture(t, store, 3, 300, &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{Name: "ledger"}}})
	appendFixture(t, store, 4, 400, create)
	appendFixture(t, store, 5, 500, transaction)
	activateWrites(t, store)
	require.NoError(t, Run(t.Context(), store))
	row, err := attributes.New().AccountIncarnation.Get(store, (domain.AccountKey{LedgerName: "ledger", Account: "alice"}).Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(500), row.GetInsertedAt().GetData())
	require.Equal(t, uint64(500), row.GetUpdatedAt().GetData())
	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	require.NoError(t, Verify(t.Context(), reader, logging.Testing()))
}

func TestBackfillAcceptsAccountTypeAddedToInitiallyUntypedLedger(t *testing.T) {
	store := migrationStore(t)
	appendFixture(t, store, 1, 100, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger"}}})
	appendFixture(t, store, 2, 200, &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: "ledger", Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_AddedAccountType{
			AddedAccountType: &commonpb.AddedAccountTypeLog{AccountType: &commonpb.AccountType{Name: "normal", Pattern: "alice", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL}},
		}}},
	}}})
	activateWrites(t, store)
	require.NoError(t, Run(t.Context(), store))
	marker, err := ReadMarker(store)
	require.NoError(t, err)
	require.True(t, marker.Complete)
}

func TestBackfillRecompilesAccountTypesWhenRemoved(t *testing.T) {
	store := migrationStore(t)
	appendFixture(t, store, 1, 100, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger"}}})
	apply := func(data *commonpb.LedgerLogPayload) *commonpb.LogPayload {
		return &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: "ledger", Log: &commonpb.LedgerLog{Data: data},
		}}}
	}
	appendFixture(t, store, 2, 200, apply(&commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_AddedAccountType{
		AddedAccountType: &commonpb.AddedAccountTypeLog{AccountType: &commonpb.AccountType{Name: "ephemeral", Pattern: "alice", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL}},
	}}))
	transaction := func() *commonpb.LedgerLog {
		return &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Postings: []*commonpb.Posting{{Source: "alice", Destination: "world"}}}},
		}}}
	}
	first := transaction()
	first.EphemeralVolumes = []*commonpb.TouchedVolume{{Account: "alice", Asset: "USD"}}
	appendFixture(t, store, 3, 300, &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{LedgerName: "ledger", Log: first}}})
	appendFixture(t, store, 4, 400, apply(&commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RemovedAccountType{
		RemovedAccountType: &commonpb.RemovedAccountTypeLog{Name: "ephemeral"},
	}}))
	second := transaction()
	second.NewKeptVolumes = []*commonpb.TouchedVolume{{Account: "alice", Asset: "USD"}}
	appendFixture(t, store, 5, 500, &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{LedgerName: "ledger", Log: second}}})
	activateWrites(t, store)
	require.NoError(t, Run(t.Context(), store))
	row, err := attributes.New().AccountIncarnation.Get(store, (domain.AccountKey{LedgerName: "ledger", Account: "alice"}).Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(500), row.GetInsertedAt().GetData())
	require.Equal(t, uint64(500), row.GetUpdatedAt().GetData())
}

func TestBackfillEphemeralIncarnationClosesOnlyAfterLastVolume(t *testing.T) {
	store := migrationStore(t)
	appendFixture(t, store, 1, 100, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger"}}})
	volumeLog := func(kept, purged []*commonpb.TouchedVolume) *commonpb.LogPayload {
		return &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: "ledger", Log: &commonpb.LedgerLog{
				Data:           &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Postings: []*commonpb.Posting{{Source: "alice", Destination: "world"}}}}}},
				NewKeptVolumes: kept, PurgedVolumes: purged,
			},
		}}}
	}
	usd := &commonpb.TouchedVolume{Account: "alice", Asset: "USD"}
	eur := &commonpb.TouchedVolume{Account: "alice", Asset: "EUR"}
	appendFixture(t, store, 2, 200, volumeLog([]*commonpb.TouchedVolume{usd, eur}, nil))
	appendFixture(t, store, 3, 300, &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: "ledger", Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_AddedAccountType{
			AddedAccountType: &commonpb.AddedAccountTypeLog{AccountType: &commonpb.AccountType{Name: "transient", Pattern: "alice", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT}},
		}}},
	}}})
	appendFixture(t, store, 4, 400, volumeLog(nil, []*commonpb.TouchedVolume{usd}))
	appendFixture(t, store, 5, 500, &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: "ledger", Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RemovedAccountType{
			RemovedAccountType: &commonpb.RemovedAccountTypeLog{Name: "transient"},
		}}},
	}}})
	appendFixture(t, store, 6, 600, &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: "ledger", Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_AddedAccountType{
			AddedAccountType: &commonpb.AddedAccountTypeLog{AccountType: &commonpb.AccountType{Name: "ephemeral", Pattern: "alice", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL}},
		}}},
	}}})
	for sequence := uint64(7); sequence <= 256; sequence++ {
		appendFixture(t, store, sequence, sequence*100, volumeLog(nil, nil))
	}
	// Close and reopen after the first committed migration cursor.
	appendFixture(t, store, 257, 25700, volumeLog(nil, []*commonpb.TouchedVolume{eur}))
	appendFixture(t, store, 258, 25800, volumeLog([]*commonpb.TouchedVolume{usd}, nil))
	activateWrites(t, store)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- RunWithPause(ctx, store, time.Hour) }()
	require.Eventually(t, func() bool {
		marker, err := ReadMarker(store)

		return err == nil && marker.Cursor == 256
	}, 10*time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.NoError(t, Run(t.Context(), store))
	key := (domain.AccountKey{LedgerName: "ledger", Account: "alice"}).Bytes()
	row, err := attributes.New().AccountIncarnation.Get(store, key)
	require.NoError(t, err)
	require.Equal(t, uint64(1), row.GetVolumeCount())
	require.Equal(t, uint64(25800), row.GetInsertedAt().GetData())
	require.Equal(t, uint64(25800), row.GetUpdatedAt().GetData())
}

func TestRequireReadyRejectsUnmigratedActiveStore(t *testing.T) {
	store := migrationStore(t)
	require.NoError(t, RequireReady(store))
	appendFixture(t, store, 1, 100, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger"}}})
	activateWrites(t, store)
	require.ErrorContains(t, RequireReady(store), "ledger-server migrate-incarnations")
	require.NoError(t, Run(t.Context(), store))
	require.NoError(t, RequireReady(store))
}
