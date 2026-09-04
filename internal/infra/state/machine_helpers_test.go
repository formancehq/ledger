package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestAllOrdersAreMaintenanceMode(t *testing.T) {
	t.Parallel()

	t.Run("all maintenance mode", func(t *testing.T) {
		t.Parallel()

		orders := []*raftcmdpb.Order{
			{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{
					Payload: &raftcmdpb.SystemScopedOrder_SetMaintenanceMode{
						SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{Enabled: true},
					},
				},
			}},
			{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{
					Payload: &raftcmdpb.SystemScopedOrder_SetMaintenanceMode{
						SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{Enabled: false},
					},
				},
			}},
		}
		require.True(t, authorizedInMaintenanceMode(orders))
	})

	t.Run("mixed orders", func(t *testing.T) {
		t.Parallel()

		orders := []*raftcmdpb.Order{
			{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{
					Payload: &raftcmdpb.SystemScopedOrder_SetMaintenanceMode{
						SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{Enabled: true},
					},
				},
			}},
			{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "test",
					Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
						CreateLedger: &raftcmdpb.CreateLedgerOrder{},
					},
				},
			}},
		}
		require.False(t, authorizedInMaintenanceMode(orders))
	})

	t.Run("no maintenance mode orders", func(t *testing.T) {
		t.Parallel()

		orders := []*raftcmdpb.Order{
			{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "a",
					Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
						CreateLedger: &raftcmdpb.CreateLedgerOrder{},
					},
				},
			}},
		}
		require.False(t, authorizedInMaintenanceMode(orders))
	})

	t.Run("empty orders", func(t *testing.T) {
		t.Parallel()
		require.True(t, authorizedInMaintenanceMode(nil))
		require.True(t, authorizedInMaintenanceMode([]*raftcmdpb.Order{}))
	})
}

func TestMachineLastPersistedIndex(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// Initially 0
	idx := machine.LastPersistedIndex()
	require.Equal(t, uint64(0), idx)

	// Set via atomic
	machine.lastPersistedIndex.Store(42)
	idx = machine.LastPersistedIndex()
	require.Equal(t, uint64(42), idx)
}

func TestMachineIsStoreUpToDate(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	machine, dataStore, _ := newTestMachine(t)

	recovery := NewRecovery(machine, dataStore)
	sync := NewSynchronizer(machine, recovery, dal.NewIncomingRestoreFactory(dataStore))

	// With no snapshot and lastAppliedIndex=0, the store is up to date
	// (snapshotIndex == 0, lastAppliedIndex == 0)
	upToDate, err := sync.IsStoreUpToDate(ctx)
	require.NoError(t, err)
	require.True(t, upToDate)

	// If snapshot index is ahead of lastAppliedIndex, store is not up to date
	machine.State.SnapshotIndex = 10
	upToDate, err = sync.IsStoreUpToDate(ctx)
	require.NoError(t, err)
	require.False(t, upToDate)

	// Catch up lastAppliedIndex
	machine.State.LastAppliedIndex = 10
	upToDate, err = sync.IsStoreUpToDate(ctx)
	require.NoError(t, err)
	require.True(t, upToDate)
}

func TestReadLastLog(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Empty store
	log, err := query.ReadLastLog(handle)
	require.NoError(t, err)
	require.Nil(t, log)

	// Add logs
	registerLedger(t, s, "test-ledger")

	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 1, testLogs...)

	log, err = query.ReadLastLog(handle)
	require.NoError(t, err)
	require.NotNil(t, log)
	require.Equal(t, uint64(4), log.GetSequence())
}

func TestReadAuditEntriesCursor(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Empty store
	cursor, err := query.ReadAuditEntries(context.Background(), handle, nil)
	require.NoError(t, err)

	_, curErr := cursor.Next()
	require.Error(t, curErr) // io.EOF

	_ = cursor.Close()

	// Add entries
	batch := s.OpenWriteSession()
	require.NoError(t, appendAuditEntries(batch,
		&auditpb.AuditEntry{Sequence: 1, ProposalId: 10, Timestamp: commonpb.NewTimestamp(libtime.Now())},
		&auditpb.AuditEntry{Sequence: 2, ProposalId: 20, Timestamp: commonpb.NewTimestamp(libtime.Now())},
		&auditpb.AuditEntry{Sequence: 3, ProposalId: 30, Timestamp: commonpb.NewTimestamp(libtime.Now())},
	))
	require.NoError(t, batch.Commit())

	// Read all
	cursor, err = query.ReadAuditEntries(context.Background(), handle, nil)
	require.NoError(t, err)

	var entries []*auditpb.AuditEntry

	for {
		entry, nextErr := cursor.Next()
		if nextErr != nil {
			break
		}

		entries = append(entries, entry)
	}

	_ = cursor.Close()

	require.Len(t, entries, 3)

	// Read after sequence 1
	afterSeq := uint64(1)
	cursor, err = query.ReadAuditEntries(context.Background(), handle, &afterSeq)
	require.NoError(t, err)

	entries = nil

	for {
		entry, nextErr := cursor.Next()
		if nextErr != nil {
			break
		}

		entries = append(entries, entry)
	}

	_ = cursor.Close()

	require.Len(t, entries, 2)
	require.Equal(t, uint64(2), entries[0].GetSequence())
	require.Equal(t, uint64(3), entries[1].GetSequence())
}
