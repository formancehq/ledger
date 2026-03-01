package state

import (
	"testing"

	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/stretchr/testify/require"
)

func TestAllOrdersAreMaintenanceMode(t *testing.T) {
	t.Parallel()

	t.Run("all maintenance mode", func(t *testing.T) {
		t.Parallel()
		orders := []*raftcmdpb.Order{
			{Type: &raftcmdpb.Order_SetMaintenanceMode{SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{Enabled: true}}},
			{Type: &raftcmdpb.Order_SetMaintenanceMode{SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{Enabled: false}}},
		}
		require.True(t, allOrdersAreMaintenanceMode(orders))
	})

	t.Run("mixed orders", func(t *testing.T) {
		t.Parallel()
		orders := []*raftcmdpb.Order{
			{Type: &raftcmdpb.Order_SetMaintenanceMode{SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{Enabled: true}}},
			{Type: &raftcmdpb.Order_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Name: "test"}}},
		}
		require.False(t, allOrdersAreMaintenanceMode(orders))
	})

	t.Run("no maintenance mode orders", func(t *testing.T) {
		t.Parallel()
		orders := []*raftcmdpb.Order{
			{Type: &raftcmdpb.Order_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Name: "a"}}},
		}
		require.False(t, allOrdersAreMaintenanceMode(orders))
	})

	t.Run("empty orders", func(t *testing.T) {
		t.Parallel()
		require.True(t, allOrdersAreMaintenanceMode(nil))
		require.True(t, allOrdersAreMaintenanceMode([]*raftcmdpb.Order{}))
	})
}

func TestSealRequestFromPeriod(t *testing.T) {
	t.Parallel()

	period := &commonpb.Period{
		Id:            5,
		CloseSequence: 42,
		LastLogHash:   []byte("hash-abc"),
	}

	req := SealRequestFromPeriod(period)
	require.Equal(t, uint64(5), req.PeriodID)
	require.Equal(t, uint64(42), req.CloseSequence)
	require.Equal(t, []byte("hash-abc"), req.LastLogHash)
}

func TestMachineAllPeriods(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// Initially empty
	require.Empty(t, machine.AllPeriods())

	// Add periods to machine
	machine.Periods.PutPeriod(&commonpb.Period{Id: 1, Status: commonpb.PeriodStatus_PERIOD_CLOSED})
	machine.Periods.PutPeriod(&commonpb.Period{Id: 2, Status: commonpb.PeriodStatus_PERIOD_OPEN})

	periods := machine.AllPeriods()
	require.Len(t, periods, 2)
}

func TestMachineClosingPeriod(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// Initially nil
	require.Nil(t, machine.ClosingPeriod())

	// Set closing period
	machine.Periods.SetClosingPeriod(&commonpb.Period{Id: 3, Status: commonpb.PeriodStatus_PERIOD_CLOSING})
	require.NotNil(t, machine.ClosingPeriod())
	require.Equal(t, uint64(3), machine.ClosingPeriod().Id)
}

func TestMachinePeriodSchedule(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// Initially empty
	require.Equal(t, "", machine.PeriodSchedule())

	// Set schedule
	machine.Periods.SetSchedule("*/5 * * * *")
	require.Equal(t, "*/5 * * * *", machine.PeriodSchedule())
}

func TestMachineScheduleChanged(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	sig := machine.ScheduleChanged()
	require.NotNil(t, sig)
}

func TestMachineSealRequestAndArchiveRequestChannels(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// SealRequestCh and ArchiveRequestCh should return the channels
	sealCh := machine.SealRequestCh()
	require.NotNil(t, sealCh)

	archiveCh := machine.ArchiveRequestCh()
	require.NotNil(t, archiveCh)

	convertCh := machine.MetadataConvertRequestCh()
	require.NotNil(t, convertCh)

	coldCh := machine.ColdCompactionCh()
	require.NotNil(t, coldCh)
}

func TestMachineLastPersistedIndex(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// Initially 0
	idx, err := machine.LastPersistedIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), idx)

	// Set via atomic
	machine.lastPersistedIndex.Store(42)
	idx, err = machine.LastPersistedIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(42), idx)
}

func TestMachineIsStoreUpToDate(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	machine, _, _ := newTestMachine(t)

	// With no snapshot and lastAppliedIndex=0, the store is up to date
	// (snapshotIndex == 0, lastAppliedIndex == 0)
	upToDate, err := machine.IsStoreUpToDate(ctx)
	require.NoError(t, err)
	require.True(t, upToDate)

	// If snapshot index is ahead of lastAppliedIndex, store is not up to date
	machine.snapshotIndex = 10
	upToDate, err = machine.IsStoreUpToDate(ctx)
	require.NoError(t, err)
	require.False(t, upToDate)

	// Catch up lastAppliedIndex
	machine.lastAppliedIndex = 10
	upToDate, err = machine.IsStoreUpToDate(ctx)
	require.NoError(t, err)
	require.True(t, upToDate)
}

func TestReadLastLog(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Empty store
	log, err := query.ReadLastLog(s)
	require.NoError(t, err)
	require.Nil(t, log)

	// Add logs
	registerLedger(t, s, "test-ledger")
	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 1, testLogs...)

	log, err = query.ReadLastLog(s)
	require.NoError(t, err)
	require.NotNil(t, log)
	require.Equal(t, uint64(4), log.Sequence)
}

func TestReadAuditEntriesCursor(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Empty store
	cursor, err := query.ReadAuditEntries(s, nil)
	require.NoError(t, err)
	_, curErr := cursor.Next()
	require.Error(t, curErr) // io.EOF
	_ = cursor.Close()

	// Add entries
	batch := s.NewBatch()
	require.NoError(t, AppendAuditEntries(batch,
		&auditpb.AuditEntry{Sequence: 1, ProposalId: 10, Timestamp: commonpb.NewTimestamp(libtime.Now())},
		&auditpb.AuditEntry{Sequence: 2, ProposalId: 20, Timestamp: commonpb.NewTimestamp(libtime.Now())},
		&auditpb.AuditEntry{Sequence: 3, ProposalId: 30, Timestamp: commonpb.NewTimestamp(libtime.Now())},
	))
	require.NoError(t, batch.Commit())

	// Read all
	cursor, err = query.ReadAuditEntries(s, nil)
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
	cursor, err = query.ReadAuditEntries(s, &afterSeq)
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
	require.Equal(t, uint64(2), entries[0].Sequence)
	require.Equal(t, uint64(3), entries[1].Sequence)
}

func TestCheckClosePeriod(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// nil result
	require.Nil(t, machine.checkClosePeriod(nil))

	// result without ClosePeriod log
	result := &ApplyResult{
		Logs: []*raftcmdpb.CreatedLogOrReference{
			{Type: &raftcmdpb.CreatedLogOrReference_CreatedLog{
				CreatedLog: &commonpb.Log{
					Sequence: 1,
					Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{LedgerName: "test"},
					}},
				},
			}},
		},
	}
	require.Nil(t, machine.checkClosePeriod(result))

	// result with ClosePeriod log
	closedPeriod := &commonpb.Period{
		Id:            5,
		CloseSequence: 42,
		LastLogHash:   []byte("hash"),
	}
	resultWithClose := &ApplyResult{
		Logs: []*raftcmdpb.CreatedLogOrReference{
			{Type: &raftcmdpb.CreatedLogOrReference_CreatedLog{
				CreatedLog: &commonpb.Log{
					Sequence: 10,
					Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_ClosePeriod{
						ClosePeriod: &commonpb.ClosePeriodLog{
							ClosedPeriod: closedPeriod,
						},
					}},
				},
			}},
		},
	}
	sealReq := machine.checkClosePeriod(resultWithClose)
	require.NotNil(t, sealReq)
	require.Equal(t, uint64(5), sealReq.PeriodID)
	require.Equal(t, uint64(42), sealReq.CloseSequence)
	require.Equal(t, []byte("hash"), sealReq.LastLogHash)
}
