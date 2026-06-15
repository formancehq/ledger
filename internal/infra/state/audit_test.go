package state

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// listAuditEntries collects all audit entries from the store into a slice.
// Pass afterSequence=0 to return all entries.
func listAuditEntries(t *testing.T, store *dal.Store, afterSequence uint64) []*auditpb.AuditEntry {
	t.Helper()

	var filter *uint64
	if afterSequence > 0 {
		filter = &afterSequence
	}

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	cursor, err := query.ReadAuditEntries(context.Background(), handle, filter)
	require.NoError(t, err)

	defer func() { _ = cursor.Close() }()

	var entries []*auditpb.AuditEntry

	for {
		entry, err := cursor.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)

		entries = append(entries, entry)
	}

	return entries
}

func TestAuditLogOnSuccess(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	_ = dataStore
	ctx := context.Background()

	const ledgerName = "audit-success"

	// Create a ledger
	result, err := machine.ApplyEntries(ctx, dataStore,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error)

	// Create a successful transaction
	result, err = machine.ApplyEntries(ctx, dataStore,
		makeEntry(t, 2, makeProposal(2,
			createTransactionOrder(ledgerName, true,
				newPosting("world", "bank", "USD", 1000),
			),
		)),
	)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error)

	// Verify audit entries exist in the store
	entries := listAuditEntries(t, dataStore, 0)
	require.Len(t, entries, 2, "should have 2 audit entries (create ledger + transaction)")

	// First entry: create ledger (success) — sequences start at 1
	first := entries[0]
	require.Equal(t, uint64(1), first.GetSequence())
	require.Equal(t, uint64(1), first.GetProposalId())
	require.NotNil(t, first.GetSuccess(), "create ledger should be success")
	require.NotZero(t, first.GetSuccess().GetMinLogSequence())
	require.NotZero(t, first.GetOrderCount())

	// Second entry: create transaction (success)
	second := entries[1]
	require.Equal(t, uint64(2), second.GetSequence())
	require.Equal(t, uint64(2), second.GetProposalId())
	require.NotNil(t, second.GetSuccess(), "transaction should be success")
	require.NotZero(t, second.GetSuccess().GetMinLogSequence())
	require.NotZero(t, second.GetOrderCount())
	require.NotNil(t, second.GetTimestamp())
}

func TestAuditLogOnFailure(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	_ = dataStore
	ctx := context.Background()

	const ledgerName = "audit-failure"

	// Create a ledger
	result, err := machine.ApplyEntries(ctx, dataStore,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	// Try to create a transaction with insufficient funds (no force)
	result, err = machine.ApplyEntries(ctx, dataStore,
		makeEntry(t, 2, makeProposal(2,
			createTransactionOrder(ledgerName, false,
				newPosting("empty:account", "bank", "USD", 99999),
			),
		)),
	)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.Error(t, result.Results[0].Error, "should fail with insufficient funds")

	// Verify audit entries
	entries := listAuditEntries(t, dataStore, 0)
	require.Len(t, entries, 2, "should have 2 audit entries (create ledger + failed tx)")

	// Second entry: failed transaction — sequences start at 1
	failEntry := entries[1]
	require.Equal(t, uint64(2), failEntry.GetSequence())
	require.Equal(t, uint64(2), failEntry.GetProposalId())
	require.NotNil(t, failEntry.GetFailure(), "should be failure")
	// The error is INSUFFICIENT_FUNDS because nil Input is treated as zero balance
	require.Equal(t, domain.ErrReasonInsufficientFunds, failEntry.GetFailure().GetErrorType())
	require.NotEmpty(t, failEntry.GetFailure().GetMessage())
	require.Contains(t, failEntry.GetFailure().GetContext(), "account")
	require.Contains(t, failEntry.GetFailure().GetContext(), "asset")
}

func TestAuditLogSequenceMonotonic(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	_ = dataStore
	ctx := context.Background()

	const ledgerName = "audit-sequence"

	// Create a ledger + several transactions
	result, err := machine.ApplyEntries(ctx, dataStore,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	for i := uint64(2); i <= 5; i++ {
		result, err = machine.ApplyEntries(ctx, dataStore,
			makeEntry(t, i, makeProposal(i,
				createTransactionOrder(ledgerName, true,
					newPosting("world", "bank", "USD", 100),
				),
			)),
		)
		require.NoError(t, err)
		require.NoError(t, result.Results[0].Error)
	}

	// Verify sequences are monotonically increasing (starting at 1)
	entries := listAuditEntries(t, dataStore, 0)
	require.Len(t, entries, 5)

	for i, entry := range entries {
		require.Equal(t, uint64(i+1), entry.GetSequence(), "sequence should be %d", i+1)
	}
}

func TestAuditLogAfterSequenceFilter(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	_ = dataStore
	ctx := context.Background()

	const ledgerName = "audit-filter"

	// Create a ledger + 3 transactions (= 4 audit entries)
	result, err := machine.ApplyEntries(ctx, dataStore,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	for i := uint64(2); i <= 4; i++ {
		result, err = machine.ApplyEntries(ctx, dataStore,
			makeEntry(t, i, makeProposal(i,
				createTransactionOrder(ledgerName, true,
					newPosting("world", "bank", "USD", 100),
				),
			)),
		)
		require.NoError(t, err)
		require.NoError(t, result.Results[0].Error)
	}

	// All entries
	all := listAuditEntries(t, dataStore, 0)
	require.Len(t, all, 4)

	// After sequence 2: should return entries with sequence 3 and 4
	after2 := listAuditEntries(t, dataStore, 2)
	require.Len(t, after2, 2)
	require.Equal(t, uint64(3), after2[0].GetSequence())
	require.Equal(t, uint64(4), after2[1].GetSequence())

	// After sequence 4: should return nothing
	after4 := listAuditEntries(t, dataStore, 4)
	require.Empty(t, after4)
}

func TestAuditSequenceAdvances(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	_ = dataStore
	ctx := context.Background()

	const ledgerName = "audit-seq"

	// Create a ledger + transaction to increment audit sequence
	result, err := machine.ApplyEntries(ctx, dataStore,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	result, err = machine.ApplyEntries(ctx, dataStore,
		makeEntry(t, 2, makeProposal(2,
			createTransactionOrder(ledgerName, true,
				newPosting("world", "bank", "USD", 1000),
			),
		)),
	)
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	// Verify audit sequence is recoverable from Pebble.
	handle, err := dataStore.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	lastAuditSeq, err := query.ReadLastAuditSequence(handle)
	require.NoError(t, err)
	require.Equal(t, uint64(2), lastAuditSeq,
		"last audit sequence should be 2 (2 entries written)")
}

func TestBuildAuditFailure(t *testing.T) {
	t.Parallel()

	t.Run("InsufficientFunds", func(t *testing.T) {
		t.Parallel()

		err := &domain.ErrInsufficientFunds{
			Account: "user:alice",
			Asset:   "USD",
		}
		failure := buildAuditFailure(err)
		require.Equal(t, domain.ErrReasonInsufficientFunds, failure.GetErrorType())
		require.Equal(t, "user:alice", failure.GetContext()["account"])
		require.Equal(t, "USD", failure.GetContext()["asset"])
	})

	t.Run("LedgerNotFound", func(t *testing.T) {
		t.Parallel()

		err := &domain.ErrLedgerNotFound{Name: "missing-ledger"}
		failure := buildAuditFailure(err)
		require.Equal(t, domain.ErrReasonLedgerNotFound, failure.GetErrorType())
		require.Equal(t, "missing-ledger", failure.GetContext()["name"])
	})

	t.Run("LedgerAlreadyExists", func(t *testing.T) {
		t.Parallel()

		err := &domain.ErrLedgerAlreadyExists{Name: "existing-ledger"}
		failure := buildAuditFailure(err)
		require.Equal(t, domain.ErrReasonLedgerAlreadyExists, failure.GetErrorType())
		require.Equal(t, "existing-ledger", failure.GetContext()["name"])
	})

	t.Run("TransactionNotFound", func(t *testing.T) {
		t.Parallel()

		err := &domain.ErrTransactionNotFound{TransactionID: 42}
		failure := buildAuditFailure(err)
		require.Equal(t, domain.ErrReasonTransactionNotFound, failure.GetErrorType())
		require.Equal(t, "42", failure.GetContext()["transactionId"])
	})

	t.Run("Validation", func(t *testing.T) {
		t.Parallel()

		err := domain.ErrScriptRequired
		failure := buildAuditFailure(err)
		require.Equal(t, domain.ErrReasonValidation, failure.GetErrorType())
	})

	t.Run("LedgerInMirrorMode", func(t *testing.T) {
		t.Parallel()

		err := &domain.ErrLedgerInMirrorMode{Name: "mirror-ledger"}
		failure := buildAuditFailure(err)
		require.Equal(t, domain.ErrReasonLedgerInMirrorMode, failure.GetErrorType())
		require.Equal(t, "mirror-ledger", failure.GetContext()["name"])
	})

	t.Run("LedgerNotInMirrorMode", func(t *testing.T) {
		t.Parallel()

		err := &domain.ErrLedgerNotInMirrorMode{Name: "normal-ledger"}
		failure := buildAuditFailure(err)
		require.Equal(t, domain.ErrReasonLedgerNotInMirrorMode, failure.GetErrorType())
		require.Equal(t, "normal-ledger", failure.GetContext()["name"])
	})

	t.Run("MaintenanceMode", func(t *testing.T) {
		t.Parallel()

		err := domain.ErrMaintenanceMode
		failure := buildAuditFailure(err)
		require.Equal(t, domain.ErrReasonMaintenanceMode, failure.GetErrorType())
	})

	t.Run("InvalidCronExpression", func(t *testing.T) {
		t.Parallel()

		err := &domain.ErrInvalidCronExpression{Expression: "bad", Details: "parse error"}
		failure := buildAuditFailure(err)
		require.Equal(t, domain.ErrReasonInvalidCronExpression, failure.GetErrorType())
		require.Equal(t, "bad", failure.GetContext()["expression"])
		require.Equal(t, "parse error", failure.GetContext()["details"])
	})

	t.Run("SinkAlreadyExists", func(t *testing.T) {
		t.Parallel()

		err := &domain.ErrSinkAlreadyExists{Name: "my-sink"}
		failure := buildAuditFailure(err)
		require.Equal(t, domain.ErrReasonSinkAlreadyExists, failure.GetErrorType())
		require.Equal(t, "my-sink", failure.GetContext()["name"])
	})

	t.Run("SinkNotFound", func(t *testing.T) {
		t.Parallel()

		err := &domain.ErrSinkNotFound{Name: "missing-sink"}
		failure := buildAuditFailure(err)
		require.Equal(t, domain.ErrReasonSinkNotFound, failure.GetErrorType())
		require.Equal(t, "missing-sink", failure.GetContext()["name"])
	})

	t.Run("PeriodNotClosed", func(t *testing.T) {
		t.Parallel()

		err := &domain.ErrPeriodNotClosed{PeriodID: 3}
		failure := buildAuditFailure(err)
		require.Equal(t, domain.ErrReasonPeriodNotClosed, failure.GetErrorType())
		require.Equal(t, "3", failure.GetContext()["periodId"])
	})

	t.Run("Unknown", func(t *testing.T) {
		t.Parallel()

		err := errors.New("some unknown error")
		failure := buildAuditFailure(err)
		require.Equal(t, "UNKNOWN", failure.GetErrorType())
	})
}

func TestExtractLogSequenceRange(t *testing.T) {
	t.Parallel()

	logsOrRefs := []*raftcmdpb.CreatedLogOrReference{
		{Type: &raftcmdpb.CreatedLogOrReference_CreatedLog{CreatedLog: &commonpb.Log{Sequence: 1}}},
		{Type: &raftcmdpb.CreatedLogOrReference_ReferenceSequence{ReferenceSequence: 5}},
		{Type: &raftcmdpb.CreatedLogOrReference_CreatedLog{CreatedLog: &commonpb.Log{Sequence: 10}}},
	}
	minSeq, maxSeq := extractLogSequenceRange(logsOrRefs)
	require.Equal(t, uint64(1), minSeq)
	require.Equal(t, uint64(10), maxSeq)
}
