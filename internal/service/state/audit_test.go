package state

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/crypto/keystore"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/cache"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"
)

// newTestMachineWithAudit creates a Machine with configurable audit enablement.
func newTestMachineWithAudit(t *testing.T, auditEnabled bool) (*Machine, *data.Store, *attributes.Attributes) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dataStore, err := data.NewStore(t.TempDir(), logger, meter, data.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	attrs := attributes.New()

	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	machine, err := NewMachine(logger, dataStore, meter, c, attrs, 1000, keystore.NewKeyStore(), auditEnabled, NoopEventNotifier{}, 0)
	require.NoError(t, err)

	return machine, dataStore, attrs
}

// listAuditEntries collects all audit entries from the store into a slice.
// Pass afterSequence=0 to return all entries.
func listAuditEntries(t *testing.T, store *data.Store, afterSequence uint64) []*auditpb.AuditEntry {
	t.Helper()
	var filter *uint64
	if afterSequence > 0 {
		filter = &afterSequence
	}
	cursor, err := store.ListAuditEntries(filter)
	require.NoError(t, err)
	defer func() { _ = cursor.Close() }()

	var entries []*auditpb.AuditEntry
	for {
		entry, err := cursor.Next()
		if err == io.EOF {
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
	ctx := context.Background()

	const ledgerName = "audit-success"

	// Create a ledger
	result, err := machine.ApplyEntries(ctx,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error)

	// Create a successful transaction
	result, err = machine.ApplyEntries(ctx,
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

	// First entry: create ledger (success) — sequences start at 0
	first := entries[0]
	require.Equal(t, uint64(0), first.Sequence)
	require.Equal(t, uint64(1), first.ProposalId)
	require.NotNil(t, first.GetSuccess(), "create ledger should be success")
	require.NotEmpty(t, first.GetSuccess().LogSequences)
	require.NotEmpty(t, first.Orders)

	// Second entry: create transaction (success)
	second := entries[1]
	require.Equal(t, uint64(1), second.Sequence)
	require.Equal(t, uint64(2), second.ProposalId)
	require.NotNil(t, second.GetSuccess(), "transaction should be success")
	require.NotEmpty(t, second.GetSuccess().LogSequences)
	require.NotEmpty(t, second.Orders)
	require.NotNil(t, second.Timestamp)
}

func TestAuditLogOnFailure(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	ctx := context.Background()

	const ledgerName = "audit-failure"

	// Create a ledger
	result, err := machine.ApplyEntries(ctx,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	// Try to create a transaction with insufficient funds (no force)
	result, err = machine.ApplyEntries(ctx,
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

	// Second entry: failed transaction — sequences start at 0
	failEntry := entries[1]
	require.Equal(t, uint64(1), failEntry.Sequence)
	require.Equal(t, uint64(2), failEntry.ProposalId)
	require.NotNil(t, failEntry.GetFailure(), "should be failure")
	// The error is BALANCE_NOT_FOUND because the account doesn't exist yet (no balance preloaded)
	require.Equal(t, processing.ErrReasonBalanceNotFound, failEntry.GetFailure().ErrorType)
	require.NotEmpty(t, failEntry.GetFailure().Message)
	require.Contains(t, failEntry.GetFailure().Context, "account")
	require.Contains(t, failEntry.GetFailure().Context, "asset")
}

func TestAuditLogSequenceMonotonic(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	ctx := context.Background()

	const ledgerName = "audit-sequence"

	// Create a ledger + several transactions
	result, err := machine.ApplyEntries(ctx,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	for i := uint64(2); i <= 5; i++ {
		result, err = machine.ApplyEntries(ctx,
			makeEntry(t, i, makeProposal(i,
				createTransactionOrder(ledgerName, true,
					newPosting("world", "bank", "USD", 100),
				),
			)),
		)
		require.NoError(t, err)
		require.NoError(t, result.Results[0].Error)
	}

	// Verify sequences are monotonically increasing (starting at 0)
	entries := listAuditEntries(t, dataStore, 0)
	require.Len(t, entries, 5)
	for i, entry := range entries {
		require.Equal(t, uint64(i), entry.Sequence, "sequence should be %d", i)
	}
}

func TestAuditLogAfterSequenceFilter(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	ctx := context.Background()

	const ledgerName = "audit-filter"

	// Create a ledger + 3 transactions (= 4 audit entries)
	result, err := machine.ApplyEntries(ctx,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	for i := uint64(2); i <= 4; i++ {
		result, err = machine.ApplyEntries(ctx,
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

	// After sequence 1: should return entries with sequence 2 and 3
	after1 := listAuditEntries(t, dataStore, 1)
	require.Len(t, after1, 2)
	require.Equal(t, uint64(2), after1[0].Sequence)
	require.Equal(t, uint64(3), after1[1].Sequence)

	// After sequence 3: should return nothing
	after3 := listAuditEntries(t, dataStore, 3)
	require.Empty(t, after3)
}

func TestAuditLogDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Create a machine with audit disabled
	machine, dataStore, _ := newTestMachineWithAudit(t, false)

	const ledgerName = "audit-disabled"

	// Create a ledger + transaction
	result, err := machine.ApplyEntries(ctx,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	result, err = machine.ApplyEntries(ctx,
		makeEntry(t, 2, makeProposal(2,
			createTransactionOrder(ledgerName, true,
				newPosting("world", "bank", "USD", 1000),
			),
		)),
	)
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	// Verify no audit entries were written
	entries := listAuditEntries(t, dataStore, 0)
	require.Empty(t, entries, "no audit entries should exist when audit is disabled")
}

func TestAuditLogInSnapshot(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)
	ctx := context.Background()

	const ledgerName = "audit-snapshot"

	// Create a ledger + transaction to increment audit sequence
	result, err := machine.ApplyEntries(ctx,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	result, err = machine.ApplyEntries(ctx,
		makeEntry(t, 2, makeProposal(2,
			createTransactionOrder(ledgerName, true,
				newPosting("world", "bank", "USD", 1000),
			),
		)),
	)
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	// Create snapshot and verify audit sequence is captured
	snapshotBytes, err := machine.CreateSnapshot(ctx)
	require.NoError(t, err)

	var snapshot raftcmdpb.MemorySnapshot
	require.NoError(t, proto.Unmarshal(snapshotBytes, &snapshot))
	require.Equal(t, uint64(2), snapshot.NextAuditSequenceId,
		"snapshot should capture next audit sequence ID (2 entries written starting at 0, next = 2)")
}

func TestBuildAuditFailure(t *testing.T) {
	t.Parallel()

	t.Run("InsufficientFunds", func(t *testing.T) {
		t.Parallel()
		err := &processing.ErrInsufficientFunds{
			Account: "user:alice",
			Asset:   "USD",
		}
		failure := buildAuditFailure(err)
		require.Equal(t, processing.ErrReasonInsufficientFunds, failure.ErrorType)
		require.Equal(t, "user:alice", failure.Context["account"])
		require.Equal(t, "USD", failure.Context["asset"])
	})

	t.Run("LedgerNotFound", func(t *testing.T) {
		t.Parallel()
		err := &processing.ErrLedgerNotFound{Name: "missing-ledger"}
		failure := buildAuditFailure(err)
		require.Equal(t, processing.ErrReasonLedgerNotFound, failure.ErrorType)
		require.Equal(t, "missing-ledger", failure.Context["name"])
	})

	t.Run("LedgerAlreadyExists", func(t *testing.T) {
		t.Parallel()
		err := &processing.ErrLedgerAlreadyExists{Name: "existing-ledger"}
		failure := buildAuditFailure(err)
		require.Equal(t, processing.ErrReasonLedgerAlreadyExists, failure.ErrorType)
		require.Equal(t, "existing-ledger", failure.Context["name"])
	})

	t.Run("TransactionNotFound", func(t *testing.T) {
		t.Parallel()
		err := &processing.ErrTransactionNotFound{TransactionID: 42}
		failure := buildAuditFailure(err)
		require.Equal(t, processing.ErrReasonTransactionNotFound, failure.ErrorType)
		require.Equal(t, "42", failure.Context["transactionId"])
	})

	t.Run("Validation", func(t *testing.T) {
		t.Parallel()
		err := processing.ErrScriptRequired
		failure := buildAuditFailure(err)
		require.Equal(t, processing.ErrReasonValidation, failure.ErrorType)
	})

	t.Run("Unknown", func(t *testing.T) {
		t.Parallel()
		err := fmt.Errorf("some unknown error")
		failure := buildAuditFailure(err)
		require.Equal(t, "UNKNOWN", failure.ErrorType)
	})
}

func TestExtractLogSequences(t *testing.T) {
	t.Parallel()

	logs := []*commonpb.Log{
		{Sequence: 1},
		{Sequence: 5},
		{Sequence: 10},
	}
	sequences := extractLogSequences(logs)
	require.Equal(t, []uint64{1, 5, 10}, sequences)
}
