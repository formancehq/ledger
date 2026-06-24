package state

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestApplyProposal_PerProposalIdempotency exercises the per-proposal
// idempotency the FSM applies in applyProposal: a duplicate proposal (same key,
// same ordered orders) replays the first outcome instead of re-executing, a
// reused key with different orders conflicts, and a frozen business failure is
// replayed. This is the behavior that used to live (per-order) in ProcessOrders
// and now lives at the proposal level.
func TestApplyProposal_PerProposalIdempotency(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	ctx := context.Background()

	const ledgerName = "idem"

	r, err := machine.ApplyEntries(ctx, dataStore, makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)

	withKey := func(id uint64, key string, orders ...*raftcmdpb.Order) *raftcmdpb.Proposal {
		p := makeProposal(id, orders...)
		p.Idempotency = &commonpb.Idempotency{Key: key}

		return p
	}

	fundAlice := func() *raftcmdpb.Order {
		return createTransactionOrder(ledgerName, true, newPosting("world", "alice", "EUR", 100))
	}

	// First apply under "k1": succeeds and commits a log.
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 2, withKey(2, "k1", fundAlice())))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)
	require.Len(t, r.Results[0].Logs, 1)
	firstSeq := r.Results[0].Logs[0].GetCreatedLog().GetSequence()
	require.NotZero(t, firstSeq)

	// Duplicate (same key + same orders): replays a REFERENCE to the original
	// log — no new log is created (no double-apply).
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 3, withKey(3, "k1", fundAlice())))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)
	require.Len(t, r.Results[0].Logs, 1)
	require.Nil(t, r.Results[0].Logs[0].GetCreatedLog(), "duplicate must not create a new log")
	require.Equal(t, firstSeq, r.Results[0].Logs[0].GetReferenceSequence(),
		"duplicate replays the original log sequence")

	// Same key, DIFFERENT orders: hash mismatch -> conflict.
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 4, withKey(4, "k1",
		createTransactionOrder(ledgerName, true, newPosting("world", "bob", "EUR", 5)))))
	require.NoError(t, err)
	var conflict *domain.ErrIdempotencyKeyConflict
	require.ErrorAs(t, r.Results[0].Error, &conflict, "reused key with different content conflicts")

	// A definitive business failure under "k2" (revert of a non-existent tx,
	// NotFound) is frozen...
	badRevert := func() *raftcmdpb.Order { return revertTransactionOrder(ledgerName, 9999) }

	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 5, withKey(5, "k2", badRevert())))
	require.NoError(t, err)
	require.Error(t, r.Results[0].Error)
	frozenMsg := r.Results[0].Error.Error()

	// ...so a duplicate replays the SAME error instead of re-executing.
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 6, withKey(6, "k2", badRevert())))
	require.NoError(t, err)
	require.Error(t, r.Results[0].Error)
	var replayed *domain.ReplayedFailure
	require.ErrorAs(t, r.Results[0].Error, &replayed, "frozen failure is replayed")
	require.Equal(t, frozenMsg, r.Results[0].Error.Error(), "replayed failure matches the original")
}

// TestApplyProposal_AuditEntryCarriesIdentity asserts the FSM records the batch
// idempotency key on the AuditEntry (the hash-chain-bound, batch-level home for
// identity), not on the committed logs, and that the AppliedProposal projection
// covers the produced log range.
func TestApplyProposal_AuditEntryCarriesIdentity(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	ctx := context.Background()

	const ledgerName = "idem-ap"

	r, err := machine.ApplyEntries(ctx, dataStore, makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)

	p := makeProposal(2, createTransactionOrder(ledgerName, true, newPosting("world", "alice", "EUR", 100)))
	p.Idempotency = &commonpb.Idempotency{Key: "batch-key"}

	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 2, p))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)
	require.Len(t, r.Results[0].Logs, 1)

	logSeq := r.Results[0].Logs[0].GetCreatedLog().GetSequence()

	aps := readAppliedProposals(t, ctx, dataStore)
	require.NotEmpty(t, aps)

	last := aps[len(aps)-1]
	require.GreaterOrEqual(t, logSeq, last.GetMinLogSequence())
	require.LessOrEqual(t, logSeq, last.GetMaxLogSequence())

	var keyed string
	for _, e := range listAuditEntries(t, dataStore, 0) {
		if e.GetSequence() == last.GetSequence() {
			keyed = e.GetIdempotency().GetKey()
		}
	}
	require.Equal(t, "batch-key", keyed, "batch key bound into the AuditEntry hash chain")
}

func readAppliedProposals(t *testing.T, ctx context.Context, store *dal.Store) []*proposalpb.AppliedProposal {
	t.Helper()

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	c, err := query.ReadAppliedProposals(ctx, handle, nil)
	require.NoError(t, err)

	defer func() { _ = c.Close() }()

	var out []*proposalpb.AppliedProposal

	for {
		ap, err := c.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		out = append(out, ap)
	}

	return out
}

// TestApplyProposal_ReplayDoesNotExtendAuditChain asserts that replaying a
// recorded outcome (success or frozen failure) returns it without appending an
// audit entry or advancing the audit hash chain — only a fresh apply does.
func TestApplyProposal_ReplayDoesNotExtendAuditChain(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	ctx := context.Background()

	const ledgerName = "idem-audit"

	r, err := machine.ApplyEntries(ctx, dataStore, makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)

	withKey := func(id uint64, key string, orders ...*raftcmdpb.Order) *raftcmdpb.Proposal {
		p := makeProposal(id, orders...)
		p.Idempotency = &commonpb.Idempotency{Key: key}

		return p
	}
	fundAlice := func() *raftcmdpb.Order {
		return createTransactionOrder(ledgerName, true, newPosting("world", "alice", "EUR", 100))
	}

	// Fresh keyed success commits a log and writes one audit entry.
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 2, withKey(2, "k1", fundAlice())))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)
	firstSeq := r.Results[0].Logs[0].GetCreatedLog().GetSequence()

	seqAfterCommit := machine.State.NextAuditSequenceID
	hashAfterCommit := append([]byte(nil), machine.State.LastAuditHash...)

	// Duplicate (same key + orders) replays the reference without auditing.
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 3, withKey(3, "k1", fundAlice())))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)
	require.Equal(t, firstSeq, r.Results[0].Logs[0].GetReferenceSequence(), "duplicate replays the original log")
	require.Equal(t, seqAfterCommit, machine.State.NextAuditSequenceID,
		"a success replay must not append an audit entry")
	require.Equal(t, hashAfterCommit, machine.State.LastAuditHash,
		"a success replay must not advance the audit hash chain")

	// Fresh frozen failure (revert of a non-existent tx) writes one audit entry.
	badRevert := func() *raftcmdpb.Order { return revertTransactionOrder(ledgerName, 9999) }

	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 4, withKey(4, "k2", badRevert())))
	require.NoError(t, err)
	require.Error(t, r.Results[0].Error)

	seqAfterFailure := machine.State.NextAuditSequenceID
	hashAfterFailure := append([]byte(nil), machine.State.LastAuditHash...)
	require.Greater(t, seqAfterFailure, seqAfterCommit, "a fresh failure must append an audit entry")

	// Duplicate replays the frozen failure without auditing.
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 5, withKey(5, "k2", badRevert())))
	require.NoError(t, err)
	var replayed *domain.ReplayedFailure
	require.ErrorAs(t, r.Results[0].Error, &replayed, "frozen failure is replayed")
	require.Equal(t, seqAfterFailure, machine.State.NextAuditSequenceID,
		"a failure replay must not append an audit entry")
	require.Equal(t, hashAfterFailure, machine.State.LastAuditHash,
		"a failure replay must not advance the audit hash chain")
}
