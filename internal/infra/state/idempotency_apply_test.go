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

// TestApplyProposal_FreezesExpiryFromClusterPolicy asserts the FSM stamps each
// idempotency outcome's absolute expires_at from the committed cluster-policy
// TTL at apply time — on both the stored projection and the chain-bound audit
// entry — never re-freezes a replayed outcome, and reads the current committed
// policy for a fresh outcome. This is the EN-1797 determinism boundary: the
// expiry is frozen from committed state, so no node-local TTL read can diverge a
// committed outcome's lifetime.
func TestApplyProposal_FreezesExpiryFromClusterPolicy(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	ctx := context.Background()

	const (
		ledgerName = "idem-ttl"
		ttlMicros  = uint64(60_000_000)
	)

	loadKey := func(key string) *commonpb.IdempotencyKeyValue {
		handle, err := dataStore.NewDirectReadHandle()
		require.NoError(t, err)

		defer func() { _ = handle.Close() }()

		v, err := LoadIdempotencyKey(handle, key)
		require.NoError(t, err)

		return v
	}

	machine.State.ClusterPolicy = &commonpb.ClusterPolicy{Revision: 1, IdempotencyTtlMicros: ttlMicros}

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

	// Keyed success: expires_at = created_at + policy TTL, and the audit entry
	// chain-binds the same value.
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 2, withKey(2, "k1", fundAlice())))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)

	stored := loadKey("k1")
	require.NotNil(t, stored)
	require.NotZero(t, stored.GetCreatedAt())
	require.Equal(t, stored.GetCreatedAt()+ttlMicros, stored.GetExpiresAt(),
		"the success outcome's expiry must be created_at + the committed policy TTL")

	var auditExpiry uint64
	for _, e := range listAuditEntries(t, dataStore, 0) {
		if e.GetIdempotency().GetKey() == "k1" {
			auditExpiry = e.GetIdempotency().GetExpiresAt()
		}
	}
	require.Equal(t, stored.GetExpiresAt(), auditExpiry,
		"the audit entry must chain-bind the same expiry the projection stored")

	// Keyed failure freezes under the same policy-derived expiry.
	badRevert := func() *raftcmdpb.Order { return revertTransactionOrder(ledgerName, 9999) }
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 3, withKey(3, "k2", badRevert())))
	require.NoError(t, err)
	require.Error(t, r.Results[0].Error)

	failStored := loadKey("k2")
	require.NotNil(t, failStored.GetFailure())
	require.Equal(t, failStored.GetCreatedAt()+ttlMicros, failStored.GetExpiresAt(),
		"a frozen failure expires under the same policy TTL as a success")

	// Raising the TTL does not retroactively change an already-frozen outcome: a
	// duplicate replays the stored value, keeping its original expiry.
	machine.State.ClusterPolicy = &commonpb.ClusterPolicy{Revision: 2, IdempotencyTtlMicros: 5 * ttlMicros}

	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 4, withKey(4, "k1", fundAlice())))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)
	require.Equal(t, stored.GetExpiresAt(), loadKey("k1").GetExpiresAt(),
		"a replay must not re-freeze the expiry under a newer policy")

	// A fresh key freezes under the currently committed (raised) policy TTL.
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 5, withKey(5, "k3",
		createTransactionOrder(ledgerName, true, newPosting("world", "carol", "EUR", 7)))))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)

	freshStored := loadKey("k3")
	require.Equal(t, freshStored.GetCreatedAt()+5*ttlMicros, freshStored.GetExpiresAt(),
		"a fresh outcome freezes under the currently committed policy TTL")
}

// TestApplyProposal_ZeroTTLNeverExpires asserts an idempotency-ttl of 0 in the
// committed policy freezes a zero expires_at (never expires) and writes no
// eviction time-index entry, so the leader eviction scan never reaches it.
func TestApplyProposal_ZeroTTLNeverExpires(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	ctx := context.Background()

	const ledgerName = "idem-ttl0"

	machine.State.ClusterPolicy = &commonpb.ClusterPolicy{Revision: 1, IdempotencyTtlMicros: 0}

	r, err := machine.ApplyEntries(ctx, dataStore, makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)

	p := makeProposal(2, createTransactionOrder(ledgerName, true, newPosting("world", "alice", "EUR", 100)))
	p.Idempotency = &commonpb.Idempotency{Key: "k1"}

	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 2, p))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)

	handle, err := dataStore.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	stored, err := LoadIdempotencyKey(handle, "k1")
	require.NoError(t, err)
	require.Equal(t, uint64(0), stored.GetExpiresAt(), "a zero policy TTL freezes a never-expiring outcome")

	hashes, _, err := machine.Registry.Idempotency.ScanExpiredKeyHashes(handle, ^uint64(0), 100)
	require.NoError(t, err)
	require.Empty(t, hashes, "a never-expiring outcome must not appear in the eviction time index")
}

// TestPreload_DoesNotResurrectEvictedOutcome is the regression for the
// stale-preload divergence: a proposal whose ExecutionPlan was built before an
// idempotency eviction still carries the evicted outcome, and Preload must not
// re-inject it. The guard reads the replicated eviction cutoff (advanced by
// applyIdempotencyEviction), NOT the HLC — which an eviction never advances — so
// the map stays in lockstep with Pebble and no second eviction double-deletes.
func TestPreload_DoesNotResurrectEvictedOutcome(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)

	const (
		key       = "evicted-key"
		expiresAt = uint64(60_000_000)
	)

	// Freeze an outcome with a finite expiry into both the map and Pebble
	// (main key + time index), exactly as the FSM success/failure path does.
	value := &commonpb.IdempotencyKeyValue{FirstLogSequence: 7, LogCount: 1, Hash: []byte("h"), CreatedAt: 1, ExpiresAt: expiresAt}

	freezeBatch := dataStore.OpenWriteSession()
	require.NoError(t, SaveIdempotencyKey(freezeBatch, key, value))
	require.NoError(t, freezeBatch.Commit())
	machine.Registry.Idempotency.Put(key, value)

	// Leader pre-scan for the eviction (cutoff == expires_at, so the outcome is
	// in-window), then apply the eviction through the real FSM handler.
	handle, err := dataStore.NewReadHandle()
	require.NoError(t, err)

	hashes, lastKey, err := machine.Registry.Idempotency.ScanExpiredKeyHashes(handle, expiresAt, 100)
	require.NoError(t, err)
	_ = handle.Close()
	require.Len(t, hashes, 1)

	eviction := &raftcmdpb.IdempotencyEviction{CutoffMicros: expiresAt, PebbleKeyHashes: hashes, LastScannedTimeIndexKey: lastKey}

	evictBatch := dataStore.OpenWriteSession()
	require.NoError(t, machine.applyIdempotencyEviction(evictBatch, eviction))
	require.NoError(t, evictBatch.Commit())

	// Eviction removed it from the map and Pebble and advanced the persisted cutoff.
	_, inMap := machine.Registry.Idempotency.Get(key)
	require.False(t, inMap, "eviction removes the key from the map")

	post, err := dataStore.NewDirectReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = post.Close() })

	gone, err := LoadIdempotencyKey(post, key)
	require.NoError(t, err)
	require.Nil(t, gone, "eviction removes the Pebble main key")

	require.Equal(t, expiresAt, machine.State.LastIdempotencyEvictionCutoff, "in-memory eviction cutoff advanced")

	persistedCutoff, err := query.ReadLastIdempotencyEvictionCutoff(post)
	require.NoError(t, err)
	require.Equal(t, expiresAt, persistedCutoff, "the eviction cutoff is persisted so replay/restore is deterministic")

	// A proposal whose plan predates the eviction still carries the value.
	// Preload must skip it: expires_at <= the eviction cutoff means evicted.
	stalePlan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: machine.Registry.Cache.BaseIndex.Gen0,
		IdempotencyKeys:    []*raftcmdpb.ReloadIdempotencyKey{{Key: key, Value: value}},
	}

	preloadBatch := dataStore.OpenWriteSession()
	defer func() { _ = preloadBatch.Cancel() }()
	require.NoError(t, machine.Preload(stalePlan, preloadBatch, 0))

	_, inMap = machine.Registry.Idempotency.Get(key)
	require.False(t, inMap, "an evicted outcome must not be resurrected into the map by a stale preload")

	// The stale preload left the map free of the key, so a second eviction naming
	// the same hash finds nothing in the map and emits no second SingleDelete.
	secondBatch := dataStore.OpenWriteSession()
	require.NoError(t, machine.applyIdempotencyEviction(secondBatch, eviction))
	require.NoError(t, secondBatch.Commit())

	// A live outcome (expires_at above the cutoff) is still re-injected.
	liveKey := "live-key"
	liveValue := &commonpb.IdempotencyKeyValue{FirstLogSequence: 9, LogCount: 1, Hash: []byte("h2"), CreatedAt: 1, ExpiresAt: expiresAt * 2}
	livePlan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: machine.Registry.Cache.BaseIndex.Gen0,
		IdempotencyKeys:    []*raftcmdpb.ReloadIdempotencyKey{{Key: liveKey, Value: liveValue}},
	}

	require.NoError(t, machine.Preload(livePlan, preloadBatch, 0))
	_, inMap = machine.Registry.Idempotency.Get(liveKey)
	require.True(t, inMap, "an outcome above the eviction cutoff is still re-injected")
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

// TestPreload_DoesNotOverwriteNewerOutcomeWithStalePlan is a unit-level check of
// the freshness guard around Preload: the eviction-cutoff gate proves a
// plan-carried value was not evicted, but not that it is still the newest value
// for the key, so a stale plan value must not overwrite a newer one in the map.
// It does not exercise the cross-component race, the idempotency gate, or
// recovery — that is TestApplyProposal_StalePreloadCannotResurrectSupersededOutcome.
func TestPreload_DoesNotOverwriteNewerOutcomeWithStalePlan(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)

	const key = "superseded-key"

	// A newer live outcome is already in the map (installed by an earlier apply).
	live := &commonpb.IdempotencyKeyValue{FirstLogSequence: 10, LogCount: 1, CreatedAt: 2_000_000}
	machine.Registry.Idempotency.Put(key, live)

	// A concurrent proposal's plan still carries the older, superseded value.
	stale := &commonpb.IdempotencyKeyValue{FirstLogSequence: 1, LogCount: 1, CreatedAt: 1_000_000}
	plan := &raftcmdpb.ExecutionPlan{
		IdempotencyKeys: []*raftcmdpb.ReloadIdempotencyKey{{Key: key, Value: stale}},
	}

	batch := dataStore.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()
	require.NoError(t, machine.Preload(plan, batch, 0))

	got, ok := machine.Registry.Idempotency.Get(key)
	require.True(t, ok)
	require.EqualValues(t, 2_000_000, got.GetCreatedAt(),
		"a stale plan value must not overwrite the newer live outcome")
	require.EqualValues(t, 10, got.GetFirstLogSequence())

	// Positive control: with no value in the map, the plan value still installs —
	// the guard blocks only OLDER values, not the bridge itself.
	const freshKey = "fresh-key"
	require.NoError(t, machine.Preload(&raftcmdpb.ExecutionPlan{
		IdempotencyKeys: []*raftcmdpb.ReloadIdempotencyKey{{Key: freshKey, Value: stale}},
	}, batch, 0))
	installed, ok := machine.Registry.Idempotency.Get(freshKey)
	require.True(t, ok, "the plan value must still install when the map has no value")
	require.EqualValues(t, 1_000_000, installed.GetCreatedAt())
}

// TestEviction_ReusedKeyDeletesCleanly is a smoke-level check that a reused
// (twice-Set) key is removed by eviction and its time-index swept. The full
// storage lifecycle — the two-index-row precondition and absence through the
// compaction and close/reopen boundaries a SingleDelete could resurrect at — is
// TestEviction_ReusedKeyLifecycleThroughCompaction.
func TestEviction_ReusedKeyDeletesCleanly(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)

	const (
		key  = "reused-key"
		expA = uint64(10_000_000)
		expB = uint64(20_000_000)
	)

	// Freeze the first outcome A, then flush so its Set lands in an SST.
	a := &commonpb.IdempotencyKeyValue{FirstLogSequence: 1, LogCount: 1, CreatedAt: 1, ExpiresAt: expA}
	b1 := dataStore.OpenWriteSession()
	require.NoError(t, SaveIdempotencyKey(b1, key, a))
	require.NoError(t, b1.Commit())
	machine.Registry.Idempotency.Put(key, a)
	require.NoError(t, dataStore.Flush())

	// A expires; a fresh proposal reuses the key and freezes B — a SECOND Set on
	// the same main key. Flush again so the two Sets sit in separate SSTs, the
	// shape under which a SingleDelete could resurrect A.
	b := &commonpb.IdempotencyKeyValue{FirstLogSequence: 2, LogCount: 1, CreatedAt: 2, ExpiresAt: expB}
	b2 := dataStore.OpenWriteSession()
	require.NoError(t, SaveIdempotencyKey(b2, key, b))
	require.NoError(t, b2.Commit())
	machine.Registry.Idempotency.Put(key, b)
	require.NoError(t, dataStore.Flush())

	// Evict B through the real handler.
	handle, err := dataStore.NewReadHandle()
	require.NoError(t, err)
	hashes, lastKey, err := machine.Registry.Idempotency.ScanExpiredKeyHashes(handle, expB, 100)
	require.NoError(t, err)
	_ = handle.Close()
	require.NotEmpty(t, hashes)

	evictBatch := dataStore.OpenWriteSession()
	require.NoError(t, machine.applyIdempotencyEviction(evictBatch,
		&raftcmdpb.IdempotencyEviction{CutoffMicros: expB, PebbleKeyHashes: hashes, LastScannedTimeIndexKey: lastKey}))
	require.NoError(t, evictBatch.Commit())
	require.NoError(t, dataStore.Flush())

	// The twice-Set key is gone and does not resurrect.
	post, err := dataStore.NewDirectReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = post.Close() })

	gone, err := LoadIdempotencyKey(post, key)
	require.NoError(t, err)
	require.Nil(t, gone, "a reused (twice-Set) key must be cleanly removed by eviction")

	remaining, _, err := machine.Registry.Idempotency.ScanExpiredKeyHashes(post, expB, 100)
	require.NoError(t, err)
	require.Empty(t, remaining, "both time-index rows for the reused key must be swept")
}

// TestApplyProposal_StalePreloadCannotResurrectSupersededOutcome is the FSM-level
// regression for the preload freshness guard. It drives the full at-most-once and
// conflict contracts through ApplyEntries (not a direct Preload call): two
// proposals are planned against the same expired-but-unevicted outcome V0; A
// supersedes it with a fresh live outcome V1, then B applies carrying its stale
// plan. Without the freshness guard B's preload would reinstall V0 over V1 and the
// gate would re-execute; with it, B replays A (identical content) or conflicts
// (different content), and the surviving outcome is V1 across a restart.
//
// Raft indices are sequential (the FSM rejects gaps); the proposal Date is set
// independently to drive HLC expiry timing (V0 expires between A0 and A; V1 stays
// live across every B).
func TestApplyProposal_StalePreloadCannotResurrectSupersededOutcome(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	ctx := context.Background()

	const (
		ledgerName = "idem-stale"
		key        = "reused-key"
		acct       = "acc"
		ttlMicros  = uint64(5) // tiny TTL so V0 expires between A0 and A
	)

	machine.State.ClusterPolicy = &commonpb.ClusterPolicy{Revision: 1, IdempotencyTtlMicros: ttlMicros}

	keyed := func(date uint64, stale *commonpb.IdempotencyKeyValue, orders ...*raftcmdpb.Order) *raftcmdpb.Proposal {
		p := makeProposal(1, orders...)
		p.Date = &commonpb.Timestamp{Data: date}
		p.Idempotency = &commonpb.Idempotency{Key: key}
		if stale != nil {
			p.GetExecutionPlan().IdempotencyKeys = []*raftcmdpb.ReloadIdempotencyKey{{Key: key, Value: stale}}
		}

		return p
	}
	fund := func(amount int64) *raftcmdpb.Order {
		return createTransactionOrder(ledgerName, true, newPosting("world", acct, "EUR", amount))
	}
	loadKey := func() *commonpb.IdempotencyKeyValue {
		h, err := dataStore.NewDirectReadHandle()
		require.NoError(t, err)

		defer func() { _ = h.Close() }()

		v, err := LoadIdempotencyKey(h, key)
		require.NoError(t, err)

		return v
	}

	r, err := machine.ApplyEntries(ctx, dataStore, makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)

	// A0 (index 2, date 1_700_000_002): freeze V0. expires_at = created_at + tiny TTL.
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 2, keyed(1_700_000_002, nil, fund(100))))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)
	v0 := loadKey()
	require.NotNil(t, v0)
	require.NotZero(t, v0.GetExpiresAt())

	// A (index 3, date 1_700_000_100): plan carries V0, but V0 is now expired, so A
	// reuses the key and executes fresh, installing a newer live outcome V1.
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 3, keyed(1_700_000_100, v0, fund(100))))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)
	require.Len(t, r.Results[0].Logs, 1)
	seqA := r.Results[0].Logs[0].GetCreatedLog().GetSequence()
	require.NotZero(t, seqA, "A must execute fresh over the expired key")
	v1 := loadKey()
	require.Greater(t, v1.GetCreatedAt(), v0.GetCreatedAt(), "A installed a newer outcome")

	// B-identical (index 4, date 1_700_000_101): stale plan (V0), same content, applies
	// after A. The freshness guard keeps V1, so the gate replays A, not re-executes.
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 4, keyed(1_700_000_101, v0, fund(100))))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)
	require.Len(t, r.Results[0].Logs, 1)
	require.Nil(t, r.Results[0].Logs[0].GetCreatedLog(), "identical duplicate must NOT create a second log")
	require.Equal(t, seqA, r.Results[0].Logs[0].GetReferenceSequence(), "identical duplicate replays A's log")
	require.True(t, v1.EqualVT(loadKey()), "the live outcome must be unchanged by the replay")

	// B-different (index 5, date 1_700_000_102): stale plan (V0), different content. The
	// live V1 stands, so the gate conflicts instead of executing a second txn.
	r, err = machine.ApplyEntries(ctx, dataStore, makeEntry(t, 5, keyed(1_700_000_102, v0, fund(999))))
	require.NoError(t, err)
	var conflict *domain.ErrIdempotencyKeyConflict
	require.ErrorAs(t, r.Results[0].Error, &conflict, "different content under a live key must conflict, not execute")
	require.True(t, v1.EqualVT(loadKey()), "a conflict must not overwrite the live outcome")

	// Restart: a fresh machine rebuilds the map from Pebble. It must hold V1 (A),
	// not the stale V0, and re-applying B's stale plan must still not resurrect V0.
	recovered := recoverMachineOnStore(t, dataStore)
	got, ok := recovered.Registry.Idempotency.Get(key)
	require.True(t, ok, "the live outcome survives restart in the rebuilt map")
	require.True(t, v1.EqualVT(got), "the rebuilt map holds A's outcome, not the stale V0")

	r, err = recovered.ApplyEntries(ctx, dataStore, makeEntry(t, 6, keyed(1_700_000_103, v0, fund(100))))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)
	require.Nil(t, r.Results[0].Logs[0].GetCreatedLog(), "after restart, a stale-plan duplicate still replays")
}
