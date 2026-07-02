package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestPreload_RejectsMalformedAttributeCoverage pins the gate that catches
// forged or partially-decoded plans before any MirrorPreload can mutate the
// cache. Without this, a coverage-only entry with a nil AttributeID would
// silently zero-pad through scope's applyPlans and admit a phantom U128 into
// the coverage slot; a seed entry would land the zero-padded U128 in the
// cache and a 0xFF Pebble write that the failure-audit batch would commit
// on later business rejection.
func TestPreload_RejectsMalformedAttributeCoverage(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)

	plan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: machine.Registry.Cache.BaseIndex.Gen0,
		Attributes: []*raftcmdpb.AttributeCoverage{{
			Id:       nil, // forged / decoded-incomplete envelope
			AttrCode: uint32(dal.SubAttrLedger),
		}},
	}

	batch := dataStore.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()

	err := machine.Preload(plan, batch, 0, 0)
	require.Error(t, err)

	var invalid *domain.ErrInvalidExecutionPlan
	require.ErrorAs(t, err, &invalid)
	require.Contains(t, invalid.Reason_, "16-byte AttributeID")
}

// TestPreload_RejectsUnknownAttrCode pins that a forged ExecutionPlan
// whose AttributeCoverage declares an attr_code the FSM does not handle is
// caught at Preload entry. Without the gate, a seed intent's MirrorPreload
// would route the write to an orphan 0xFF Pebble slot, and a technical-only
// / no-read proposal would never reach a scope-level validation that could
// catch it.
func TestPreload_RejectsUnknownAttrCode(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	u128, _ := attributes.MakeKey(domain.LedgerKey{Name: "L"}.Bytes())

	plan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: machine.Registry.Cache.BaseIndex.Gen0,
		Attributes: []*raftcmdpb.AttributeCoverage{{
			Id:       &raftcmdpb.AttributeID{Id: u128[:]},
			AttrCode: 0xff, // unknown attr_code

		}},
	}

	batch := dataStore.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()

	err := machine.Preload(plan, batch, 0, 0)
	require.Error(t, err)

	var invalid *domain.ErrInvalidExecutionPlan
	require.ErrorAs(t, err, &invalid)
	require.Contains(t, invalid.Reason_, "0xff")
	require.Contains(t, invalid.Reason_, "FSM does not handle")
}

// TestPreload_IdempotencyOnlyProposalAppliesKeys pins the behaviour for a
// proposal that ships only idempotency keys (no AttributeCoverage entries) —
// the typical shape of an idempotent maintenance / signature order with no
// attribute needs. The early-exit on `len(GetAttributes()) == 0` must NOT
// short-circuit the IdempotencyStore restore, otherwise at-most-once
// breaks on replay (#462 NumaryBot blocker).
func TestPreload_IdempotencyOnlyProposalAppliesKeys(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	_ = dataStore

	const gen0Byte byte = 0

	value := &commonpb.IdempotencyKeyValue{FirstLogSequence: 42, LogCount: 1, Hash: []byte("h"), HashVersion: 1, CreatedAt: 1700000000}

	executionPlan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: machine.Registry.Cache.BaseIndex.Gen0,
		IdempotencyKeys: []*raftcmdpb.ReloadIdempotencyKey{{
			Key:   "idem-only",
			Value: value,
		}},
	}

	batch := dataStore.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()

	require.NoError(t, machine.Preload(executionPlan, batch, gen0Byte, 0))

	got, ok := machine.Registry.Idempotency.Get("idem-only")
	require.True(t, ok, "idempotency key must be present after Preload even with no AttributeCoverage entries")
	require.Equal(t, uint64(42), got.GetFirstLogSequence())
}

// Asserts a CacheMiss preload for a key already in gen0 is a no-op for that
// generation — must not clobber a fresher in-batch Merge value at 0xFF gen0Byte
// with the (potentially stale) preload value computed at admission time.
func TestPreload_FullPreloadSkipsWhenGen0HasFreshValue(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	_ = dataStore
	registry := machine.Registry

	const (
		gen0Byte byte = 0
		gen1Byte byte = 1
	)

	canonicalKey := []byte("lending\x00disburse_loan\x001.0.0")
	hash := attributes.HashU128(canonicalKey)
	tag := uint64(42)

	staleInfo := &commonpb.NumscriptInfo{
		Ledger:  "lending",
		Name:    "disburse_loan",
		Version: "1.0.0",
		Content: "STALE — leader's admission view",
	}
	freshInfo := &commonpb.NumscriptInfo{
		Ledger:  "lending",
		Name:    "disburse_loan",
		Version: "1.0.0",
		Content: "FRESH — written by an earlier in-batch Merge",
	}

	// Post-Merge shape on entry N: fresh value in gen0 + 0xFF gen0Byte.
	registry.Cache.NumscriptContents.Gen0().Put(hash, attributes.Entry[*commonpb.NumscriptInfo]{Tag: tag, Data: freshInfo})

	seedBatch := dataStore.OpenWriteSession()

	freshBytes, err := freshInfo.MarshalVT()
	require.NoError(t, err)
	require.NoError(t,
		writeCacheRaw(seedBatch, gen0Byte, dal.SubAttrNumscriptContent, hash, tag, false, freshBytes))
	require.NoError(t, seedBatch.Commit())

	// Entry N+1's preload arrives with the leader's admission-time value
	// (stale wrt the in-batch merge).
	executionPlan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: registry.Cache.BaseIndex.Gen0,
		Attributes: []*raftcmdpb.AttributeCoverage{{
			Id:       &raftcmdpb.AttributeID{Id: hash[:], Tag: tag},
			AttrCode: uint32(dal.SubAttrNumscriptContent),
			Value:    rawPreload(t, dal.SubAttrNumscriptContent, staleInfo),
		}},
		// NOTE: helper rawPreload defined at the bottom of this file.
	}

	applyBatch := dataStore.OpenWriteSession()
	defer func() { _ = applyBatch.Cancel() }()

	require.NoError(t, machine.Preload(executionPlan, applyBatch, gen0Byte, 0))
	require.NoError(t, applyBatch.Commit())

	got, ok := registry.Cache.NumscriptContents.Gen0().Get(hash)
	require.True(t, ok)
	require.Equal(t, freshInfo.GetContent(), got.Data.GetContent(),
		"in-memory gen0 must not be overwritten with stale preload")

	gen0Key := []byte{dal.ZoneCache, gen0Byte, dal.SubAttrNumscriptContent}
	gen0Key = append(gen0Key, hash[:]...)
	val, closer, err := dataStore.Get(gen0Key)
	require.NoError(t, err)

	defer func() { _ = closer.Close() }()

	require.Equal(t, cacheValueFlagLive, val[8], "0xFF gen0 row must be live")
	require.Equal(t, freshBytes, val[cacheValueHeaderLen:],
		"0xFF gen0 row must not be clobbered with stale preload value")

	gen1Key := []byte{dal.ZoneCache, gen1Byte, dal.SubAttrNumscriptContent}
	gen1Key = append(gen1Key, hash[:]...)
	gen1Val, gen1Closer, err := dataStore.Get(gen1Key)
	require.NoError(t, err, "0xFF gen1 row should still be populated from the preload")
	require.NotEmpty(t, gen1Val)
	require.NoError(t, gen1Closer.Close())
}

// Asserts a full preload (CacheMiss path) lands at both 0xFF byte positions
// and survives a restart, even when no order subsequently modifies the key.
func TestPreload_FullPreloadIsPersistedToCacheZone(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	_ = dataStore
	registry := machine.Registry

	const (
		gen0Byte byte = 0
		gen1Byte byte = 1
	)

	// Seed the meta sentinel so RestoreFromStore has something to anchor on.
	require.NoError(t, persistToStore(machine.cacheSnapshotter, dataStore))

	canonicalKey := []byte("lending\x00disburse_loan\x001.0.0")
	hash := attributes.HashU128(canonicalKey)
	tag := uint64(42)

	scriptInfo := &commonpb.NumscriptInfo{
		Ledger:  "lending",
		Name:    "disburse_loan",
		Version: "1.0.0",
		Content: "send $amount (source = @world destination = $borrower_loan)",
	}

	executionPlan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: registry.Cache.BaseIndex.Gen0,
		Attributes: []*raftcmdpb.AttributeCoverage{{
			Id:       &raftcmdpb.AttributeID{Id: hash[:], Tag: tag},
			AttrCode: uint32(dal.SubAttrNumscriptContent),
			Value:    rawPreload(t, dal.SubAttrNumscriptContent, scriptInfo),
		}},
	}

	applyBatch := dataStore.OpenWriteSession()
	defer func() { _ = applyBatch.Cancel() }()

	require.NoError(t, machine.Preload(executionPlan, applyBatch, gen0Byte, 0))
	require.NoError(t, applyBatch.Commit())

	gotGen0, ok := registry.Cache.NumscriptContents.Gen0().Get(hash)
	require.True(t, ok, "gen0 must hold the value after preload")
	require.Equal(t, scriptInfo.GetContent(), gotGen0.Data.GetContent())
	require.Equal(t, tag, gotGen0.Tag)

	gotGen1, ok := registry.Cache.NumscriptContents.Gen1().Get(hash)
	require.True(t, ok, "gen1 must hold the value after preload")
	require.Equal(t, scriptInfo.GetContent(), gotGen1.Data.GetContent())

	for _, b := range []byte{gen0Byte, gen1Byte} {
		key := []byte{dal.ZoneCache, b, dal.SubAttrNumscriptContent}
		key = append(key, hash[:]...)

		val, closer, getErr := dataStore.Get(key)
		require.NoErrorf(t, getErr, "0xFF byte %d row missing after preload", b)
		require.NotEmpty(t, val)
		require.NoError(t, closer.Close())
	}

	// Restart simulation.
	registry.Cache.Reset()
	require.NoError(t, machine.cacheSnapshotter.RestoreFromStore(dataStore))

	restored, ok := registry.Cache.NumscriptContents.Gen0().Get(hash)
	require.True(t, ok, "gen0 must hold the value after restore")
	require.Equal(t, scriptInfo.GetContent(), restored.Data.GetContent())

	restoredGen1, ok := registry.Cache.NumscriptContents.Gen1().Get(hash)
	require.True(t, ok, "gen1 must hold the value after restore")
	require.Equal(t, scriptInfo.GetContent(), restoredGen1.Data.GetContent())
}
