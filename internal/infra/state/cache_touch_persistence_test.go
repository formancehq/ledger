package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// Asserts a CacheTouch promotion lands at 0xFF gen0Byte and survives a
// restart — without the mirror, RestoreFromStore would put the entry back
// into gen1 and the next rotation would evict it.
func TestPreload_TouchIsPersistedToCacheZone(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	registry := machine.Registry

	// gen0Byte / gen1Byte for currentGeneration=0.
	const (
		gen0Byte byte = 0
		gen1Byte byte = 1
	)

	// Pre-touch shape: entry in gen1 in-memory and at 0xFF gen1Byte.
	ledgerKey := domain.LedgerKey{Name: "gaming"}
	id := attributes.HashU128(attributes.DefaultSeeds, ledgerKey.Bytes())
	info := &commonpb.LedgerInfo{Name: "gaming"}

	registry.Cache.Ledgers.Gen1().Put(id, attributes.Entry[*commonpb.LedgerInfo]{Tag: 7, Data: info})

	seedBatch := dataStore.NewBatch()
	infoBytes, err := info.MarshalVT()
	require.NoError(t, err)
	require.NoError(t,
		writeCacheRaw(seedBatch, gen1Byte, dal.AttributeCodeLedger, id, 7, infoBytes))
	require.NoError(t, seedBatch.Commit())

	gen1Key := []byte{dal.KeyPrefixCacheSnapshot, gen1Byte, dal.AttributeCodeLedger}
	gen1Key = append(gen1Key, id[:]...)
	gen0Key := []byte{dal.KeyPrefixCacheSnapshot, gen0Byte, dal.AttributeCodeLedger}
	gen0Key = append(gen0Key, id[:]...)

	if val, closer, getErr := dataStore.Get(gen1Key); getErr == nil {
		require.NotEmpty(t, val)
		require.NoError(t, closer.Close())
	} else {
		t.Fatalf("seed: gen1 row missing: %v", getErr)
	}

	_, _, err = dataStore.Get(gen0Key)
	require.Error(t, err, "seed: gen0 row should not exist yet")

	applyBatch := dataStore.NewBatch()
	defer func() { _ = applyBatch.Cancel() }()

	preloadSet := &raftcmdpb.PreloadSet{
		LastPersistedIndex: registry.Cache.BaseIndex.Gen0,
		Touches: []*raftcmdpb.CacheTouch{{
			Id:       id[:],
			AttrType: uint32(dal.AttributeCodeLedger),
		}},
	}

	require.NoError(t, machine.Preload(preloadSet, applyBatch, gen0Byte))
	require.NoError(t, applyBatch.Commit())

	// Touch is a copy, not a move — gen1 keeps the entry.
	got, ok := registry.Cache.Ledgers.Gen0().Get(id)
	require.True(t, ok, "after touch: gen0 must have the entry")
	require.Equal(t, "gaming", got.Data.GetName())
	require.Equal(t, uint64(7), got.Tag)

	if val, closer, getErr := dataStore.Get(gen0Key); getErr != nil {
		t.Fatalf("0xFF gen0 row missing after touch: %v", getErr)
	} else {
		require.NotEmpty(t, val)
		require.NoError(t, closer.Close())
	}

	// Restart simulation.
	registry.Cache.Reset()
	require.NoError(t, machine.cacheSnapshotter.RestoreFromStore())

	restored, ok := registry.Cache.Ledgers.Gen0().Get(id)
	require.True(t, ok, "gen0 must hold the entry after restore")
	require.Equal(t, "gaming", restored.Data.GetName())
}

// Asserts a CacheTouch for a key already in gen0 is a no-op and doesn't
// overwrite 0xFF gen0Byte (which may hold a fresher in-batch Merge value).
func TestPreload_TouchSkipsWhenGen0HasFreshValue(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	registry := machine.Registry

	const (
		gen0Byte byte = 0
		gen1Byte byte = 1
	)

	ledgerKey := domain.LedgerKey{Name: "gaming"}
	id := attributes.HashU128(attributes.DefaultSeeds, ledgerKey.Bytes())

	staleInfo := &commonpb.LedgerInfo{Name: "gaming-stale"}
	freshInfo := &commonpb.LedgerInfo{Name: "gaming-fresh"}

	// Post-Merge shape: stale in gen1 + 0xFF gen1Byte; fresh in gen0 + 0xFF gen0Byte.
	registry.Cache.Ledgers.Gen1().Put(id, attributes.Entry[*commonpb.LedgerInfo]{Tag: 1, Data: staleInfo})
	registry.Cache.Ledgers.Gen0().Put(id, attributes.Entry[*commonpb.LedgerInfo]{Tag: 1, Data: freshInfo})

	seedBatch := dataStore.NewBatch()
	staleBytes, err := staleInfo.MarshalVT()
	require.NoError(t, err)

	freshBytes, err := freshInfo.MarshalVT()
	require.NoError(t, err)
	require.NoError(t,
		writeCacheRaw(seedBatch, gen1Byte, dal.AttributeCodeLedger, id, 1, staleBytes))
	require.NoError(t,
		writeCacheRaw(seedBatch, gen0Byte, dal.AttributeCodeLedger, id, 1, freshBytes))
	require.NoError(t, seedBatch.Commit())

	applyBatch := dataStore.NewBatch()
	defer func() { _ = applyBatch.Cancel() }()

	preloadSet := &raftcmdpb.PreloadSet{
		LastPersistedIndex: registry.Cache.BaseIndex.Gen0,
		Touches: []*raftcmdpb.CacheTouch{{
			Id:       id[:],
			AttrType: uint32(dal.AttributeCodeLedger),
		}},
	}
	require.NoError(t, machine.Preload(preloadSet, applyBatch, gen0Byte))
	require.NoError(t, applyBatch.Commit())

	got, ok := registry.Cache.Ledgers.Gen0().Get(id)
	require.True(t, ok)
	require.Equal(t, "gaming-fresh", got.Data.GetName(), "touch must not overwrite fresh gen0 value")

	gen0Key := []byte{dal.KeyPrefixCacheSnapshot, gen0Byte, dal.AttributeCodeLedger}
	gen0Key = append(gen0Key, id[:]...)
	val, closer, err := dataStore.Get(gen0Key)
	require.NoError(t, err)

	defer func() { _ = closer.Close() }()

	// Lean format: [8-byte tag LE][value bytes].
	require.Equal(t, freshBytes, val[8:], "0xFF gen0 row must not be clobbered with stale gen1 value")
}

// Asserts a CacheMiss preload for a key already in gen0 is a no-op for that
// generation — must not clobber a fresher in-batch Merge value at 0xFF gen0Byte
// with the (potentially stale) preload value computed at admission time.
func TestPreload_FullPreloadSkipsWhenGen0HasFreshValue(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	registry := machine.Registry

	const (
		gen0Byte byte = 0
		gen1Byte byte = 1
	)

	canonicalKey := []byte("lending\x00disburse_loan\x001.0.0")
	hash := attributes.HashU128(attributes.DefaultSeeds, canonicalKey)
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

	seedBatch := dataStore.NewBatch()

	freshBytes, err := freshInfo.MarshalVT()
	require.NoError(t, err)
	require.NoError(t,
		writeCacheRaw(seedBatch, gen0Byte, dal.AttributeCodeNumscriptContent, hash, tag, freshBytes))
	require.NoError(t, seedBatch.Commit())

	// Entry N+1's preload arrives with the leader's admission-time value
	// (stale wrt the in-batch merge).
	preloadSet := &raftcmdpb.PreloadSet{
		LastPersistedIndex: registry.Cache.BaseIndex.Gen0,
		Preloads: []*raftcmdpb.Preload{{
			Type: &raftcmdpb.Preload_NumscriptContent{
				NumscriptContent: &raftcmdpb.PreloadNumscriptContent{
					Id:    &raftcmdpb.AttributeID{Id: hash[:], Tag: tag},
					Value: staleInfo,
				},
			},
		}},
	}

	applyBatch := dataStore.NewBatch()
	defer func() { _ = applyBatch.Cancel() }()

	require.NoError(t, machine.Preload(preloadSet, applyBatch, gen0Byte))
	require.NoError(t, applyBatch.Commit())

	got, ok := registry.Cache.NumscriptContents.Gen0().Get(hash)
	require.True(t, ok)
	require.Equal(t, freshInfo.GetContent(), got.Data.GetContent(),
		"in-memory gen0 must not be overwritten with stale preload")

	gen0Key := []byte{dal.KeyPrefixCacheSnapshot, gen0Byte, dal.AttributeCodeNumscriptContent}
	gen0Key = append(gen0Key, hash[:]...)
	val, closer, err := dataStore.Get(gen0Key)
	require.NoError(t, err)

	defer func() { _ = closer.Close() }()

	require.Equal(t, freshBytes, val[8:],
		"0xFF gen0 row must not be clobbered with stale preload value")

	gen1Key := []byte{dal.KeyPrefixCacheSnapshot, gen1Byte, dal.AttributeCodeNumscriptContent}
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
	registry := machine.Registry

	const (
		gen0Byte byte = 0
		gen1Byte byte = 1
	)

	// Seed the meta sentinel so RestoreFromStore has something to anchor on.
	require.NoError(t, persistToStore(machine.cacheSnapshotter))

	canonicalKey := []byte("lending\x00disburse_loan\x001.0.0")
	hash := attributes.HashU128(attributes.DefaultSeeds, canonicalKey)
	tag := uint64(42)

	scriptInfo := &commonpb.NumscriptInfo{
		Ledger:  "lending",
		Name:    "disburse_loan",
		Version: "1.0.0",
		Content: "send $amount (source = @world destination = $borrower_loan)",
	}

	preloadSet := &raftcmdpb.PreloadSet{
		LastPersistedIndex: registry.Cache.BaseIndex.Gen0,
		Preloads: []*raftcmdpb.Preload{{
			Type: &raftcmdpb.Preload_NumscriptContent{
				NumscriptContent: &raftcmdpb.PreloadNumscriptContent{
					Id:    &raftcmdpb.AttributeID{Id: hash[:], Tag: tag},
					Value: scriptInfo,
				},
			},
		}},
	}

	applyBatch := dataStore.NewBatch()
	defer func() { _ = applyBatch.Cancel() }()

	require.NoError(t, machine.Preload(preloadSet, applyBatch, gen0Byte))
	require.NoError(t, applyBatch.Commit())

	gotGen0, ok := registry.Cache.NumscriptContents.Gen0().Get(hash)
	require.True(t, ok, "gen0 must hold the value after preload")
	require.Equal(t, scriptInfo.GetContent(), gotGen0.Data.GetContent())
	require.Equal(t, tag, gotGen0.Tag)

	gotGen1, ok := registry.Cache.NumscriptContents.Gen1().Get(hash)
	require.True(t, ok, "gen1 must hold the value after preload")
	require.Equal(t, scriptInfo.GetContent(), gotGen1.Data.GetContent())

	for _, b := range []byte{gen0Byte, gen1Byte} {
		key := []byte{dal.KeyPrefixCacheSnapshot, b, dal.AttributeCodeNumscriptContent}
		key = append(key, hash[:]...)

		val, closer, getErr := dataStore.Get(key)
		require.NoErrorf(t, getErr, "0xFF byte %d row missing after preload", b)
		require.NotEmpty(t, val)
		require.NoError(t, closer.Close())
	}

	// Restart simulation.
	registry.Cache.Reset()
	require.NoError(t, machine.cacheSnapshotter.RestoreFromStore())

	restored, ok := registry.Cache.NumscriptContents.Gen0().Get(hash)
	require.True(t, ok, "gen0 must hold the value after restore")
	require.Equal(t, scriptInfo.GetContent(), restored.Data.GetContent())

	restoredGen1, ok := registry.Cache.NumscriptContents.Gen1().Get(hash)
	require.True(t, ok, "gen1 must hold the value after restore")
	require.Equal(t, scriptInfo.GetContent(), restoredGen1.Data.GetContent())
}
