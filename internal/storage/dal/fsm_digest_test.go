package dal

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// newDigestStore builds a Store with DeterministicEncoding=true so that
// OpenFSMWriteSession actually attaches the supplied chain. With the flag
// off Store.OpenFSMWriteSession returns a plain session (the production
// short-circuit) which would defeat every test below.
func newDigestStore(t *testing.T) *Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	cfg := DefaultConfig()
	cfg.DeterministicEncoding = true

	s, err := NewStore(t.TempDir(), logger, meter, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

// testChain is a deterministic, dependency-free FSMDigestChain for unit
// tests. It uses plain XXH3-128 (no per-cluster seed) so two test stores
// with different tempdirs but the same write sequence produce the same
// hash — exactly the cross-node-equivalent the FSM contract guarantees in
// production (the per-cluster seed in processing.NewFSMDigestChain
// preserves the same property because all peers share the same ClusterID).
type testChain struct {
	out [16]byte
}

func newTestChain() *testChain { return &testChain{} }

func (c *testChain) Advance(prevHash, entryOps []byte) []byte {
	h := xxh3.New()
	_, _ = h.Write(entryOps)
	_, _ = h.Write(prevHash)
	sum := h.Sum128()
	c.out = sum.Bytes()

	return c.out[:]
}

// TestFSMDigest_BatchBoundaryInvariance is the headline invariant: applying
// the same per-entry write streams produces the same final rolling digest
// regardless of how those entries are grouped into Pebble commits. This is
// the exact property that the old (per-batch chain) design failed to
// satisfy cross-node and that the new per-entry chain restores.
func TestFSMDigest_BatchBoundaryInvariance(t *testing.T) {
	t.Parallel()

	type entryWrites struct {
		sets    [][2][]byte // (key, value)
		deletes [][]byte
	}

	entries := []entryWrites{
		{sets: [][2][]byte{
			{{ZoneAttributes, SubAttrVolume, 0xAA}, []byte("vol-1")},
			{{ZoneAttributes, SubAttrMetadata, 0xBB}, []byte("meta-1")},
		}},
		{sets: [][2][]byte{
			{{ZoneCold, SubColdLog, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05}, []byte("log-5")},
		}},
		{
			sets: [][2][]byte{
				{{ZoneIdempotency, SubIdempKeys, 0x42}, []byte("idemp-42")},
			},
			deletes: [][]byte{{ZoneAttributes, SubAttrVolume, 0xCC}},
		},
	}

	applyEntries := func(s *Store, batches [][]int) []byte {
		t.Helper()
		// batches lists entry indices grouped per Pebble commit, in order.
		// e.g. [[0],[1],[2]] = three commits of one entry each.
		// [[0,1,2]] = one commit of three entries.
		appliedIndex := uint64(0)

		for _, group := range batches {
			sess := s.OpenFSMWriteSession(newTestChain())
			for _, idx := range group {
				e := entries[idx]
				for _, kv := range e.sets {
					require.NoError(t, sess.SetBytes(kv[0], kv[1]))
				}
				for _, k := range e.deletes {
					require.NoError(t, sess.DeleteKey(k))
				}
				sess.AdvanceDigest()
				appliedIndex++
			}
			_, err := sess.CommitWithRollingDigest(appliedIndex)
			require.NoError(t, err)
		}

		_, hash := s.RollingDigest()

		return hash
	}

	// Layout A: leader-like — one entry per Pebble commit.
	storeA := newDigestStore(t)
	hashA := applyEntries(storeA, [][]int{{0}, {1}, {2}})

	// Layout B: follower-like — three entries grouped into one commit.
	storeB := newDigestStore(t)
	hashB := applyEntries(storeB, [][]int{{0, 1, 2}})

	// Layout C: mixed — first two together, third alone.
	storeC := newDigestStore(t)
	hashC := applyEntries(storeC, [][]int{{0, 1}, {2}})

	require.NotEqual(t, ZeroFSMDigest, hashA, "digest must advance away from zero seed")
	require.Equal(t, hashA, hashB, "1-per-batch vs 3-in-one-batch must yield the same final digest")
	require.Equal(t, hashA, hashC, "mixed batching must yield the same final digest as any other grouping")
}

// TestFSMDigest_NonHashedZonesDoNotPerturb validates that writes outside
// the hashed-zones set ({Attributes, Cold, Idempotency}) are byte-for-byte
// transparent to the rolling digest. Two sessions with the same hashed-
// zone writes but DIFFERENT writes to non-hashed zones must produce the
// same final digest. This is the corollary that lets each node hold its
// own node-local Cache/PerLedger/Global state without contaminating the
// cross-node check.
func TestFSMDigest_NonHashedZonesDoNotPerturb(t *testing.T) {
	t.Parallel()

	apply := func(s *Store, extraNonHashed [][2][]byte) []byte {
		sess := s.OpenFSMWriteSession(newTestChain())
		require.NoError(t, sess.SetBytes([]byte{ZoneAttributes, SubAttrVolume, 0xAA}, []byte("vol-1")))
		require.NoError(t, sess.SetBytes([]byte{ZoneCold, SubColdLog, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, []byte("log-1")))

		for _, kv := range extraNonHashed {
			require.NoError(t, sess.SetBytes(kv[0], kv[1]))
		}

		sess.AdvanceDigest()
		_, err := sess.CommitWithRollingDigest(1)
		require.NoError(t, err)

		_, hash := s.RollingDigest()

		return hash
	}

	storeA := newDigestStore(t)
	hashA := apply(storeA, nil)

	storeB := newDigestStore(t)
	hashB := apply(storeB, [][2][]byte{
		{{ZoneCache, 0x00, 0x12, 0x34}, []byte("node-local-cache")},
		{{ZonePerLedger, SubPLReversions, 0x99}, []byte("node-local-per-ledger")},
		{{ZoneClusterTransient, SubTransientBackupJob, 0x00}, []byte("node-local-transient")},
	})

	require.Equal(t, hashA, hashB,
		"writes to non-hashed zones must not perturb the rolling digest")
}

// TestFSMDigest_PersistedRecord_RoundTrip checks that the (appliedIndex,
// hash) tuple persisted by CommitWithRollingDigest round-trips through
// LoadFSMDigest and matches the in-memory cache on Store. This is the
// invariant the GetFSMDigest gRPC handler relies on to return the same
// (appliedIndex, hash) the FSM committed.
func TestFSMDigest_PersistedRecord_RoundTrip(t *testing.T) {
	t.Parallel()

	s := newDigestStore(t)

	sess := s.OpenFSMWriteSession(newTestChain())
	require.NoError(t, sess.SetBytes([]byte{ZoneAttributes, SubAttrVolume, 0xAA}, []byte("vol-1")))
	sess.AdvanceDigest()
	expected, err := sess.CommitWithRollingDigest(42)
	require.NoError(t, err)
	require.Len(t, expected, FSMDigestSize)

	// In-memory cache reflects the just-committed state.
	cachedIdx, cachedHash := s.RollingDigest()
	require.Equal(t, uint64(42), cachedIdx, "Store rolling-digest cache must track the latest applied index")
	require.True(t, bytes.Equal(expected, cachedHash), "Store rolling-digest cache must match CommitWithRollingDigest return")

	// Pebble persistence matches.
	persistedIdx, persistedHash, err := LoadFSMDigest(s.getDB())
	require.NoError(t, err)
	require.Equal(t, uint64(42), persistedIdx, "persisted applied index must match the committed value")
	require.True(t, bytes.Equal(expected, persistedHash), "persisted hash must match the committed value")
}

// TestFSMDigest_SelfExcluded covers the recursion-guard: the
// SubGlobFSMDigest write CommitWithRollingDigest itself emits must NOT be
// folded back into the chain (it would change the value mid-flight and
// the next session would seed from a hash that depended on the previous
// commit's hash AGAIN, double-counted).
//
// We verify this indirectly: two sessions that produce the same
// hashed-zone op stream must yield the same digest, no matter how many
// times their commits happened in between (each commit writes
// SubGlobFSMDigest — if those writes leaked into the chain, two stores
// with different commit counts would diverge).
func TestFSMDigest_SelfExcluded(t *testing.T) {
	t.Parallel()

	apply := func(s *Store, splits [][]int, values []string) []byte {
		// One AdvanceDigest per (entry) write; splits[i] = entries in i-th
		// Pebble commit. Each entry writes the same single key with a
		// distinct value so the chain content depends only on the value
		// stream, not on the commit framing.
		entryIdx := 0
		appliedIndex := uint64(0)

		for _, group := range splits {
			sess := s.OpenFSMWriteSession(newTestChain())
			for range group {
				require.NoError(t, sess.SetBytes(
					[]byte{ZoneAttributes, SubAttrVolume, 0xFF},
					[]byte(values[entryIdx]),
				))
				sess.AdvanceDigest()
				entryIdx++
				appliedIndex++
			}
			_, err := sess.CommitWithRollingDigest(appliedIndex)
			require.NoError(t, err)
		}

		_, hash := s.RollingDigest()

		return hash
	}

	values := []string{"v1", "v2", "v3", "v4"}

	storeA := newDigestStore(t)
	hashA := apply(storeA, [][]int{{0}, {0}, {0}, {0}}, values) // four separate commits
	storeB := newDigestStore(t)
	hashB := apply(storeB, [][]int{{0, 0, 0, 0}}, values) // one commit, four entries

	require.Equal(t, hashA, hashB,
		"four commits vs one commit (same op stream) must produce the same digest; "+
			"divergence would indicate the SubGlobFSMDigest write is leaking into the chain")
}

// TestFSMDigest_OpOrderCanonicalised is the finding-#2 regression: two nodes
// reach byte-identical final Pebble state via a DIFFERENT op application order
// (the FSM drains its overlay by ranging Go maps, whose iteration order is
// randomised per process). AdvanceDigest must sort the per-entry op records so
// the folded bytes — and hence the rolling digest — are identical regardless
// of the order the writes arrived in. Divergence here would be a FALSE digest
// mismatch across otherwise-consistent peers.
func TestFSMDigest_OpOrderCanonicalised(t *testing.T) {
	t.Parallel()

	// Same three hashed-zone writes, presented in two different orders within
	// a single entry. Keys are distinct so final state is identical; only the
	// insertion order differs.
	writes := [][2][]byte{
		{{ZoneAttributes, SubAttrVolume, 0x03}, []byte("vol-3")},
		{{ZoneAttributes, SubAttrMetadata, 0x01}, []byte("meta-1")},
		{{ZoneIdempotency, SubIdempKeys, 0x02}, []byte("idemp-2")},
	}

	apply := func(order []int) []byte {
		s := newDigestStore(t)
		sess := s.OpenFSMWriteSession(newTestChain())
		for _, i := range order {
			require.NoError(t, sess.SetBytes(writes[i][0], writes[i][1]))
		}
		sess.AdvanceDigest()
		_, err := sess.CommitWithRollingDigest(1)
		require.NoError(t, err)

		_, hash := s.RollingDigest()

		return hash
	}

	hashForward := apply([]int{0, 1, 2})
	hashReverse := apply([]int{2, 1, 0})
	hashShuffled := apply([]int{1, 2, 0})

	require.NotEqual(t, ZeroFSMDigest, hashForward, "digest must advance away from zero seed")
	require.Equal(t, hashForward, hashReverse,
		"same final state via reversed op order must produce the same digest")
	require.Equal(t, hashForward, hashShuffled,
		"same final state via shuffled op order must produce the same digest")
}

// TestFSMDigest_PendingSeedCarriesAcrossPipeline is the finding-#1 regression:
// under pipelining PrepareDecodedEntries opens the NEXT batch's WriteSession
// before the previous batch has committed. If the next session seeded from the
// committed rolling digest it would chain from the pre-previous hash and drop
// the previous batch from the digest. RecordPendingDigest publishes the
// prepared batch's hash so the next session seeds from it. This test drives the
// pipeline ordering directly: open batch 2 (reading the pending seed) BEFORE
// batch 1 commits, and require the final digest to incorporate BOTH batches.
func TestFSMDigest_PendingSeedCarriesAcrossPipeline(t *testing.T) {
	t.Parallel()

	key := []byte{ZoneAttributes, SubAttrVolume, 0xAA}

	// Reference: strictly sequential (open→advance→record→commit, then next).
	sequential := func() []byte {
		s := newDigestStore(t)
		for i, v := range []string{"b1", "b2"} {
			sess := s.OpenFSMWriteSession(newTestChain())
			require.NoError(t, sess.SetBytes(key, []byte(v)))
			sess.AdvanceDigest()
			sess.RecordPendingDigest(uint64(i + 1))
			_, err := sess.CommitWithRollingDigest(uint64(i + 1))
			require.NoError(t, err)
		}
		_, hash := s.RollingDigest()

		return hash
	}

	// Pipelined: prepare batch 2 (which reads the pending seed at open time)
	// BEFORE batch 1 commits. The only way batch 2 chains from batch 1 is if
	// batch 1's RecordPendingDigest published its hash to the pending slot.
	pipelined := func() []byte {
		s := newDigestStore(t)

		// Prepare batch 1.
		sess1 := s.OpenFSMWriteSession(newTestChain())
		require.NoError(t, sess1.SetBytes(key, []byte("b1")))
		sess1.AdvanceDigest()
		sess1.RecordPendingDigest(1)

		// Prepare batch 2 BEFORE batch 1 commits — seeds from pending (batch 1).
		sess2 := s.OpenFSMWriteSession(newTestChain())
		require.NoError(t, sess2.SetBytes(key, []byte("b2")))
		sess2.AdvanceDigest()
		sess2.RecordPendingDigest(2)

		// Commit in order.
		_, err := sess1.CommitWithRollingDigest(1)
		require.NoError(t, err)
		_, err = sess2.CommitWithRollingDigest(2)
		require.NoError(t, err)

		_, hash := s.RollingDigest()

		return hash
	}

	// Dropped-batch control: if batch 2 seeded from the COMMITTED digest while
	// batch 1's commit was still in flight, it would chain from zero (batch 1
	// dropped). Emulate that by seeding batch 2 from a single write only.
	batch2Only := func() []byte {
		s := newDigestStore(t)
		sess := s.OpenFSMWriteSession(newTestChain())
		require.NoError(t, sess.SetBytes(key, []byte("b2")))
		sess.AdvanceDigest()
		_, err := sess.CommitWithRollingDigest(1)
		require.NoError(t, err)
		_, hash := s.RollingDigest()

		return hash
	}

	require.Equal(t, sequential(), pipelined(),
		"pipelined prepare (batch 2 opened before batch 1 commits) must yield the same digest as strict sequential — batch 1 must not be dropped")
	require.NotEqual(t, batch2Only(), pipelined(),
		"the pipelined digest must incorporate batch 1 — it must differ from a chain that saw only batch 2")
}

// TestFSMDigest_PlainCommitRefused enforces the FSM-side invariant: a
// session opened via OpenFSMWriteSession MUST call CommitWithRollingDigest.
// Calling plain Commit on such a session would advance the in-memory chain
// state without persisting it — the next session would re-seed from a
// stale Pebble value and produce a divergent hash chain.
func TestFSMDigest_PlainCommitRefused(t *testing.T) {
	t.Parallel()

	s := newDigestStore(t)
	sess := s.OpenFSMWriteSession(newTestChain())

	require.NoError(t, sess.SetBytes([]byte{ZoneAttributes, SubAttrVolume, 0xAA}, []byte("vol")))
	sess.AdvanceDigest()

	err := sess.Commit()
	require.Error(t, err, "Commit on an FSM session must be refused")
	require.Contains(t, err.Error(), "CommitWithRollingDigest")
}

// TestFSMDigest_UnflushedEntryRefused enforces the FSM-side invariant:
// CommitWithRollingDigest fails when entryOps is non-empty, i.e. when the
// FSM forgot AdvanceDigest after the last applied entry. Silently
// committing in that state would lose the last entry's writes from the
// chain — exactly the divergence the test is meant to catch.
func TestFSMDigest_UnflushedEntryRefused(t *testing.T) {
	t.Parallel()

	s := newDigestStore(t)
	sess := s.OpenFSMWriteSession(newTestChain())

	require.NoError(t, sess.SetBytes([]byte{ZoneAttributes, SubAttrVolume, 0xAA}, []byte("vol")))
	// Deliberately skip AdvanceDigest.

	_, err := sess.CommitWithRollingDigest(1)
	require.Error(t, err, "CommitWithRollingDigest must refuse to commit with unflushed entry ops")
	require.Contains(t, err.Error(), "unflushed")
}
