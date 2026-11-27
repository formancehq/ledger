package readstore

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type failingEventGCIterator struct {
	eventGCIterator

	failOnNext int
	nextCalls  int
	failed     bool
	err        error
}

type countingEventGCIterator struct {
	eventGCIterator

	nextCalls int
}

func (i *countingEventGCIterator) Next() bool {
	i.nextCalls++

	return i.eventGCIterator.Next()
}

func (i *failingEventGCIterator) Valid() bool {
	return !i.failed && i.eventGCIterator.Valid()
}

func (i *failingEventGCIterator) Next() bool {
	i.nextCalls++
	if i.nextCalls == i.failOnNext {
		i.failed = true

		return false
	}

	return i.eventGCIterator.Next()
}

func (i *failingEventGCIterator) Error() error {
	if i.failed {
		return i.err
	}

	return i.eventGCIterator.Error()
}

func TestGCEventZone_IteratorFailureDiscardsPendingDeletes(t *testing.T) {
	t.Parallel()

	s, _ := eventFixture(t, "v",
		ev{"a", 10, IndexEventAdd},
		ev{"a", 20, IndexEventDel},
		ev{"b", 10, IndexEventAdd},
	)

	iter, err := s.DB().NewIter(&pebble.IterOptions{
		LowerBound: []byte{PrefixMetadataIndex},
		UpperBound: IncrementBytes([]byte{PrefixMetadataIndex}),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()

	injected := errors.New("injected iterator failure")
	failing := &failingEventGCIterator{
		eventGCIterator: iter,
		failOnNext:      3,
		err:             injected,
	}
	changed, next, err := gcEventZoneWithIterator(s.DB(), failing, 30, 1<<20)
	require.ErrorIs(t, err, injected)
	require.Zero(t, changed)
	require.Nil(t, next)

	kb := dal.NewKeyBuilder()
	for _, key := range [][]byte{
		MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte("v"), []byte("a"), 10, IndexEventAdd),
		MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte("v"), []byte("a"), 20, IndexEventDel),
	} {
		_, closer, getErr := s.DB().Get(key)
		require.NoError(t, getErr, "iterator failure must discard the whole pending delete batch")
		require.NoError(t, closer.Close())
	}
}

// The pruning rule: below the watermark, only a group's latest event may
// survive, and only as an ADD. Resolution at any pin >= watermark must be
// identical before and after.
func TestGCEventZone(t *testing.T) {
	t.Parallel()

	s, prefix := eventFixture(t, "v",
		ev{"dead", 10, IndexEventAdd}, // add@10 del@20: whole pair reclaimed
		ev{"dead", 20, IndexEventDel},
		ev{"live", 10, IndexEventAdd}, // superseded by re-add
		ev{"live", 20, IndexEventDel},
		ev{"live", 25, IndexEventAdd}, // latest below watermark, ADD: survives
		ev{"old", 10, IndexEventAdd},  // single old ADD: survives
		ev{"span", 10, IndexEventAdd}, // ADD below + DEL above: both survive
		ev{"span", 40, IndexEventDel},
	)

	const watermark = 30

	before := map[uint64][]string{}
	for _, pin := range []uint64{30, 35, 45} {
		before[pin] = collect(t, s, prefix, pin)
	}

	changed, next, err := GCEventZone(s.DB(), PrefixMetadataIndex, nil, watermark, 1<<20)
	require.NoError(t, err)
	require.Equal(t, 4, changed, "dead's pair + live's superseded pair")
	require.Nil(t, next, "single pass covers the whole zone")

	for _, pin := range []uint64{30, 35, 45} {
		require.Equal(t, before[pin], collect(t, s, prefix, pin), "pin %d verdicts unchanged", pin)
	}

	// The survivors are exactly: live@25(ADD), old@10(ADD), span@10(ADD), span@40(DEL).
	remaining := 0
	it, err := s.DB().NewIter(nil)
	require.NoError(t, err)
	defer func() { _ = it.Close() }()
	for it.First(); it.Valid(); it.Next() {
		remaining++
	}
	require.Equal(t, 4, remaining)
}

// A budget-bounded walk resumes at a group boundary and converges to the
// same end state as one unbounded pass.
func TestGCEventZone_BudgetedResume(t *testing.T) {
	t.Parallel()

	s, prefix := eventFixture(t, "v",
		ev{"a", 10, IndexEventAdd},
		ev{"a", 20, IndexEventDel},
		ev{"b", 10, IndexEventAdd},
		ev{"b", 20, IndexEventDel},
		ev{"c", 10, IndexEventAdd},
	)

	const watermark = 30

	totalChanged := 0

	var resume []byte

	rounds := 0
	for {
		rounds++
		require.Less(t, rounds, 10, "walk must terminate")

		changed, next, err := GCEventZone(s.DB(), PrefixMetadataIndex, resume, watermark, 2)
		require.NoError(t, err)

		totalChanged += changed
		if next == nil {
			break
		}

		resume = next
	}

	require.Greater(t, rounds, 1, "budget of 2 keys must split the walk")
	require.Equal(t, 4, totalChanged, "a's pair + b's pair reclaimed, c's ADD kept")
	require.Equal(t, []string{"c"}, collect(t, s, prefix, 35))
}

// A single hot group must make durable progress without making one builder
// wake proportional to its entire history. A backfill between slices must
// also be seen when the shortened group is revisited.
func TestGCEventZone_HotGroupAndLateBackfill(t *testing.T) {
	for _, tc := range []struct {
		name string
		zone byte
		key  func(*dal.KeyBuilder, uint64) []byte
	}{
		{"metadata", PrefixMetadataIndex, func(kb *dal.KeyBuilder, seq uint64) []byte {
			return MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte("v"), []byte("a"), seq, IndexEventAdd)
		}},
		{"exists", PrefixEntityExists, func(kb *dal.KeyBuilder, seq uint64) []byte {
			return EntityExistsEventKeyV(kb, "l", NamespaceAccount, "k", 1, false, []byte("a"), seq, IndexEventAdd)
		}},
		{"asset", PrefixAccountByAsset, func(kb *dal.KeyBuilder, seq uint64) []byte {
			return AccountByAssetEventKey(kb, "l", "USD", 2, "accounts:a", seq)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newTestStore(t)
			kb := dal.NewKeyBuilder()
			for seq := uint64(1); seq <= 128; seq++ {
				require.NoError(t, s.DB().Set(tc.key(kb, seq), nil, pebble.NoSync))
			}
			const budget = 17
			iter, err := s.DB().NewIter(&pebble.IterOptions{LowerBound: []byte{tc.zone}, UpperBound: IncrementBytes([]byte{tc.zone})})
			require.NoError(t, err)
			counted := &countingEventGCIterator{eventGCIterator: iter}
			changed, resume, err := gcEventZoneWithIterator(s.DB(), counted, 129, budget)
			require.NoError(t, iter.Close())
			require.NoError(t, err)
			require.Equal(t, budget-1, changed)
			require.LessOrEqual(t, counted.nextCalls, budget, "one slice must stop within the group")
			require.NotNil(t, resume)
			// The backfill arrives behind the cursor after the first commit.
			require.NoError(t, s.DB().Set(tc.key(kb, 0), nil, pebble.NoSync))
			total := changed
			for round := 0; resume != nil; round++ {
				require.Less(t, round, 20, "hot group must converge")
				changed, resume, err = GCEventZone(s.DB(), tc.zone, resume, 129, budget)
				require.NoError(t, err)
				require.LessOrEqual(t, changed, budget-1, "one slice must be bounded")
				total += changed
			}
			require.Equal(t, 128, total)
			iter, err = s.DB().NewIter(&pebble.IterOptions{LowerBound: []byte{tc.zone}, UpperBound: IncrementBytes([]byte{tc.zone})})
			require.NoError(t, err)
			defer func() { require.NoError(t, iter.Close()) }()
			require.True(t, iter.First())
			require.True(t, bytes.Equal(tc.key(kb, 128), iter.Key()))
			require.False(t, iter.Next())
		})
	}
}

func TestGCEventZone_SkipsLongAboveWatermarkTail(t *testing.T) {
	t.Parallel()
	s, _ := eventFixture(t, "v", ev{"a", 1, IndexEventAdd}, ev{"b", 1, IndexEventAdd}, ev{"b", 2, IndexEventDel})
	kb := dal.NewKeyBuilder()
	for seq := uint64(100); seq < 200; seq++ {
		require.NoError(t, s.DB().Set(MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte("v"), []byte("a"), seq, IndexEventAdd), nil, pebble.NoSync))
	}
	var resume []byte
	total := 0
	for round := range 10 {
		changed, next, err := GCEventZone(s.DB(), PrefixMetadataIndex, resume, 50, 3)
		require.NoError(t, err)
		total += changed
		if next == nil {
			break
		}
		resume = next
		require.Less(t, round, 9, "above-watermark tail must be skipped")
	}
	require.Equal(t, 2, total, "dead group b is still reached")
}

func TestGCEventZone_ReclaimsDelBeforeAboveWatermarkTail(t *testing.T) {
	t.Parallel()
	s, prefix := eventFixture(t, "v", ev{"a", 1, IndexEventDel}, ev{"a", 100, IndexEventAdd})
	before50 := collect(t, s, prefix, 50)
	before100 := collect(t, s, prefix, 100)
	changed, next, err := GCEventZone(s.DB(), PrefixMetadataIndex, nil, 50, 2)
	require.NoError(t, err)
	require.Equal(t, 1, changed)
	require.NotNil(t, next)
	require.Equal(t, before50, collect(t, s, prefix, 50))
	require.Equal(t, before100, collect(t, s, prefix, 100))
}

// Benchmark one builder wake with a hot account/asset group in a read store
// configured like production. Flush the seeded history before timing GC.
// Run with -benchtime=1x to measure a fresh 100k-event group.
func BenchmarkGCEventZone_HotAssetSlice(b *testing.B) {
	for b.Loop() {
		b.StopTimer()
		store, err := New(b.TempDir(), logging.NopZap(), DefaultConfig())
		if err != nil {
			b.Fatal(err)
		}
		db := store.DB()
		kb := dal.NewKeyBuilder()
		batch := db.NewBatch()
		for seq := uint64(1); seq <= 100_000; seq++ {
			if err := batch.Set(AccountByAssetEventKey(kb, "l", "USD", 2, "accounts:hot", seq), nil, nil); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			b.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			b.Fatal(err)
		}
		if err := db.Flush(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		changed, next, err := GCEventZone(db, PrefixAccountByAsset, nil, 100_001, 4096)
		b.StopTimer()
		if err != nil || changed != 4095 || next == nil {
			b.Fatalf("GC slice: changed=%d next=%x err=%v", changed, next, err)
		}
		if err := store.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}
