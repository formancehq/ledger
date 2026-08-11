package readstore

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func eventFixture(t *testing.T, value string, events ...struct {
	entity string
	seq    uint64
	op     byte
}) (*Store, []byte) {
	t.Helper()

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()

	for _, e := range events {
		key := MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte(value), []byte(e.entity), e.seq, e.op)
		require.NoError(t, s.DB().Set(key, nil, pebble.NoSync))
	}

	return s, eventValuePrefix("l", NamespaceAccount, "k", 1, []byte(value))
}

// eventValuePrefix builds the scan range covering one (metadata key, encoded
// value) pair, exactly as the compile path does: the versioned field prefix
// with the encoded value appended.
func eventValuePrefix(ledger, ns, key string, version uint32, encoded []byte) []byte {
	prefix := append([]byte(nil), MetadataIndexPrefixV(dal.NewKeyBuilder(), ledger, ns, key, version)...)

	return append(prefix, encoded...)
}

type ev = struct {
	entity string
	seq    uint64
	op     byte
}

func collect(t *testing.T, s *Store, prefix []byte, pin uint64) []string {
	t.Helper()

	it, err := NewEventResolveIterator(s.DB(), prefix, pin)
	require.NoError(t, err)
	defer it.Close()

	var out []string
	for it.Next() {
		out = append(out, string(it.Current()))
	}

	require.NoError(t, it.Err())

	return out
}

// The worked example from the design: E gets red@10, transitions to blue@25
// (ADD in blue's range + DEL in red's range), and is unset@40. Each pin
// resolves entirely inside one value's range.
func TestEventResolveIterator_TransitionTimeline(t *testing.T) {
	t.Parallel()

	s, redPrefix := eventFixture(t, "red",
		ev{"E", 10, MetadataEventAdd},
		ev{"E", 25, MetadataEventDel},
	)

	kb := dal.NewKeyBuilder()
	require.NoError(t, s.DB().Set(MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte("blue"), []byte("E"), 25, MetadataEventAdd), nil, pebble.NoSync))
	require.NoError(t, s.DB().Set(MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte("blue"), []byte("E"), 40, MetadataEventDel), nil, pebble.NoSync))
	bluePrefix := eventValuePrefix("l", NamespaceAccount, "k", 1, []byte("blue"))

	require.Empty(t, collect(t, s, redPrefix, 5), "before the first ADD")
	require.Equal(t, []string{"E"}, collect(t, s, redPrefix, 15), "red between 10 and 25")
	require.Empty(t, collect(t, s, redPrefix, 30), "red tombstoned at 25")
	require.Equal(t, []string{"E"}, collect(t, s, bluePrefix, 30), "blue between 25 and 40")
	require.Empty(t, collect(t, s, bluePrefix, 45), "blue unset at 40")
}

// A value that comes back: the group holds two intervals and each pin lands
// in the right one.
func TestEventResolveIterator_ReAddHistory(t *testing.T) {
	t.Parallel()

	s, prefix := eventFixture(t, "v",
		ev{"E", 10, MetadataEventAdd},
		ev{"E", 20, MetadataEventDel},
		ev{"E", 30, MetadataEventAdd},
	)

	require.Equal(t, []string{"E"}, collect(t, s, prefix, 15))
	require.Empty(t, collect(t, s, prefix, 25))
	require.Equal(t, []string{"E"}, collect(t, s, prefix, 35))
}

// Prefix-nested entity names must form disjoint groups — the NUL terminator
// keeps "a"'s seq bytes from interleaving with "ab"'s events.
func TestEventResolveIterator_PrefixNestedEntities(t *testing.T) {
	t.Parallel()

	s, prefix := eventFixture(t, "v",
		ev{"a", 10, MetadataEventAdd},
		ev{"ab", 10, MetadataEventAdd},
		ev{"ab", 20, MetadataEventDel},
	)

	require.Equal(t, []string{"a", "ab"}, collect(t, s, prefix, 15))
	require.Equal(t, []string{"a"}, collect(t, s, prefix, 25), "ab's group resolves dead without touching a's")
}

// Absolute-seek contract: repeatable, backward after exhaustion, and dead
// groups are skipped on the way.
func TestEventResolveIterator_SeekContract(t *testing.T) {
	t.Parallel()

	s, prefix := eventFixture(t, "v",
		ev{"a:1", 10, MetadataEventAdd},
		ev{"a:2", 10, MetadataEventAdd},
		ev{"a:2", 20, MetadataEventDel},
		ev{"a:3", 10, MetadataEventAdd},
	)

	it, err := NewEventResolveIterator(s.DB(), prefix, 25)
	require.NoError(t, err)
	defer it.Close()

	require.True(t, it.SeekGE([]byte("a:2")), "lands on the first LIVE entity >= target")
	require.Equal(t, "a:3", string(it.Current()), "a:2 is dead at pin 25")
	require.True(t, it.SeekGE([]byte("a:2")), "repeatable")
	require.Equal(t, "a:3", string(it.Current()))
	require.False(t, it.Next())

	require.True(t, it.SeekGE([]byte("a:0")), "backward after exhaustion")
	require.Equal(t, "a:1", string(it.Current()))
	require.NoError(t, it.Err())
}

// The floor memoises a proven-empty seek so a composite re-seeking an
// exhausted leaf does not re-resolve every group to reach the same verdict.
// It must bound only what the failed seek proved: nothing at or beyond that
// target, and nothing at all about smaller ones.
func TestEventResolveIterator_SeekFloor(t *testing.T) {
	t.Parallel()

	s, prefix := eventFixture(t, "v",
		ev{"a:1", 10, MetadataEventAdd},
		ev{"a:5", 10, MetadataEventAdd},
		ev{"a:5", 20, MetadataEventDel},
	)

	it, err := NewEventResolveIterator(s.DB(), prefix, 25)
	require.NoError(t, err)

	defer it.Close()

	// a:5 is dead at this pin and nothing lives beyond it, so the seek is
	// proven empty and the bound is recorded.
	require.False(t, it.SeekGE([]byte("a:5")))
	require.True(t, it.floor.covers([]byte("a:5")), "the failed seek must be memoised")
	require.True(t, it.floor.covers([]byte("a:9")), "a higher target is proven empty by the same failure")

	// Nothing below the bound is covered: the live group at a:1 is still
	// reachable, which is the property a latch would break.
	require.False(t, it.floor.covers([]byte("a:0")))
	require.True(t, it.SeekGE([]byte("a:0")))
	require.Equal(t, "a:1", string(it.Current()))

	// And the memoised verdict is still the right one on a re-seek.
	require.False(t, it.SeekGE([]byte("a:5")))
	require.NoError(t, it.Err())
}

// A seek that ends in a storage fault proves nothing about the keyspace, so it
// must not be memoised — otherwise a transient fault turns into a permanent
// empty result for every later seek at or above that target.
func TestEventResolveIterator_SeekFloorIgnoresFaults(t *testing.T) {
	t.Parallel()

	s, prefix := eventFixture(t, "v", ev{"a:1", 10, MetadataEventAdd})

	it, err := NewEventResolveIterator(s.DB(), prefix, 25)
	require.NoError(t, err)

	defer it.Close()

	it.floor.fail([]byte("a:0"), errFaultProbe)
	require.False(t, it.floor.covers([]byte("a:0")), "a faulted seek must leave the floor unset")

	require.True(t, it.SeekGE([]byte("a:0")))
	require.Equal(t, "a:1", string(it.Current()))
}

var errFaultProbe = errors.New("probe: simulated storage fault")

// An op byte this package never writes is unreadable, not a DEL. Folding it
// into the `op == ADD` test would silently drop matching entities, so the
// query path refuses it the way it refuses any malformed key, and the GC
// leaves the group whole rather than reclaiming around bytes it cannot read.
func TestEventResolveIterator_RejectsUnknownOp(t *testing.T) {
	t.Parallel()

	s, prefix := eventFixture(t, "v", ev{"a:1", 10, MetadataEventAdd})

	kb := dal.NewKeyBuilder()
	corrupt := append([]byte(nil), MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte("v"), []byte("a:2"), 20, 0x7f)...)
	require.NoError(t, s.DB().Set(corrupt, nil, pebble.NoSync))

	it, err := NewEventResolveIterator(s.DB(), prefix, 25)
	require.NoError(t, err)

	defer it.Close()

	// Iteration stops with an error rather than reporting a membership it
	// cannot vouch for — the scan reaches the corrupt key while walking the
	// preceding group, so it surfaces before any row is trusted.
	for it.Next() {
	}

	require.Error(t, it.Err(), "an unknown op must be reported, not read as a delete")

	// The GC must preserve the whole group, not just the unreadable event:
	// a:3's ADD is superseded by its DEL and would ordinarily be reclaimed,
	// but the unreadable event between them may be what re-added it.
	// The key builder reuses its buffer, so each key is copied before the next.
	event := func(seq uint64, op byte) []byte {
		return append([]byte(nil), MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte("v"), []byte("a:3"), seq, op)...)
	}
	survivors := [][]byte{
		event(5, MetadataEventAdd),
		event(6, 0x7f),
		event(7, MetadataEventDel),
	}
	for _, key := range survivors {
		require.NoError(t, s.DB().Set(key, nil, pebble.NoSync))
	}

	_, _, err = GCEventZone(s.DB(), PrefixMetadataIndex, nil, 1_000, 1<<20)
	require.NoError(t, err)

	for i, key := range append(survivors, corrupt) {
		_, closer, getErr := s.DB().Get(key)
		require.NoError(t, getErr, "event %d of an unreadable group must survive the sweep", i)

		require.NoError(t, closer.Close())
	}
}

// The deferred reclamation must not cost the GC its actual job: a group with
// no unreadable event still collapses to its latest below-watermark ADD.
func TestGCEventZone_ReclaimsReadableGroups(t *testing.T) {
	t.Parallel()

	s, _ := eventFixture(t, "v",
		ev{"a:1", 5, MetadataEventAdd},
		ev{"a:1", 6, MetadataEventDel},
		ev{"a:1", 7, MetadataEventAdd},
	)

	pruned, _, err := GCEventZone(s.DB(), PrefixMetadataIndex, nil, 1_000, 1<<20)
	require.NoError(t, err)
	require.Equal(t, 2, pruned, "the two superseded events are reclaimed")

	kb := dal.NewKeyBuilder()
	live := MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte("v"), []byte("a:1"), 7, MetadataEventAdd)
	_, closer, err := s.DB().Get(live)
	require.NoError(t, err, "the latest below-watermark ADD decides membership and must survive")
	require.NoError(t, closer.Close())
}

// Introspection reports statistics, so a key it cannot read must fail the scan
// rather than be counted around: a plausible cardinality computed over the
// events surrounding corruption hides the corruption it was computed over.
func TestInspectIndex_RejectsUnknownOp(t *testing.T) {
	t.Parallel()

	s, _ := eventFixture(t, "v", ev{"a:1", 10, MetadataEventAdd})

	kb := dal.NewKeyBuilder()
	corrupt := MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte("v"), []byte("a:2"), 20, 0x7f)
	require.NoError(t, s.DB().Set(corrupt, nil, pebble.NoSync))

	for _, mode := range []InspectMode{InspectDistinctValuesMode, InspectFacetsMode, InspectSummaryMode} {
		_, err := InspectIndex(InspectParams{
			Reader:      s.DB(),
			KB:          kb,
			LedgerName:  "l",
			Namespace:   NamespaceAccount,
			MetadataKey: "k",
			Version:     1,
			Mode:        mode,
		})
		require.Error(t, err, "mode %d must refuse to report statistics over an unreadable event", mode)
	}
}
