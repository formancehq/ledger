package readstore

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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

func TestInspectIndex_ResolvesMembershipAtMainHorizon(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()

	encoded := func(value *commonpb.MetadataValue) []byte {
		return EncodeMetadataValue(nil, value)
	}
	stringValue := func(value string) *commonpb.MetadataValue {
		return &commonpb.MetadataValue{
			Type: &commonpb.MetadataValue_StringValue{StringValue: value},
		}
	}
	nullValue := &commonpb.MetadataValue{
		Type: &commonpb.MetadataValue_NullValue{NullValue: &commonpb.NullValue{}},
	}
	putMetadata := func(value *commonpb.MetadataValue, entity string, seq uint64, op byte) {
		require.NoError(t, s.DB().Set(
			MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, encoded(value), []byte(entity), seq, op),
			nil,
			pebble.NoSync,
		))
	}
	putExists := func(isNull bool, entity string, seq uint64, op byte) {
		require.NoError(t, s.DB().Set(
			EntityExistsEventKeyV(kb, "l", NamespaceAccount, "k", 1, isNull, []byte(entity), seq, op),
			nil,
			pebble.NoSync,
		))
	}

	gold := stringValue("gold")
	silver := stringValue("silver")

	// At H=20 all three entities are gold and non-null. The projection view
	// also contains their post-H mutations: a:1 becomes silver and a:3 becomes
	// null. Inspect must reconstruct H rather than report the projection head.
	for _, entity := range []string{"a:1", "a:2", "a:3"} {
		putMetadata(gold, entity, 10, MetadataEventAdd)
		putExists(false, entity, 10, MetadataEventAdd)
	}
	putMetadata(gold, "a:1", 30, MetadataEventDel)
	putMetadata(silver, "a:1", 30, MetadataEventAdd)
	putMetadata(gold, "a:3", 30, MetadataEventDel)
	putMetadata(nullValue, "a:3", 30, MetadataEventAdd)
	putExists(false, "a:3", 30, MetadataEventDel)
	putExists(true, "a:3", 30, MetadataEventAdd)

	base := InspectParams{
		Reader:          s.DB(),
		KB:              kb,
		LedgerName:      "l",
		Namespace:       NamespaceAccount,
		MetadataKey:     "k",
		Version:         1,
		HorizonSequence: 20,
	}

	distinct, err := InspectIndex(func() InspectParams {
		params := base
		params.Mode = InspectDistinctValuesMode

		return params
	}())
	require.NoError(t, err)
	require.Len(t, distinct.Values, 1)
	require.Equal(t, "gold", distinct.Values[0].GetStringValue())

	facets, err := InspectIndex(func() InspectParams {
		params := base
		params.Mode = InspectFacetsMode

		return params
	}())
	require.NoError(t, err)
	require.Len(t, facets.Facets, 1)
	require.Equal(t, "gold", facets.Facets[0].Value.GetStringValue())
	require.Equal(t, uint64(3), facets.Facets[0].Count)

	summary, err := InspectIndex(func() InspectParams {
		params := base
		params.Mode = InspectSummaryMode

		return params
	}())
	require.NoError(t, err)
	require.Equal(t, uint64(1), summary.Cardinality)
	require.Equal(t, "gold", summary.Min.GetStringValue())
	require.Equal(t, "gold", summary.Max.GetStringValue())
	require.Equal(t, uint64(3), summary.EntitiesWithKey)
	require.Zero(t, summary.EntitiesWithNull)
}

// The unreadable event can arrive after the group has already been judged:
// an event at or above the watermark settles the pending one mid-group, and
// only a later key reveals the group cannot be read. Reclamation must still
// be withheld, so judging and reclaiming have to be separate steps.
func TestGCEventZone_PreservesAGroupCorruptedAfterItsWatermarkEvent(t *testing.T) {
	t.Parallel()

	const watermark = 100

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()

	event := func(seq uint64, op byte) []byte {
		return append([]byte(nil), MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte("v"), []byte("a:1"), seq, op)...)
	}

	// Two condemnable events below the watermark, then one above it (which
	// settles them), then the unreadable one.
	keys := [][]byte{
		event(5, MetadataEventAdd),
		event(7, MetadataEventDel),
		event(150, MetadataEventAdd),
		event(160, 0x7f),
	}
	for _, key := range keys {
		require.NoError(t, s.DB().Set(key, nil, pebble.NoSync))
	}

	pruned, _, err := GCEventZone(s.DB(), PrefixMetadataIndex, nil, watermark, 1<<20)
	require.NoError(t, err)
	require.Zero(t, pruned, "an unreadable group is preserved whole, whenever the unreadable event appears")

	for i, key := range keys {
		_, closer, getErr := s.DB().Get(key)
		require.NoError(t, getErr, "event %d must survive: the group is unreadable", i)
		require.NoError(t, closer.Close())
	}
}

// A truncated key sorts BEFORE the events it shares an identity with, since
// it is their prefix. Its own identity cannot be parsed, so it lands between
// two groups with no way to tell which one it came from: both must be
// preserved, or the group it actually belongs to is reclaimed around it.
func TestGCEventZone_PreservesTheGroupAfterAMalformedKey(t *testing.T) {
	t.Parallel()

	const watermark = 100

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()

	event := func(entity string, seq uint64, op byte) []byte {
		return append([]byte(nil), MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte("v"), []byte(entity), seq, op)...)
	}

	// a:1 is condemnable on its own: an ADD superseded by a DEL, both below
	// the watermark.
	keys := [][]byte{event("a:1", 5, MetadataEventAdd), event("a:1", 7, MetadataEventDel)}

	// The malformed key is a:1's own key with its tail cut off, so it sorts
	// ahead of both — and its terminator is no longer where the layout says.
	truncated := append([]byte(nil), keys[0][:len(keys[0])-3]...)
	require.NoError(t, s.DB().Set(truncated, nil, pebble.NoSync))

	for _, key := range keys {
		require.NoError(t, s.DB().Set(key, nil, pebble.NoSync))
	}

	pruned, _, err := GCEventZone(s.DB(), PrefixMetadataIndex, nil, watermark, 1<<20)
	require.NoError(t, err)
	require.Zero(t, pruned, "the group behind an unattributable key must be preserved")

	for i, key := range keys {
		_, closer, getErr := s.DB().Get(key)
		require.NoError(t, getErr, "event %d must survive the malformed key that precedes it", i)
		require.NoError(t, closer.Close())
	}
}

// The mark an unattributable key raises lives in memory, so a budgeted pass
// that ends at the very boundary it was carried across must not resume past
// that key: the next pass would have no reason left to preserve the group and
// would reclaim it, which is the same loss the mark exists to prevent.
func TestGCEventZone_CarriesTheUnsafeMarkAcrossABudgetedResume(t *testing.T) {
	t.Parallel()

	const watermark = 100

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()

	event := func(entity string, seq uint64, op byte) []byte {
		return append([]byte(nil), MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte("v"), []byte(entity), seq, op)...)
	}

	first := [][]byte{event("a:0", 5, MetadataEventAdd), event("a:0", 7, MetadataEventDel)}
	second := [][]byte{event("a:1", 5, MetadataEventAdd), event("a:1", 7, MetadataEventDel)}

	// Truncated a:1 key: sorts ahead of a:1's events, identity unparseable.
	truncated := append([]byte(nil), second[0][:len(second[0])-3]...)

	for _, key := range append(append(append([][]byte{}, first...), truncated), second...) {
		require.NoError(t, s.DB().Set(key, nil, pebble.NoSync))
	}

	// Budget 3 ends the pass exactly at the a:0 -> a:1 boundary, which is
	// where the mark is handed to a:1.
	pruned, next, err := GCEventZone(s.DB(), PrefixMetadataIndex, nil, watermark, 3)
	require.NoError(t, err)
	require.Zero(t, pruned, "a:0 borders the unattributable key too, so it is preserved as well")
	require.NotNil(t, next, "the pass stopped on budget")

	// Whatever the resume point is, the second pass must still preserve a:1.
	_, _, err = GCEventZone(s.DB(), PrefixMetadataIndex, next, watermark, 1<<20)
	require.NoError(t, err)

	for i, key := range second {
		_, closer, getErr := s.DB().Get(key)
		require.NoError(t, getErr, "a:1 event %d must survive a resume across the mark", i)
		require.NoError(t, closer.Close())
	}
}

// The unreadable key can be the first of a group rather than one reached
// while scanning it — a distinct parse site, and the one a truncated key
// lands on, since it sorts ahead of the events it shares an identity with.
func TestEventResolveIterator_RejectsAnUnreadableGroupHead(t *testing.T) {
	t.Parallel()

	s, prefix := eventFixture(t, "v", ev{"a:9", 10, MetadataEventAdd})

	kb := dal.NewKeyBuilder()
	head := append([]byte(nil), MetadataIndexEventKeyV(kb, "l", NamespaceAccount, "k", 1, []byte("v"), []byte("a:1"), 10, MetadataEventAdd)...)
	require.NoError(t, s.DB().Set(head[:len(head)-3], nil, pebble.NoSync))

	it, err := NewEventResolveIterator(s.DB(), prefix, 25)
	require.NoError(t, err)

	defer it.Close()

	for it.Next() {
	}

	require.Error(t, it.Err(), "a group whose first key cannot be parsed must fail the scan")
}
