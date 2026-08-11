package readstore

import (
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
