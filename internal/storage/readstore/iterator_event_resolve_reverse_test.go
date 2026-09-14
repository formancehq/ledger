package readstore

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestReverseEventResolveIterator_PinnedHistories(t *testing.T) {
	t.Parallel()
	s, prefix := eventFixture(t, "v",
		ev{"a", 10, MetadataEventAdd}, ev{"a", 20, MetadataEventDel}, ev{"a", 30, MetadataEventAdd},
		ev{"ab", 10, MetadataEventAdd}, ev{"ab", 20, MetadataEventDel},
		ev{"b", 20, MetadataEventDel}, ev{"b", 20, MetadataEventAdd},
		ev{"z", 40, MetadataEventAdd},
	)
	for _, tc := range []struct {
		pin  uint64
		want []string
	}{
		{0, nil}, {10, []string{"ab", "a"}}, {20, []string{"b"}}, {30, []string{"b", "a"}}, {40, []string{"z", "b", "a"}},
	} {
		t.Run(strconv.FormatUint(tc.pin, 10), func(t *testing.T) {
			it, err := NewReverseEventResolveIterator(s.DB(), prefix, tc.pin)
			require.NoError(t, err)
			defer it.Close()
			var got []string
			for it.Next() {
				got = append(got, string(it.Current()))
			}
			require.Equal(t, tc.want, got)
			require.NoError(t, it.Err())
			require.False(t, it.Next())
		})
	}
}

func TestReverseEventResolveIterator_AbsoluteSeek(t *testing.T) {
	t.Parallel()
	s, prefix := eventFixture(t, "v", ev{"a", 10, MetadataEventAdd}, ev{"ab", 10, MetadataEventAdd},
		ev{"b", 10, MetadataEventAdd}, ev{"b", 20, MetadataEventDel}, ev{"c", 10, MetadataEventAdd})
	it, err := NewReverseEventResolveIterator(s.DB(), prefix, 25)
	require.NoError(t, err)
	defer it.Close()
	for _, tc := range []struct{ target, want string }{{"z", "c"}, {"b", "ab"}, {"b", "ab"}, {"a", "a"}} {
		require.True(t, it.Seek([]byte(tc.target)))
		require.Equal(t, tc.want, string(it.Current()))
	}
	require.False(t, it.Next())
	require.True(t, it.Seek([]byte("c")), "reposition after Next exhaustion")
	require.Equal(t, "c", string(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, "ab", string(it.Current()))
	require.False(t, it.Seek([]byte("0")))
	require.False(t, it.Next())
	require.False(t, it.Seek(nil), "a lower target remains empty")
	require.True(t, it.Seek([]byte("a")), "failed seek must not hide larger targets")
	require.Equal(t, "a", string(it.Current()))
	require.NoError(t, it.Err())
}

func TestReverseEventResolveIterator_Empty(t *testing.T) {
	t.Parallel()
	s, prefix := eventFixture(t, "v")
	it, err := NewReverseEventResolveIterator(s.DB(), prefix, 25)
	require.NoError(t, err)
	defer it.Close()
	require.False(t, it.Next())
	require.False(t, it.Seek([]byte("z")))
	require.False(t, it.Seek([]byte("zz")))
	require.False(t, it.Next())
	require.NoError(t, it.Err())
}

func TestReverseEventResolveIterator_MalformedKeys(t *testing.T) {
	t.Parallel()
	for _, corruption := range []string{"unknown operation", "truncated tail", "missing terminator"} {
		for _, seek := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/seek=%t", corruption, seek), func(t *testing.T) {
				t.Parallel()
				s, prefix := eventFixture(t, "v", ev{"a", 10, MetadataEventAdd})
				key := append([]byte(nil), MetadataIndexEventKeyV(dal.NewKeyBuilder(), "l", NamespaceAccount, "k", 1, []byte("v"), []byte("z"), 20, MetadataEventAdd)...)
				switch corruption {
				case "unknown operation":
					key[len(key)-1] = 0x7f
				case "truncated tail":
					key = key[:len(key)-3]
				case "missing terminator":
					key[len(key)-metadataEventSuffixLen-1] = 2
				}
				require.NoError(t, s.DB().Set(key, nil, pebble.NoSync))
				it, err := NewReverseEventResolveIterator(s.DB(), prefix, 25)
				require.NoError(t, err)
				defer it.Close()
				if seek {
					require.False(t, it.Seek([]byte("zz")))
				} else {
					require.False(t, it.Next())
				}
				require.EqualError(t, it.Err(), fmt.Sprintf("malformed metadata event key %x", key))
				require.False(t, it.Next())
				require.False(t, it.Seek([]byte("a")), "malformed event errors remain sticky")
			})
		}
	}
}

func TestReverseEventResolveIterator_MalformedEarlierEvent(t *testing.T) {
	t.Parallel()
	s, prefix := eventFixture(t, "v", ev{"a", 10, MetadataEventAdd}, ev{"z", 30, MetadataEventAdd})
	key := append([]byte(nil), MetadataIndexEventKeyV(dal.NewKeyBuilder(), "l", NamespaceAccount, "k", 1, []byte("v"), []byte("z"), 20, 0x7f)...)
	require.NoError(t, s.DB().Set(key, nil, pebble.NoSync))
	it, err := NewReverseEventResolveIterator(s.DB(), prefix, 30)
	require.NoError(t, err)
	defer it.Close()
	require.False(t, it.Next(), "an already decided ADD cannot hide corruption earlier in its group")
	require.EqualError(t, it.Err(), fmt.Sprintf("malformed metadata event key %x", key))
}
