package readstore

import (
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestReversePrefixIteratorStampVisibility(t *testing.T) {
	for _, seek := range []bool{false, true} {
		name := "next"
		if seek {
			name = "seek"
		}
		t.Run(name, func(t *testing.T) {
			s := newTestStore(t)
			prefix := []byte("stamp/")
			for entity, stamp := range map[string]uint64{"a": 9, "b": 11, "c": 10, "d": 12} {
				require.NoError(t, s.DB().Set(append(append([]byte(nil), prefix...), entity...), binary.BigEndian.AppendUint64(nil, stamp), pebble.NoSync))
			}
			it, err := NewStampGatedReversePrefixIterator(s.DB(), prefix, len(prefix), 0, 10)
			require.NoError(t, err)
			defer it.Close()
			if seek {
				require.True(t, it.Seek([]byte("d")))
			} else {
				require.True(t, it.Next())
			}
			require.Equal(t, "c", string(it.Current()), "admit the pin itself and reject a newer first row")
			require.True(t, it.Next())
			require.Equal(t, "a", string(it.Current()), "skip newer rows between visible rows")
			require.False(t, it.Next())
			require.NoError(t, it.Err())
			require.True(t, it.Seek([]byte("b")))
			require.Equal(t, "a", string(it.Current()), "reposition after exhaustion while applying the gate")
		})
	}
}

func TestReversePrefixIteratorMalformedStamp(t *testing.T) {
	for _, seek := range []bool{false, true} {
		name := "next"
		if seek {
			name = "seek"
		}
		t.Run(name, func(t *testing.T) {
			s := newTestStore(t)
			prefix := []byte("stamp/")
			require.NoError(t, s.DB().Set([]byte("stamp/b"), []byte{1}, pebble.NoSync))
			require.NoError(t, s.DB().Set([]byte("stamp/a"), binary.BigEndian.AppendUint64(nil, 1), pebble.NoSync))
			it, err := NewStampGatedReversePrefixIterator(s.DB(), prefix, len(prefix), 0, 10)
			require.NoError(t, err)
			defer it.Close()
			if seek {
				require.False(t, it.Seek([]byte("b")))
			} else {
				require.False(t, it.Next())
			}
			require.ErrorContains(t, it.Err(), "1-byte value")
			require.False(t, it.Next(), "malformed values must latch exhaustion")
			require.ErrorContains(t, it.Err(), "1-byte value")
			ungated, err := NewStampGatedReversePrefixIterator(s.DB(), prefix, len(prefix), 0, 0)
			require.NoError(t, err)
			defer ungated.Close()
			require.True(t, ungated.Next(), "pin zero disables value validation")
			require.Equal(t, "b", string(ungated.Current()))
			require.NoError(t, ungated.Err())
		})
	}
}
