package readstore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// The filter must skip rejected entities on both Next and SeekGE while
// preserving the absolute-seek contract: a seek lands on the first ADMITTED
// entity >= target, repeatably, including after exhaustion.
func TestFilterIterator_SkipsAndKeepsSeekContract(t *testing.T) {
	t.Parallel()

	keepEven := func(e []byte) (bool, error) {
		return (e[0]-'a')%2 == 0, nil // admits a, c, e
	}

	it := NewFilterIterator(newAliasingIter("a", "b", "c", "d", "e"), keepEven)
	defer it.Close()

	var got []string
	for it.Next() {
		got = append(got, string(it.Current()))
	}
	require.Equal(t, []string{"a", "c", "e"}, got)
	require.NoError(t, it.Err())

	require.True(t, it.SeekGE([]byte("b")), "seek lands on the first admitted entity >= target")
	require.Equal(t, "c", string(it.Current()))
	require.True(t, it.SeekGE([]byte("b")), "repeated seek is non-consuming")
	require.Equal(t, "c", string(it.Current()))

	require.True(t, it.SeekGE([]byte("a")), "backward seek after exhaustion")
	require.Equal(t, "a", string(it.Current()))

	require.False(t, it.SeekGE([]byte("f")), "nothing admitted at or beyond target")
	require.NoError(t, it.Err())
}

func TestFilterIterator_KeepErrorLatches(t *testing.T) {
	t.Parallel()

	probeErr := errors.New("probe failed")
	it := NewFilterIterator(newAliasingIter("a", "b"), func(e []byte) (bool, error) {
		if string(e) == "b" {
			return false, probeErr
		}

		return true, nil
	})
	defer it.Close()

	require.True(t, it.Next())
	require.Equal(t, "a", string(it.Current()))
	require.False(t, it.Next(), "keep error stops iteration")
	require.ErrorIs(t, it.Err(), probeErr)
	require.False(t, it.SeekGE([]byte("a")), "error is sticky, like a storage error")
	require.ErrorIs(t, it.Err(), probeErr)
}
