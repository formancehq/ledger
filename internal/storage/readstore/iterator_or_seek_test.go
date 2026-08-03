package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// OrIterator.SeekGE must re-establish the union after exhaustion — the union
// mirror of TestAndIterator_SeekGERepositionsAllChildren.
func TestOrIterator_SeekGERepositionsAfterExhaustion(t *testing.T) {
	t.Parallel()

	it := NewOrIterator(newAliasingIter("a", "c"), newAliasingIter("b"))
	defer it.Close()

	var got []string
	for it.Next() {
		got = append(got, string(it.Current()))
	}
	require.Equal(t, []string{"a", "b", "c"}, got)

	require.True(t, it.SeekGE([]byte("b")), "reposition after exhaustion")
	require.Equal(t, "b", string(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, "c", string(it.Current()))
	require.False(t, it.Next())

	require.True(t, it.SeekGE([]byte("a")), "backward absolute seek")
	require.Equal(t, "a", string(it.Current()))
	require.NoError(t, it.Err())
}
