package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// OrIterator.Seek must re-establish the union after exhaustion — the union
// mirror of TestAndIterator_SeekRepositionsAllChildren.
func TestOrIterator_SeekRepositionsAfterExhaustion(t *testing.T) {
	t.Parallel()

	it := NewOrIterator(newAliasingIter("a", "c"), newAliasingIter("b"))
	defer it.Close()

	var got []string
	for it.Next() {
		got = append(got, string(it.Current()))
	}
	require.Equal(t, []string{"a", "b", "c"}, got)

	require.True(t, it.Seek([]byte("b")), "reposition after exhaustion")
	require.Equal(t, "b", string(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, "c", string(it.Current()))
	require.False(t, it.Next())

	require.True(t, it.Seek([]byte("a")), "backward absolute seek")
	require.Equal(t, "a", string(it.Current()))
	require.NoError(t, it.Err())
}

// ReverseOrIterator.Seek must re-establish the union after exhaustion —
// the descending mirror of the OR test above.
func TestReverseOrIterator_SeekRepositionsAfterExhaustion(t *testing.T) {
	t.Parallel()

	it := NewReverseOrIterator(newReverseAliasingIter("a", "c"), newReverseAliasingIter("b"))
	defer it.Close()

	var got []string
	for it.Next() {
		got = append(got, string(it.Current()))
	}
	require.Equal(t, []string{"c", "b", "a"}, got)

	require.True(t, it.Seek([]byte("b")), "reposition after exhaustion")
	require.Equal(t, "b", string(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, "a", string(it.Current()))
	require.False(t, it.Next())

	require.True(t, it.Seek([]byte("c")), "forward absolute seek")
	require.Equal(t, "c", string(it.Current()))
	require.NoError(t, it.Err())
}
