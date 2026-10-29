package httptransport

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

type wrappedTransport struct{ http.RoundTripper }

func TestCloneDefaultReturnsPrivateTransport(t *testing.T) {
	t.Parallel()

	rt, owned := CloneDefault()
	require.True(t, owned)
	require.IsType(t, &http.Transport{}, rt)
	require.NotSame(t, http.DefaultTransport, rt)
}

func TestCloneKeepsUncloneableRoundTripperShared(t *testing.T) {
	t.Parallel()

	shared := wrappedTransport{RoundTripper: http.DefaultTransport}
	rt, owned := clone(shared)
	require.False(t, owned)
	require.Equal(t, shared, rt)
}
