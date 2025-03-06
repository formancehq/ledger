package runner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func ShouldReceive[T any](t *testing.T, expected T, ch <-chan T) {
	t.Helper()

	require.Eventually(t, func() bool {
		select {
		case item := <-ch:
			require.Equal(t, expected, item)
			return true
		default:
			return false
		}
	}, time.Second, 20*time.Millisecond)
}
