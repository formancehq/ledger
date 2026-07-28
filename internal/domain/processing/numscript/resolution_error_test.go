package numscript

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDependencyResolutionError(t *testing.T) {
	t.Parallel()

	t.Run("errors.As recovers the wrapper and flag", func(t *testing.T) {
		t.Parallel()
		cause := errors.New("boom")
		err := error(&DependencyResolutionError{Cause: cause, MutableReadAttempted: true})

		var dre *DependencyResolutionError
		require.True(t, errors.As(err, &dre))
		require.True(t, dre.MutableReadAttempted)
	})

	t.Run("Unwrap keeps errors.Is transparent to the cause", func(t *testing.T) {
		t.Parallel()
		sentinel := errors.New("sentinel cause")
		err := error(&DependencyResolutionError{Cause: sentinel, MutableReadAttempted: false})
		require.ErrorIs(t, err, sentinel)
		require.Equal(t, sentinel.Error(), err.Error())
	})
}
