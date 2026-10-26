package generation

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunOnceSuccess(t *testing.T) {
	t.Parallel()
	called := 0
	require.NoError(t, RunOnce(func(s *Source) {
		called++
		require.Equal(t, 0, s.Intn(1))
		require.Equal(t, uint64(math.MaxUint64), s.Uint64Range(math.MaxUint64, math.MaxUint64))
	}))
	require.Equal(t, 1, called)
}

func TestRunOncePanic(t *testing.T) {
	t.Parallel()
	for _, value := range []any{"episode failed", errors.New("episode error"), nil} {
		t.Run("panic", func(t *testing.T) {
			t.Parallel()
			called := 0
			err := RunOnce(func(s *Source) {
				called++
				s.Intn(10)
				panic(value)
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), "generation: panic:")
			if cause, ok := value.(error); ok {
				require.ErrorIs(t, err, cause)
			}
			require.Equal(t, 1, called, "Hegel failure retries/replay must not repeat user effects")
		})
	}
}

func TestRunOnceHegelFailure(t *testing.T) {
	t.Parallel()
	called := 0
	err := RunOnce(func(s *Source) {
		called++
		s.tc.Errorf("native callback failure")
	})
	require.ErrorContains(t, err, "native callback failure")
	require.Equal(t, 1, called)
}

func TestRunOnceNativeDrawFailure(t *testing.T) {
	t.Parallel()
	called := 0
	err := RunOnce(func(s *Source) {
		called++
		// A non-finite event value is rejected by the native engine.
		s.tc.EventValue("invalid", math.NaN())
	})
	require.Error(t, err)
	require.Equal(t, 1, called)
}

func TestRunOnceNilCallback(t *testing.T) {
	t.Parallel()
	require.ErrorContains(t, RunOnce(nil), "nil callback")
}

func TestRunOnceConcurrentIndependentRuns(t *testing.T) {
	t.Parallel()
	for range 8 {
		t.Run("episode", func(t *testing.T) {
			t.Parallel()
			called := 0
			err := RunOnce(func(s *Source) {
				called++
				for range 32 {
					require.Less(t, s.Intn(11), 11)
					require.Equal(t, "only", Choice(s, []string{"only"}))
				}
			})
			require.NoError(t, err)
			require.Equal(t, 1, called)
		})
	}
}
