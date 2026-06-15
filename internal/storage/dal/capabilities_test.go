package dal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSentinelFactory_Disabled_NoopRunDoesNotInvokeCallback pins the contract
// that a disabled SentinelFactory never calls its callback and returns nil.
// Callers therefore do not have to gate Run with an `if sentinelMode` check.
func TestSentinelFactory_Disabled_NoopRunDoesNotInvokeCallback(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	factory := NewSentinelFactory(s, false)

	called := false
	err := factory.Run(func(PebbleReader) error {
		called = true

		return errors.New("must not be invoked")
	})
	require.NoError(t, err, "disabled sentinel factory must return nil")
	require.False(t, called, "disabled sentinel factory must not invoke fn")
}

// TestSentinelFactory_Enabled_RunPassesSnapshotReader pins the contract that
// the enabled factory opens a snapshot read handle and passes it to fn.
func TestSentinelFactory_Enabled_RunPassesSnapshotReader(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	factory := NewSentinelFactory(s, true)

	var got PebbleReader
	err := factory.Run(func(r PebbleReader) error {
		got = r

		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, got, "enabled sentinel factory must materialise the reader")
}

// TestSentinelFactory_Enabled_PropagatesCallbackError pins error propagation
// from the callback through Run.
func TestSentinelFactory_Enabled_PropagatesCallbackError(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	factory := NewSentinelFactory(s, true)

	sentinel := errors.New("simulated check failure")
	err := factory.Run(func(PebbleReader) error {
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
}

// TestIncomingRestoreFactory_Run_CallbackErrorSkipsActivate pins the failure
// contract: if fn errors, the factory must not activate or restore — the
// staging directory is left for offline inspection and the original error
// surfaces.
func TestIncomingRestoreFactory_Run_CallbackErrorSkipsActivate(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	factory := NewIncomingRestoreFactory(s)

	fetchErr := errors.New("simulated fetch failure")
	checkpointID, err := factory.Run(func(stagingDir string) error {
		require.NotEmpty(t, stagingDir, "Run must hand a non-empty staging dir to fn")

		return fetchErr
	})

	require.ErrorIs(t, err, fetchErr, "Run must surface the callback error verbatim")
	require.Zero(t, checkpointID, "Run must not return a checkpoint id on callback error")

	// No checkpoint should have been activated; CurrentCheckpointID stays 0.
	require.Equal(t, uint64(0), s.GetCurrentCheckpointID(),
		"Run must not activate a checkpoint when fn errors")
}
