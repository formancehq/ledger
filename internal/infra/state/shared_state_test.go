package state

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSharedStateDefaults(t *testing.T) {
	t.Parallel()

	ss := NewSharedState()
	require.False(t, ss.MaintenanceMode())
	require.False(t, ss.RequireSignatures())
}

func TestSharedStateMaintenanceMode(t *testing.T) {
	t.Parallel()

	ss := NewSharedState()
	require.False(t, ss.MaintenanceMode())

	ss.SetMaintenanceMode(true)
	require.True(t, ss.MaintenanceMode())

	ss.SetMaintenanceMode(false)
	require.False(t, ss.MaintenanceMode())
}

func TestSharedStateRequireSignatures(t *testing.T) {
	t.Parallel()

	ss := NewSharedState()
	require.False(t, ss.RequireSignatures())

	ss.SetRequireSignatures(true)
	require.True(t, ss.RequireSignatures())

	ss.SetRequireSignatures(false)
	require.False(t, ss.RequireSignatures())
}

func TestSharedStateReset(t *testing.T) {
	t.Parallel()

	ss := NewSharedState()
	ss.SetMaintenanceMode(true)
	ss.SetRequireSignatures(true)

	require.True(t, ss.MaintenanceMode())
	require.True(t, ss.RequireSignatures())

	ss.Reset()

	require.False(t, ss.MaintenanceMode())
	require.False(t, ss.RequireSignatures())
}

func TestSharedStateConcurrency(t *testing.T) {
	t.Parallel()

	ss := NewSharedState()

	var wg sync.WaitGroup

	const goroutines = 50

	// Concurrent writers
	wg.Add(goroutines * 2)

	for i := range goroutines {
		go func(enabled bool) {
			defer wg.Done()

			ss.SetMaintenanceMode(enabled)
		}(i%2 == 0)
		go func(enabled bool) {
			defer wg.Done()

			ss.SetRequireSignatures(enabled)
		}(i%2 == 0)
	}

	// Concurrent readers
	wg.Add(goroutines * 2)

	for range goroutines {
		go func() {
			defer wg.Done()

			_ = ss.MaintenanceMode()
		}()
		go func() {
			defer wg.Done()

			_ = ss.RequireSignatures()
		}()
	}

	wg.Wait()
	// No race condition or deadlock means success
}
