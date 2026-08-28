//go:build s3

package coldstorage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Neither test below uses t.Parallel(): both need exclusive control of the
// process-wide s3UploadSem to deterministically observe its blocking
// behavior, and unlike every other test in this package neither needs
// MinIO/Docker.

// fillAllUploadSlots acquires every s3UploadSlots slot and returns their
// release funcs, so tests exercise "one more than capacity blocks" without
// hardcoding s3UploadSlots' current value.
func fillAllUploadSlots(t *testing.T) []func() {
	t.Helper()

	releases := make([]func(), 0, s3UploadSlots)

	for range s3UploadSlots {
		release, err := AcquireS3UploadSlot(context.Background())
		require.NoError(t, err)

		releases = append(releases, release)
	}

	return releases
}

func TestAcquireS3UploadSlot_SerializesAndRespectsContext(t *testing.T) {
	releases := fillAllUploadSlots(t)

	acquired := make(chan func())
	go func() {
		release, _ := AcquireS3UploadSlot(context.Background())
		acquired <- release
	}()

	require.Never(t, func() bool {
		select {
		case <-acquired:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond,
		"an acquire beyond s3UploadSlots must not succeed while every slot is held")

	// Release exactly one slot; the waiter must take it.
	releases[0]()

	var extra func()

	require.Eventually(t, func() bool {
		select {
		case extra = <-acquired:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond,
		"acquire must succeed once a slot is released")

	extra()

	for _, release := range releases[1:] {
		release()
	}
}

func TestAcquireS3UploadSlot_ContextCanceledWhileWaiting(t *testing.T) {
	releases := fillAllUploadSlots(t)
	defer func() {
		for _, release := range releases {
			release()
		}
	}()

	waitCtx, cancel := context.WithCancel(context.Background())

	result := make(chan error, 1)
	go func() {
		_, err := AcquireS3UploadSlot(waitCtx)
		result <- err
	}()

	cancel()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("acquire did not return after context cancellation")
	}
}
