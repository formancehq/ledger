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

func TestAcquireS3UploadSlot_SerializesAndRespectsContext(t *testing.T) {
	release1, err := AcquireS3UploadSlot(context.Background())
	require.NoError(t, err)

	acquired := make(chan func())
	go func() {
		release2, _ := AcquireS3UploadSlot(context.Background())
		acquired <- release2
	}()

	require.Never(t, func() bool {
		select {
		case <-acquired:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond,
		"second acquire must not succeed while the first slot is still held")

	release1()

	var release2 func()

	require.Eventually(t, func() bool {
		select {
		case release2 = <-acquired:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond,
		"second acquire must succeed once the first slot is released")

	release2()
}

func TestAcquireS3UploadSlot_ContextCanceledWhileWaiting(t *testing.T) {
	release, err := AcquireS3UploadSlot(context.Background())
	require.NoError(t, err)

	defer release()

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
