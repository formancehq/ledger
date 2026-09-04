package dal

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func testLogger() logging.Logger {
	return logging.FromContext(logging.TestingContext())
}

func TestOpenPebbleWithRetry_RetriesTemporarilyUnavailable(t *testing.T) {
	t.Parallel()

	attempts := 0
	delays := make([]time.Duration, 0, 2)

	db, err := openPebbleWithRetry(
		"unused",
		&pebble.Options{},
		testLogger(),
		func(_ string, _ *pebble.Options) (*pebble.DB, error) {
			attempts++
			if attempts < 3 {
				return nil, fmt.Errorf("opening lock: %w", syscall.EAGAIN)
			}

			return nil, nil
		},
		func(delay time.Duration) {
			delays = append(delays, delay)
		},
	)

	require.NoError(t, err)
	require.Nil(t, db)
	require.Equal(t, 3, attempts)
	require.Equal(t, []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}, delays)
}

func TestOpenPebbleWithRetry_DoesNotRetryOtherErrors(t *testing.T) {
	t.Parallel()

	expected := errors.New("permission denied")
	attempts := 0

	_, err := openPebbleWithRetry(
		"unused",
		&pebble.Options{},
		testLogger(),
		func(_ string, _ *pebble.Options) (*pebble.DB, error) {
			attempts++

			return nil, expected
		},
		func(time.Duration) {
			t.Fatal("sleep called for a non-retryable error")
		},
	)

	require.ErrorIs(t, err, expected)
	require.Equal(t, 1, attempts)
}

func TestOpenPebbleWithRetry_StopsAfterBoundedBackoff(t *testing.T) {
	t.Parallel()

	attempts := 0
	delays := make([]time.Duration, 0, pebbleOpenMaxRetries)

	_, err := openPebbleWithRetry(
		"unused",
		&pebble.Options{},
		testLogger(),
		func(_ string, _ *pebble.Options) (*pebble.DB, error) {
			attempts++

			return nil, syscall.EAGAIN
		},
		func(delay time.Duration) {
			delays = append(delays, delay)
		},
	)

	require.ErrorIs(t, err, syscall.EAGAIN)
	require.Equal(t, pebbleOpenMaxRetries+1, attempts)
	require.Len(t, delays, pebbleOpenMaxRetries)
	require.Equal(t, 26*time.Second+300*time.Millisecond, sumDurations(delays))
	require.Equal(t, pebbleOpenMaxRetryDelay, delays[len(delays)-1])
}

func TestRetryablePebbleOpenError_RecognizesRealLockContention(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "db")
	db, err := pebble.Open(path, &pebble.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	cmd := exec.Command(os.Args[0], "-test.run=^TestRetryablePebbleOpenError_HelperProcess$")
	cmd.Env = append(os.Environ(), "PEBBLE_LOCK_TEST_PATH="+path)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
}

func TestRetryablePebbleOpenError_HelperProcess(t *testing.T) {
	t.Parallel()

	path := os.Getenv("PEBBLE_LOCK_TEST_PATH")
	if path == "" {
		return
	}

	_, err := pebble.Open(path, &pebble.Options{})
	require.Error(t, err)
	require.True(t, isRetryablePebbleOpenError(err), "expected EAGAIN-shaped Pebble lock error, got %v", err)
}

func sumDurations(durations []time.Duration) time.Duration {
	var total time.Duration
	for _, duration := range durations {
		total += duration
	}

	return total
}
