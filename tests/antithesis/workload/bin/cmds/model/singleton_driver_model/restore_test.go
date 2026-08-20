package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFileTriggerWaitsForClaimedRestoreAfterCancellation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	reqPath := filepath.Join(dir, "restore.req")
	respPath := filepath.Join(dir, "restore.resp")
	trigger := &fileTrigger{reqPath: reqPath, respPath: respPath}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- trigger.Fire(ctx)
	}()

	claimedPath := reqPath + ".claimed"
	require.Eventually(t, func() bool {
		return os.Rename(reqPath, claimedPath) == nil
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, os.Remove(claimedPath))

	// Once the sidecar has claimed the request, the command deadline must not
	// release the driver while the cluster may be between teardown and restore.
	cancel()
	select {
	case err := <-result:
		t.Fatalf("claimed restore returned on cancellation: %v", err)
	case <-time.After(2 * restorePoll):
	}

	require.NoError(t, os.WriteFile(respPath, []byte("ok\n"), 0o600))
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("claimed restore did not finish after the orchestrator response")
	}
}
