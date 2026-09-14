package agentvalidationenv

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

func TestGoCacheClockRollbackForcesBudgetCheck(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	cacheRoot := filepath.Join(root, "cache")
	first := captureGoCache(t, root, cacheRoot, root, filepath.Join(root, "first"))
	require.NoError(t, os.WriteFile(filepath.Join(first, "oversized"), make([]byte, 2<<20), 0o644))
	timestamp := filepath.Join(cacheRoot, "go-build-generations", "check-timestamp")
	require.NoError(t, os.WriteFile(timestamp, fmt.Appendf(nil, "%d\n", time.Now().Add(24*time.Hour).Unix()), 0o644))
	command := testenv.Command(t, "bash", validationEnvPath(t), filepath.Join(root, "second"), "sh", "-c", `printf %s "$GOCACHE"`)
	command.Env = testenv.Environment("HOME="+root, "LEDGER_AI_CACHE_ROOT="+cacheRoot,
		"LEDGER_AI_GOCACHE_MAX_MIB=1", "LEDGER_AI_GOCACHE_CHECK_INTERVAL_SECONDS=300")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.NotEqual(t, first, string(output))
	require.NoDirExists(t, first)
}

func TestEphemeralWorkloadLeaseSurvivesSupervisorKill(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	cacheRoot := filepath.Join(root, "cache")
	ready := filepath.Join(root, "ready")
	done := filepath.Join(root, "done")
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, "bash", validationEnvPath(t), "--ephemeral", "sh", "-c",
		`printf '%s\n%s\n' "$GOCACHE" "$$" >"$1"; IFS= read -r release <&3; printf done >"$2"`, "holder", ready, done)
	command.Env = append(cacheBudgetEnvironment(root, cacheRoot), "TMPDIR="+root)
	command.ExtraFiles = []*os.File{reader}
	require.NoError(t, command.Start())
	t.Cleanup(func() { _ = command.Process.Kill() })
	require.NoError(t, reader.Close())
	var first string
	var childPID int
	require.Eventually(t, func() bool {
		data, readErr := os.ReadFile(ready)
		parts := strings.Split(strings.TrimSpace(string(data)), "\n")
		if readErr != nil || len(parts) != 2 {
			return false
		}
		first = parts[0]
		_, scanErr := fmt.Sscanf(parts[1], "%d", &childPID)

		return scanErr == nil
	}, 10*time.Second, 10*time.Millisecond)
	child, err := os.FindProcess(childPID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = child.Kill() })
	require.NoError(t, command.Process.Kill())
	require.Error(t, command.Wait())
	require.NoError(t, os.WriteFile(filepath.Join(first, "oversized"), make([]byte, 2<<20), 0o644))
	second := captureGoCache(t, root, cacheRoot, root, filepath.Join(root, "second"))
	require.NotEqual(t, first, second)
	require.DirExists(t, first)
	_, err = writer.WriteString("release\n")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := os.Stat(done)

		return err == nil
	}, 10*time.Second, 10*time.Millisecond)
}

func TestGoCacheResumesInterruptedRetirement(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	cacheRoot := filepath.Join(root, "cache")
	first := captureGoCache(t, root, cacheRoot, root, filepath.Join(root, "first"))
	retired := filepath.Join(cacheRoot, "go-build-generations", "retired-gen-1-2-3-4")
	require.NoError(t, os.MkdirAll(filepath.Join(retired, "payload"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(retired, "marker"), []byte("ledger-ai-go-cache-generation-v1\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(retired, "owner"), []byte("999999999\nunknown\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(retired, "payload", "leftover"), []byte("partial deletion"), 0o644))
	second := captureGoCache(t, root, cacheRoot, root, filepath.Join(root, "second"))
	require.Equal(t, first, second)
	require.NoDirExists(t, retired)
}

func TestSlowCacheMaintenanceDoesNotHoldSelectionLock(t *testing.T) {
	for _, stage := range []string{"du", "find"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			cacheRoot := filepath.Join(root, "cache")
			first := captureGoCache(t, root, cacheRoot, root, filepath.Join(root, "first"))
			require.NoError(t, os.WriteFile(filepath.Join(first, "oversized"), make([]byte, 2<<20), 0o644))
			bin := filepath.Join(root, "bin")
			require.NoError(t, os.Mkdir(bin, 0o755))
			realTool, err := exec.LookPath(stage)
			require.NoError(t, err)
			ready := filepath.Join(root, "ready")
			require.NoError(t, syscall.Mkfifo(ready, 0o600))
			// The peer starts only after maintenance reaches the blocking operation.
			// It reports ready only after its initializer completes, so the shared
			// supervisor cannot release maintenance while the peer is still blocked.
			script := "#!/usr/bin/env bash\nset -eu\nprintf 'ready\\n' >\"$MAINTENANCE_READY\"\nprintf 'ready\\n' >&3\nIFS= read -r release <&4\nexec \"$MAINTENANCE_TOOL\" \"$@\"\n"
			require.NoError(t, os.WriteFile(filepath.Join(bin, stage), []byte(script), 0o755))
			command := testenv.Command(t, "bash", validationEnvPath(t), filepath.Join(root, "slow"), "true")
			command.Env = append(cacheBudgetEnvironment(root, cacheRoot), "PATH="+bin+":"+os.Getenv("PATH"), "MAINTENANCE_READY="+ready, "MAINTENANCE_TOOL="+realTool)
			peer := testenv.Command(t, "bash", "-c",
				`set -eu; IFS= read -r ready <"$1"; bash "$2" "$3" true; printf 'ready\n' >&3; IFS= read -r release <&4`,
				"peer", ready, validationEnvPath(t), filepath.Join(root, "peer"))
			peer.Env = cacheBudgetEnvironment(root, cacheRoot)
			_, err = testenv.RunSynchronized(t, 30*time.Second,
				testenv.SynchronizedCommand{Name: "maintenance-" + stage, Command: command},
				testenv.SynchronizedCommand{Name: "peer", Command: peer},
			)
			require.NoError(t, err)
		})
	}
}
