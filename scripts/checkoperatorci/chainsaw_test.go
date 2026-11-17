package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

func TestOperatorChainsawPrerequisite(t *testing.T) {
	t.Parallel()

	justPath, err := exec.LookPath("just")
	require.NoError(t, err)
	shellPath, err := exec.LookPath("sh")
	require.NoError(t, err)
	justfilePath, err := filepath.Abs(filepath.Join("..", "..", "misc", "operator", "justfile"))
	require.NoError(t, err)

	for _, provided := range []bool{false, true} {
		name := "missing"
		if provided {
			name = "provided"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			toolDirectory := t.TempDir()
			invocationPath := filepath.Join(toolDirectory, "unexpected-invocation")
			toolScript := "#!" + shellPath + "\nprintf '%s\\n' invoked > \"$TEST_INVOCATION\"\nexit 1\n"
			require.NoError(t, os.WriteFile(filepath.Join(toolDirectory, "go"), []byte(toolScript), 0o755))
			if provided {
				require.NoError(t, os.WriteFile(filepath.Join(toolDirectory, "chainsaw"), []byte(toolScript), 0o755))
			}

			command := exec.CommandContext(t.Context(), justPath, "--no-dotenv", "--shell", shellPath, "--justfile", justfilePath, "_ensure-chainsaw")
			command.Env = testenv.Environment("PATH="+toolDirectory, "TEST_INVOCATION="+invocationPath)
			output, err := command.CombinedOutput()
			if provided {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
				require.Contains(t, string(output), "chainsaw is required")
				require.Contains(t, string(output), "nix develop .#upgrade")
			}
			require.NoFileExists(t, invocationPath, "the prerequisite must not install or execute tools")
		})
	}
}

func TestOperatorE2ERecipesPreserveDependencies(t *testing.T) {
	t.Parallel()

	justfile, err := os.ReadFile(filepath.Join("..", "..", "misc", "operator", "justfile"))
	require.NoError(t, err)
	require.Contains(t, string(justfile), "\ntest-e2e: e2e-cluster-up _ensure-chainsaw\n    just _e2e-run-tests\n")
	require.Contains(t, string(justfile), "\ntest-e2e-only: _ensure-chainsaw\n    just _e2e-run-tests\n")
}
