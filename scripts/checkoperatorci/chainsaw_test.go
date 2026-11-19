package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

	for _, testCase := range []struct {
		name       string
		hostBinary bool
		pinnedPath bool
		shell      bool
		wantPass   bool
	}{
		{name: "missing"},
		{name: "host binary only", hostBinary: true},
		{name: "host binary and shell marker", hostBinary: true, shell: true},
		{name: "pinned path without shell marker", pinnedPath: true},
		{name: "pinned path in upgrade shell", pinnedPath: true, shell: true, wantPass: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			toolDirectory := t.TempDir()
			invocationPath := filepath.Join(toolDirectory, "unexpected-invocation")
			toolScript := "#!" + shellPath + "\nprintf '%s\\n' invoked > \"$TEST_INVOCATION\"\nexit 1\n"
			require.NoError(t, os.WriteFile(filepath.Join(toolDirectory, "go"), []byte(toolScript), 0o755))
			if testCase.hostBinary {
				require.NoError(t, os.WriteFile(filepath.Join(toolDirectory, "chainsaw"), []byte(toolScript), 0o755))
			}
			pinnedPath := ""
			if testCase.pinnedPath {
				pinnedPath = filepath.Join(toolDirectory, "pinned-chainsaw")
				require.NoError(t, os.WriteFile(pinnedPath, []byte(toolScript), 0o755))
			}
			shellMarker := ""
			if testCase.shell {
				shellMarker = "1"
			}

			command := exec.CommandContext(t.Context(), justPath, "--no-dotenv", "--shell", shellPath, "--justfile", justfilePath, "_ensure-chainsaw")
			command.Env = testenv.Environment("PATH="+toolDirectory, "TEST_INVOCATION="+invocationPath, "LEDGER_UPGRADE_SHELL="+shellMarker, "LEDGER_CHAINSAW_BIN="+pinnedPath)
			output, err := command.CombinedOutput()
			if testCase.wantPass {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
				require.Contains(t, string(output), "pinned chainsaw is required")
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
	require.Contains(t, string(justfile), "\ntest-e2e: _ensure-chainsaw e2e-cluster-up\n    just _e2e-run-tests\n")
	require.Contains(t, string(justfile), "\ntest-e2e-only: _ensure-chainsaw\n    just _e2e-run-tests\n")
	require.Equal(t, 2, strings.Count(string(justfile), `"$LEDGER_CHAINSAW_BIN" test`))
	justPath, err := exec.LookPath("just")
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), justPath, "--no-dotenv", "--justfile", filepath.Join("..", "..", "misc", "operator", "justfile"), "--dry-run", "test-e2e")
	command.Env = testenv.Environment()
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	prerequisiteIndex := strings.Index(string(output), "pinned chainsaw is required")
	clusterBuildIndex := strings.Index(string(output), "docker build")
	require.GreaterOrEqual(t, prerequisiteIndex, 0, string(output))
	require.GreaterOrEqual(t, clusterBuildIndex, 0, string(output))
	require.Less(t, prerequisiteIndex, clusterBuildIndex, string(output))

	flake, err := os.ReadFile(filepath.Join("..", "..", "flake.nix"))
	require.NoError(t, err)
	require.Contains(t, string(flake), `LEDGER_CHAINSAW_BIN = "${pkgs.kyverno-chainsaw}/bin/chainsaw";`)
}
