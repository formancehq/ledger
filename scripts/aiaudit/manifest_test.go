package aiaudit

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

func TestAPIBoundaryManifestContract(t *testing.T) {
	t.Parallel()

	const auditID = "api-boundary-contracts"
	repository := repositoryRoot(t)
	manifestPath := filepath.Join("docs", "technical", "audits", auditID+".json")
	contents, err := os.ReadFile(filepath.Join(repository, manifestPath))
	require.NoError(t, err)

	var manifest struct {
		Paths       []string `json:"paths"`
		RelatedDocs []string `json:"related_docs"`
	}
	require.NoError(t, json.Unmarshal(contents, &manifest))
	for _, pattern := range manifest.Paths {
		require.True(t, filepath.IsLocal(pattern), pattern)
		// The manifest uses literal paths or a trailing /**. filepath.Glob
		// checks a nonempty first level for those recursive scope directories.
		matches, err := filepath.Glob(filepath.Join(repository, pattern))
		require.NoError(t, err, pattern)
		require.NotEmpty(t, matches, pattern)
	}
	for _, path := range manifest.RelatedDocs {
		require.True(t, filepath.IsLocal(path), path)
		require.FileExists(t, filepath.Join(repository, path))
	}

	for _, valid := range []bool{true, false} {
		name := "accepted"
		if !valid {
			name = "malformed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fixture := newFixture(t)
			payload := contents
			if !valid {
				var fields map[string]any
				require.NoError(t, json.Unmarshal(contents, &fields))
				delete(fields, "invariants")
				encoded, marshalErr := json.Marshal(fields)
				require.NoError(t, marshalErr)
				payload = encoded
			}
			require.NoError(t, os.WriteFile(filepath.Join(fixture.checkout, manifestPath), payload, 0o600))

			// Leave the copied manifest untracked: the real launcher validates
			// its shape/id and stops at the clean-HEAD gate, before any provider.
			command := exec.Command("bash", filepath.Join(fixture.checkout, "scripts", "ai-audit"), auditID)
			command.Dir = fixture.checkout
			command.Env = testenv.Environment(
				"PATH="+fixture.fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"FAKE_CODEX_PROMPT_CAPTURE="+fixture.promptCapture,
			)
			output, runErr := command.CombinedOutput()
			require.Error(t, runErr, string(output))
			if valid {
				require.Contains(t, string(output), "repository worktree must be clean")
			} else {
				require.Contains(t, string(output), "invalid audit manifest")
			}
			require.NoFileExists(t, fixture.promptCapture)
			require.NoDirExists(t, filepath.Join(fixture.checkout, "build", "ai-audit"))
		})
	}
}
