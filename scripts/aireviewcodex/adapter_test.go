package aireviewcodex_test

import (
	"errors"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/scripts/internal/testenv"
)

const (
	testHead        = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testBase        = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testMergeBase   = "cccccccccccccccccccccccccccccccccccccccc"
	testFingerprint = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
)

type adapterFixture struct {
	repositoryRoot     string
	adapterPath        string
	temporaryDirectory string
	userHome           string
	userCodexHome      string
	resultPath         string
	targetPath         string
	promptCapture      string
	argumentsCapture   string
	environmentCapture string
	authCapture        string
	homeCapture        string
	validationRunDir   string
	knownFindingsPath  string
	reviewContextPath  string
	invocationCount    string
	substitutionMarker string
	path               string
}

func TestAdapterFramesOneExactFinalReviewWithoutPersonalState(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	output, err := runAdapter(t, fixture, nil)
	require.NoError(t, err, output)

	prompt := readFile(t, fixture.promptCapture)
	require.Contains(t, prompt, "Copy the trusted `head` and `worktree_fingerprint`")
	require.Contains(t, prompt, "one exact final technical review")
	require.NotContains(t, prompt, "PREVIOUS-REVIEW")
	require.NotContains(t, prompt, "bounded review/fix loop")
	require.Contains(t, prompt, "`git diff --find-renames "+testMergeBase+" "+testHead+" --`")
	require.NoFileExists(t, fixture.substitutionMarker)

	arguments := strings.Fields(readFile(t, fixture.argumentsCapture))
	for _, expected := range []string{
		"--ephemeral",
		"--ignore-user-config",
		"--ignore-rules",
		"--sandbox",
		"read-only",
		"--output-schema",
	} {
		require.Contains(t, arguments, expected)
	}
	for _, disabled := range []string{"hooks", "plugins", "apps", "memories", "multi_agent", "skill_search"} {
		require.Contains(t, arguments, disabled)
	}

	require.Equal(t, "personal_skill=false\nconfig=false\nrules=false\nplugins=false\nskills=false\n", readFile(t, fixture.environmentCapture))
	require.Equal(t, "auth-canary\n", readFile(t, fixture.authCapture))
	isolatedHome := strings.TrimSpace(readFile(t, fixture.homeCapture))
	require.NotEqual(t, fixture.userHome, isolatedHome)
	require.NoFileExists(t, filepath.Join(isolatedHome, ".agents", "skills", "personal-canary", "SKILL.md"))
	require.NoFileExists(t, filepath.Join(isolatedHome, ".codex", "config.toml"))
	require.NoDirExists(t, isolatedHome)
	require.FileExists(t, fixture.resultPath)
}

func TestAdapterPropagatesCodexFailure(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	output, err := runAdapter(t, fixture, map[string]string{"FAKE_CODEX_EXIT": "23"})
	require.Error(t, err, output)
	var exitError *exec.ExitError
	require.True(t, errors.As(err, &exitError))
	require.Equal(t, 23, exitError.ExitCode())
}

func TestAdapterRejectsMissingCodexResult(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	output, err := runAdapter(t, fixture, map[string]string{"FAKE_CODEX_NO_RESULT": "1"})
	require.Error(t, err)
	require.Contains(t, output, "Codex exited successfully but did not produce")
}

func TestAdapterUsesTargetManifestForUntrackedFiles(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	output, err := runAdapter(t, fixture, nil)
	require.NoError(t, err, output)

	prompt := readFile(t, fixture.promptCapture)
	require.Contains(t, prompt, "`untracked_paths` array of `AI_REVIEW_CHANGE_TARGET`")
	require.NotContains(t, prompt, "git ls-files --others --exclude-standard")
}

func TestAdapterSuppliesKnownFindingsToTheSingleTechnicalReview(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	output, err := runAdapter(t, fixture, map[string]string{
		"AI_REVIEW_KNOWN_FINDINGS":      fixture.knownFindingsPath,
		"AI_REVIEW_KNOWN_FINDINGS_PR":   "123",
		"AI_REVIEW_KNOWN_FINDINGS_HEAD": testHead,
		"AI_REVIEW_CONTEXT":             fixture.reviewContextPath,
		"FAKE_KNOWN_STATUS":             "FIXED",
	})
	require.NoError(t, err, output)
	prompt := readFile(t, fixture.promptCapture)
	require.Contains(t, prompt, "UNTRUSTED GITHUB FINDINGS")
	require.Contains(t, prompt, "UNTRUSTED TASK AND PR CONTEXT")
	require.NotContains(t, prompt, "Untrusted GitHub claim")
	require.NotContains(t, prompt, "EN-9999 untrusted task")
	require.Equal(t, "1\n", readFile(t, fixture.invocationCount))
	require.Contains(t, readFile(t, fixture.resultPath), `"id":"github-review-thread-THREAD_1"`)
}

func TestAdapterRejectsOmittedKnownFinding(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	output, err := runAdapter(t, fixture, map[string]string{
		"AI_REVIEW_KNOWN_FINDINGS":      fixture.knownFindingsPath,
		"AI_REVIEW_KNOWN_FINDINGS_PR":   "123",
		"AI_REVIEW_KNOWN_FINDINGS_HEAD": testHead,
	})
	require.Error(t, err, output)
	require.Contains(t, output, "incomplete known-finding coverage")
	require.Equal(t, "1\n", readFile(t, fixture.invocationCount))
}

func TestAdapterRejectsInconsistentUntrackedManifest(t *testing.T) {
	t.Parallel()

	fixture := newAdapterFixture(t)
	target := strings.Replace(readFile(t, fixture.targetPath), `"untracked_paths": []`, `"untracked_paths": ["state.json"]`, 1)
	writeFile(t, fixture.targetPath, target, 0o644)

	output, err := runAdapter(t, fixture, nil)
	require.Error(t, err)
	require.Contains(t, output, "invalid change target contract")
}

func newAdapterFixture(t *testing.T) adapterFixture {
	t.Helper()

	repositoryRoot := strings.TrimSpace(runCommand(t, "", "git", "rev-parse", "--show-toplevel"))
	temporaryDirectory := filepath.Join(t.TempDir(), "fixture with spaces")
	require.NoError(t, os.MkdirAll(temporaryDirectory, 0o755))
	validationRunDir := filepath.Join(temporaryDirectory, "validation-run")
	require.NoError(t, os.MkdirAll(validationRunDir, 0o755))
	userHome := filepath.Join(temporaryDirectory, "user-home")
	userCodexHome := filepath.Join(userHome, ".codex")
	for _, directory := range []string{
		filepath.Join(userHome, ".agents", "skills", "personal-canary"),
		filepath.Join(userCodexHome, "rules"),
		filepath.Join(userCodexHome, "plugins"),
		filepath.Join(userCodexHome, "skills", "personal-canary"),
		filepath.Join(temporaryDirectory, "bin"),
		filepath.Join(temporaryDirectory, "tmp"),
	} {
		require.NoError(t, os.MkdirAll(directory, 0o755))
	}
	writeFile(t, filepath.Join(userHome, ".agents", "skills", "personal-canary", "SKILL.md"), "personal skill\n", 0o644)
	writeFile(t, filepath.Join(userCodexHome, "auth.json"), "auth-canary\n", 0o600)
	writeFile(t, filepath.Join(userCodexHome, "config.toml"), "model = \"personal-canary\"\n", 0o644)
	writeFile(t, filepath.Join(userCodexHome, "rules", "default.rules"), "personal rule\n", 0o644)
	writeFile(t, filepath.Join(userCodexHome, "plugins", "canary"), "personal plugin\n", 0o644)
	writeFile(t, filepath.Join(userCodexHome, "skills", "personal-canary", "SKILL.md"), "personal codex skill\n", 0o644)

	fixture := adapterFixture{
		repositoryRoot:     repositoryRoot,
		adapterPath:        filepath.Join(repositoryRoot, "scripts", "ai-review-codex"),
		temporaryDirectory: temporaryDirectory,
		userHome:           userHome,
		userCodexHome:      userCodexHome,
		resultPath:         filepath.Join(temporaryDirectory, "result.json"),
		targetPath:         filepath.Join(temporaryDirectory, "target.json"),
		promptCapture:      filepath.Join(temporaryDirectory, "prompt.txt"),
		argumentsCapture:   filepath.Join(temporaryDirectory, "arguments.txt"),
		environmentCapture: filepath.Join(temporaryDirectory, "environment.txt"),
		authCapture:        filepath.Join(temporaryDirectory, "auth.txt"),
		homeCapture:        filepath.Join(temporaryDirectory, "isolated-home.txt"),
		validationRunDir:   validationRunDir,
		knownFindingsPath:  filepath.Join(temporaryDirectory, "known-findings.json"),
		reviewContextPath:  filepath.Join(temporaryDirectory, "pr-metadata.json"),
		invocationCount:    filepath.Join(temporaryDirectory, "invocation-count"),
		substitutionMarker: filepath.Join(temporaryDirectory, "substitution-marker"),
		path:               filepath.Join(temporaryDirectory, "bin") + string(os.PathListSeparator) + os.Getenv("PATH"),
	}

	target := `{
  "kind": "BASE_COMPARISON",
  "base_ref": "explicit-base",
  "base_sha": "` + testBase + `",
  "merge_base_sha": "` + testMergeBase + `",
  "head": "` + testHead + `",
  "worktree_scope": {"staged": true, "unstaged": true, "untracked": true},
  "worktree_present": {"staged": false, "unstaged": false, "untracked": false},
  "untracked_paths": []
}
`
	writeFile(t, fixture.targetPath, target, 0o644)
	writeFile(t, fixture.knownFindingsPath, `{"version":1,"pr_number":123,"head":"`+testHead+`","review_decision":"CHANGES_REQUESTED","findings":[{"id":"github-review-thread-THREAD_1","kind":"unresolved-review-thread","url":"https://example/finding","author":"reviewer","path":"example.go","line":7,"original_line":7,"is_outdated":false,"body":"Untrusted GitHub claim"}]}`+"\n", 0o644)
	writeFile(t, fixture.reviewContextPath, `{"title":"EN-9999 untrusted task","body":"Task details"}`+"\n", 0o644)

	fakeCodex := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" >"$FAKE_ARGUMENTS_CAPTURE"
cat >"$FAKE_PROMPT_CAPTURE"
count=0
[[ ! -f "$FAKE_INVOCATION_COUNT" ]] || count=$(cat "$FAKE_INVOCATION_COUNT")
printf '%s\n' "$((count + 1))" >"$FAKE_INVOCATION_COUNT"
printf '%s\n' "$HOME" >"$FAKE_HOME_CAPTURE"
{
    if [[ -e "$HOME/.agents/skills/personal-canary/SKILL.md" ]]; then printf 'personal_skill=true\n'; else printf 'personal_skill=false\n'; fi
    if [[ -e "$CODEX_HOME/config.toml" ]]; then printf 'config=true\n'; else printf 'config=false\n'; fi
    if [[ -e "$CODEX_HOME/rules/default.rules" ]]; then printf 'rules=true\n'; else printf 'rules=false\n'; fi
    if [[ -e "$CODEX_HOME/plugins/canary" ]]; then printf 'plugins=true\n'; else printf 'plugins=false\n'; fi
    if [[ -e "$CODEX_HOME/skills/personal-canary/SKILL.md" ]]; then printf 'skills=true\n'; else printf 'skills=false\n'; fi
} >"$FAKE_ENVIRONMENT_CAPTURE"
cp "$CODEX_HOME/auth.json" "$FAKE_AUTH_CAPTURE"
if [[ -n "${FAKE_CODEX_EXIT:-}" ]]; then exit "$FAKE_CODEX_EXIT"; fi
if [[ "${FAKE_CODEX_NO_RESULT:-}" == "1" ]]; then exit 0; fi
known='[]'
if [[ -n "${FAKE_KNOWN_STATUS:-}" ]]; then
    known='[{"id":"github-review-thread-THREAD_1","status":"'"$FAKE_KNOWN_STATUS"'","reason":"verified against current code"}]'
fi
printf '{"decision":"APPROVE","head":"%s","worktree_fingerprint":"%s","known_findings":%s,"findings":[],"residual_risk":"LOW","human_decision_context":""}\n' "$AI_REVIEW_HEAD" "$AI_REVIEW_WORKTREE_FINGERPRINT" "$known" >"$AI_REVIEW_RESULT"
`
	writeFile(t, filepath.Join(temporaryDirectory, "bin", "codex"), fakeCodex, 0o755)
	fakeHead := "#!/usr/bin/env bash\nprintf 'invoked\\n' >\"$SUBSTITUTION_MARKER\"\n"
	writeFile(t, filepath.Join(temporaryDirectory, "bin", "head"), fakeHead, 0o755)

	return fixture
}

func runAdapter(t *testing.T, fixture adapterFixture, extraEnvironment map[string]string) (string, error) {
	t.Helper()
	expectedHead := strings.TrimSpace(runCommand(t, fixture.repositoryRoot, "git", "rev-parse", "HEAD"))

	environment := map[string]string{
		"HOME":                           fixture.userHome,
		"CODEX_HOME":                     fixture.userCodexHome,
		"PATH":                           fixture.path,
		"TMPDIR":                         filepath.Join(fixture.temporaryDirectory, "tmp"),
		"VALIDATION_RUN_DIR":             fixture.validationRunDir,
		"AI_REVIEW_RESULT":               fixture.resultPath,
		"AI_REVIEW_HEAD":                 testHead,
		"AI_REVIEW_WORKTREE_FINGERPRINT": testFingerprint,
		"AI_REVIEW_CHANGE_TARGET":        fixture.targetPath,
		"EXPECTED_PR_NUMBER":             "123",
		"EXPECTED_WORKTREE":              fixture.repositoryRoot,
		"EXPECTED_HEAD":                  expectedHead,
		"FAKE_PROMPT_CAPTURE":            fixture.promptCapture,
		"FAKE_ARGUMENTS_CAPTURE":         fixture.argumentsCapture,
		"FAKE_ENVIRONMENT_CAPTURE":       fixture.environmentCapture,
		"FAKE_AUTH_CAPTURE":              fixture.authCapture,
		"FAKE_HOME_CAPTURE":              fixture.homeCapture,
		"FAKE_INVOCATION_COUNT":          fixture.invocationCount,
		"SUBSTITUTION_MARKER":            fixture.substitutionMarker,
	}
	maps.Copy(environment, extraEnvironment)

	cmd := exec.Command("bash", fixture.adapterPath)
	cmd.Dir = fixture.repositoryRoot
	cmd.Env = testenv.EnvironmentMap(environment)
	output, err := cmd.CombinedOutput()

	return string(output), err
}

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), mode))
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(content)
}

func runCommand(t *testing.T, directory, name string, arguments ...string) string {
	t.Helper()
	cmd := testenv.Command(t, name, arguments...)
	if directory != "" {
		cmd.Dir = directory
	}
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	return string(output)
}
