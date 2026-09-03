package aipradoptcandidate

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdoptCandidateAcceptsLegitimateNonBugfix(t *testing.T) {
	t.Parallel()

	fixture := newAdoptionFixture(t, adoptionFixtureOptions{})
	output, exitCode := fixture.run(t, adoptionRun{})
	require.Equal(t, 0, exitCode, output)
	require.Contains(t, output, "AI_PR_ADOPT_RESULT: APPROVED_NOT_PUSHED")
	require.Equal(t, "1", fixture.count(t, "triage"))
	require.Equal(t, "1", fixture.count(t, "known-findings"))
	require.Equal(t, "1", fixture.count(t, "base-review"))
	require.Equal(t, "1", fixture.count(t, "reconciliation"))
	require.Equal(t, "1", fixture.count(t, "validation"))

	arguments := strings.Split(strings.TrimSpace(readFile(t, fixture.capture)), "\n")
	worktree := argumentValue(t, arguments, "--worktree")
	require.Equal(t, worktree, strings.TrimSpace(readFile(t, fixture.capture+".cwd")))
	require.Equal(t, "123", argumentValue(t, arguments, "--pr"))
	require.Equal(t, fixture.candidateSHA, argumentValue(t, arguments, "--expected-head"))
	require.NotEqual(t, fixture.checkout, worktree)
	resolvedCheckout, err := filepath.EvalSymlinks(fixture.checkout)
	require.NoError(t, err)
	require.Equal(t, resolvedCheckout, argumentValue(t, arguments, "--trusted-root"))
	require.Contains(t, argumentValue(t, arguments, "--review-cmd"), "trusted-tools/scripts/ai-review-known-findings")
	require.Contains(t, argumentValue(t, arguments, "--validation-tool-root"), "trusted-tools")
	require.NotContains(t, argumentValue(t, arguments, "--state-dir"), worktree)
	require.Contains(t, argumentValue(t, arguments, "--validation-gates-cmd"), "agent-check-pr --list")
	require.Equal(t, "123|"+fixture.headSHA, strings.TrimSpace(readFile(t, fixture.capture+".known-binding")))
}

func TestAdoptCandidateRefusesKnownBlockerMissedByFreshReview(t *testing.T) {
	t.Parallel()

	fixture := newAdoptionFixture(t, adoptionFixtureOptions{})
	known := `[{"id":"github-review-9","kind":"review-body","source_review_id":9,"source_id":9,"url":"https://github.com/owner/repo/pull/123#pullrequestreview-9","author":"reviewer","path":"","line":null,"body":"[P1][blocking] unresolved old blocker"}]`
	output, exitCode := fixture.run(t, adoptionRun{knownFindings: known})
	require.Equal(t, 2, exitCode, output)
	require.Contains(t, output, "AI_PR_ADOPT_RESULT: REFUSED (candidate was not approved)")
	require.Equal(t, "1", fixture.count(t, "base-review"), "fresh base review deliberately misses the old blocker")
	require.Equal(t, "1", fixture.count(t, "reconciliation"))
	require.Equal(t, "0", fixture.count(t, "validation"))
}

func TestAdoptCandidateEnforcesBugfixEvidence(t *testing.T) {
	t.Parallel()

	complete := "DISCOVERY: DIRECT_FIX\nBEFORE_FIX: BUG_REPRODUCED\nAFTER_FIX: PASS\n"
	tests := []struct {
		name        string
		body        string
		wantExit    int
		wantMessage string
	}{
		{name: "missing discovery", body: "BEFORE_FIX: BUG_REPRODUCED\nAFTER_FIX: PASS\n", wantExit: 1, wantMessage: "discovery classification missing"},
		{name: "missing before fix", body: "DISCOVERY: DIRECT_FIX\nAFTER_FIX: PASS\n", wantExit: 1, wantMessage: "BEFORE_FIX missing"},
		{name: "missing after fix", body: "DISCOVERY: DIRECT_FIX\nBEFORE_FIX: BUG_REPRODUCED\n", wantExit: 1, wantMessage: "AFTER_FIX missing"},
		{name: "complete", body: complete, wantExit: 0, wantMessage: "BUGFIX_EVIDENCE=PASS"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAdoptionFixture(t, adoptionFixtureOptions{})
			output, exitCode := fixture.run(t, adoptionRun{title: "fix: repair bug", body: test.body})
			require.Equal(t, test.wantExit, exitCode, output)
			require.Contains(t, output, test.wantMessage)
			if test.wantExit == 0 {
				require.Contains(t, output, "AI_PR_ADOPT_RESULT: APPROVED_NOT_PUSHED")
				require.Equal(t, "1", fixture.count(t, "validation"))
			} else {
				require.Equal(t, "0", fixture.count(t, "base-review"), "evidence must fail before review")
				require.Equal(t, "0", fixture.count(t, "validation"))
			}
		})
	}
}

func TestAdoptCandidateDoesNotRequireBugfixEvidenceForNonBugfix(t *testing.T) {
	t.Parallel()

	fixture := newAdoptionFixture(t, adoptionFixtureOptions{})
	output, exitCode := fixture.run(t, adoptionRun{title: "docs: clarify adoption"})
	require.Equal(t, 0, exitCode, output)
	require.Contains(t, output, "BUGFIX_EVIDENCE=NOT_REQUIRED")
}

func TestAdoptCandidateUsesNormalBugfixIntentSignals(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		body   string
		labels []string
	}{
		{name: "label", labels: []string{"bugfix"}},
		{name: "jira body", body: "Fixes EN-1920"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAdoptionFixture(t, adoptionFixtureOptions{})
			output, exitCode := fixture.run(t, adoptionRun{
				title: "chore: neutral title", body: test.body, labels: test.labels,
			})
			require.Equal(t, 1, exitCode, output)
			require.Contains(t, output, "discovery classification missing")
		})
	}
}

func TestAdoptCandidateRequiresBaselineClassificationWhenApplicable(t *testing.T) {
	t.Parallel()

	fixture := newAdoptionFixture(t, adoptionFixtureOptions{})
	body := "DISCOVERY: DIRECT_FIX\nBEFORE_FIX: BUG_REPRODUCED\nAFTER_FIX: PASS\n"
	output, exitCode := fixture.run(t, adoptionRun{title: "fix: repair bug", body: body, validationFailure: true})
	require.Equal(t, 1, exitCode, output)
	require.Contains(t, output, "baseline classification missing")

	fixture = newAdoptionFixture(t, adoptionFixtureOptions{})
	body += "BASELINE_CLASSIFICATION: BASELINE_FAILURE\n"
	output, exitCode = fixture.run(t, adoptionRun{title: "fix: repair bug", body: body, validationFailure: true})
	require.Equal(t, 0, exitCode, output)
	require.Contains(t, output, "BUGFIX_EVIDENCE=PASS")
}

func TestAdoptCandidateRefusesLegitimacyQuestion(t *testing.T) {
	t.Parallel()

	fixture := newAdoptionFixture(t, adoptionFixtureOptions{})
	output, exitCode := fixture.run(t, adoptionRun{triageDecision: "QUESTION"})
	require.Equal(t, 2, exitCode, output)
	require.Contains(t, output, "AI_PR_ADOPT_RESULT: HUMAN_DECISION_REQUIRED")
	require.Equal(t, "0", fixture.count(t, "known-findings"))
	require.Equal(t, "0", fixture.count(t, "base-review"))
}

func TestAdoptCandidateRefusesCandidateHeadMismatch(t *testing.T) {
	t.Parallel()

	fixture := newAdoptionFixture(t, adoptionFixtureOptions{})
	output, exitCode := fixture.run(t, adoptionRun{candidateSHA: fixture.baseSHA})
	require.Equal(t, 2, exitCode, output)
	require.Contains(t, output, "candidate is not a descendant of PR head")
	require.Equal(t, "0", fixture.count(t, "triage"))
}

func TestAdoptCandidatePreservesStaleBaseFailure(t *testing.T) {
	t.Parallel()

	fixture := newAdoptionFixture(t, adoptionFixtureOptions{})
	fixture.advanceBase(t)
	output, exitCode := fixture.run(t, adoptionRun{})
	require.Equal(t, 3, exitCode, output)
	require.Contains(t, output, "AI_PR_ADOPT_RESULT: BASE_UPDATE_REQUIRED")
	require.Equal(t, "0", fixture.count(t, "triage"))
}

func TestAdoptCandidateRequiresBaseUpdateBeforeResolvingNewSharedPolicy(t *testing.T) {
	t.Parallel()

	fixture := newAdoptionFixture(t, adoptionFixtureOptions{omitPreconditions: true})
	fixture.advanceBase(t)
	output, exitCode := fixture.run(t, adoptionRun{})
	require.Equal(t, 3, exitCode, output)
	require.Contains(t, output, "AI_PR_ADOPT_RESULT: BASE_UPDATE_REQUIRED")
	require.NotContains(t, output, "TOOLING_ERROR")
	require.NotContains(t, output, "trusted base lacks readable non-symlink scripts/ai-pr-publication-preconditions")
	require.Equal(t, "0", fixture.count(t, "triage"))
	require.Equal(t, "0", fixture.count(t, "base-review"))
}

func TestAdoptCandidateUsesBasePinnedGatesDespiteCandidateTampering(t *testing.T) {
	t.Parallel()

	fixture := newAdoptionFixture(t, adoptionFixtureOptions{tamperCandidateGates: true})
	output, exitCode := fixture.run(t, adoptionRun{
		title: "fix: candidate tries to bypass evidence",
		body:  "BEFORE_FIX: BUG_REPRODUCED\nAFTER_FIX: PASS\n",
	})
	require.Equal(t, 1, exitCode, output)
	require.Contains(t, output, "discovery classification missing")
	require.Equal(t, "1", fixture.count(t, "triage"), "the trusted base triage must run")
	require.Equal(t, "0", fixture.count(t, "base-review"))
}

func TestAdoptCandidateClassifiesGenuinelyMissingSharedPolicyAsToolingError(t *testing.T) {
	t.Parallel()

	fixture := newAdoptionFixture(t, adoptionFixtureOptions{omitPreconditions: true})
	output, exitCode := fixture.run(t, adoptionRun{})
	require.Equal(t, 1, exitCode, output)
	require.Contains(t, output, "AI_PR_ADOPT_RESULT: TOOLING_ERROR (missing publication preconditions)")
	require.Contains(t, output, "trusted base lacks readable non-symlink scripts/ai-pr-publication-preconditions")
	require.NotContains(t, output, "HUMAN_DECISION_REQUIRED")
	require.Equal(t, "0", fixture.count(t, "triage"))
	require.Equal(t, "0", fixture.count(t, "base-review"))
}

func TestAdoptCandidateDistrustsOldTrustArtifacts(t *testing.T) {
	t.Parallel()

	fixture := newAdoptionFixture(t, adoptionFixtureOptions{})
	artifactDir := filepath.Join(fixture.checkout, "build", "ai-pr-adopt-candidate")
	require.NoError(t, os.MkdirAll(artifactDir, 0o755))
	for _, name := range []string{"exact-review-approved", "validation-pass", "pre-commit-pass", "bot-approval"} {
		require.NoError(t, os.WriteFile(filepath.Join(artifactDir, name), []byte("old success\n"), 0o644))
	}

	output, exitCode := fixture.run(t, adoptionRun{})
	require.Equal(t, 0, exitCode, output)
	require.Equal(t, "1", fixture.count(t, "triage"))
	require.Equal(t, "1", fixture.count(t, "known-findings"))
	require.Equal(t, "1", fixture.count(t, "reconciliation"))
	require.Equal(t, "1", fixture.count(t, "validation"))
	require.Equal(t, "1", fixture.count(t, "pre-commit"), "normalization must run despite an old receipt")
}

func TestAdoptCandidateRerunsPreCommitImmediatelyBeforePush(t *testing.T) {
	t.Parallel()

	fixture := newAdoptionFixture(t, adoptionFixtureOptions{})
	output, exitCode := fixture.run(t, adoptionRun{push: true})
	require.Equal(t, 0, exitCode, output)
	require.Contains(t, output, "AI_PR_ADOPT_RESULT: PUSHED")
	require.Equal(t, "2", fixture.count(t, "pre-commit"), "normalization and last-mile pre-commit must both run")
	remoteHead := runGitOutput(t, fixture.root, "--git-dir", fixture.remote, "rev-parse", "refs/heads/feature")
	require.Equal(t, fixture.candidateSHA, remoteHead)
}

func TestAdoptCandidateRevalidatesBeforeAndAfterExactReview(t *testing.T) {
	t.Parallel()

	for _, stage := range []string{"before-exact-review", "after-exact-review"} {
		t.Run(stage, func(t *testing.T) {
			fixture := newAdoptionFixture(t, adoptionFixtureOptions{})
			output, exitCode := fixture.run(t, adoptionRun{targetMutation: stage})
			require.Equal(t, 3, exitCode, output)
			require.Contains(t, output, "BASE_REVALIDATION_CLASSIFICATION=ADVANCED")
			require.Contains(t, output, "AI_PR_ADOPT_RESULT: BASE_UPDATE_REQUIRED")
			require.NotContains(t, output, "AI_PR_ADOPT_RESULT: APPROVED_NOT_PUSHED")
			require.Contains(t, output, "CURRENT_CANDIDATE_SHA="+fixture.candidateSHA)
			require.Contains(t, output, "EXPECTED_BASE_SHA="+fixture.baseSHA)
			require.Contains(t, output, "OBSERVED_BASE_SHA="+fixture.advancedBaseSHA)
			candidateWorktree := preservedAdoptionWorktree(t, output)
			require.Equal(t, fixture.candidateSHA, runGitOutput(t, candidateWorktree, "rev-parse", "HEAD"))
		})
	}
}

func TestAdoptCandidateRevalidatesAfterLastPreCommitBeforePush(t *testing.T) {
	t.Parallel()

	fixture := newAdoptionFixture(t, adoptionFixtureOptions{})
	output, exitCode := fixture.run(t, adoptionRun{push: true, targetMutation: "during-final-precommit"})
	require.Equal(t, 3, exitCode, output)
	require.Contains(t, output, "AI_PR_ADOPT_RESULT: BASE_UPDATE_REQUIRED")
	require.NotContains(t, output, "AI_PR_ADOPT_RESULT: PUSHED")
	require.Equal(t, fixture.headSHA, runGitOutput(t, fixture.root, "--git-dir", fixture.remote, "rev-parse", "refs/heads/feature"))
	require.Equal(t, fixture.candidateSHA, runGitOutput(t, preservedAdoptionWorktree(t, output), "rev-parse", "HEAD"))
}

type adoptionRun struct {
	title             string
	body              string
	labels            []string
	knownFindings     string
	triageDecision    string
	candidateSHA      string
	push              bool
	validationFailure bool
	targetMutation    string
}

type adoptionFixture struct {
	root            string
	remote          string
	seed            string
	checkout        string
	fakeBin         string
	baseSHA         string
	headSHA         string
	candidateSHA    string
	advancedBaseSHA string
	capture         string
	countsDir       string
}

type adoptionFixtureOptions struct {
	tamperCandidateGates bool
	omitPreconditions    bool
}

func newAdoptionFixture(t *testing.T, options adoptionFixtureOptions) adoptionFixture {
	t.Helper()

	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	seed := filepath.Join(root, "seed")
	checkout := filepath.Join(root, "checkout")
	require.NoError(t, os.MkdirAll(filepath.Join(seed, "scripts"), 0o755))
	runGit(t, root, "init", "--bare", remote)
	runGit(t, seed, "init")
	runGit(t, seed, "config", "user.name", "Adoption Test")
	runGit(t, seed, "config", "user.email", "adoption@example.com")

	baseScripts := []string{"ai-pr-adopt-candidate", "ai-target-base-revalidate", "ai-bugfix-gate", "ai-git-guard"}
	if !options.omitPreconditions {
		baseScripts = append(baseScripts, "ai-pr-publication-preconditions")
	}
	for _, name := range baseScripts {
		copyFile(t, sourceScriptPath(t, name), filepath.Join(seed, "scripts", name), 0o755)
	}
	writeExecutable(t, filepath.Join(seed, "scripts", "ai-pr-triage"), `#!/usr/bin/env bash
set -euo pipefail
increment "$TEST_COUNTS_DIR/triage"
output=""
while [[ $# -gt 0 ]]; do
    case "$1" in --output) output=$2; shift 2 ;; *) shift ;; esac
done
printf '{"decision":"%s","base_sha":"%s","head":"%s"}\n' \
    "$TEST_TRIAGE_DECISION" "$AI_PR_TRIAGE_EXPECT_BASE_SHA" "$AI_PR_TRIAGE_EXPECT_HEAD_SHA" > "$output"
`)
	writeExecutable(t, filepath.Join(seed, "scripts", "ai-pr-known-findings"), `#!/usr/bin/env bash
set -euo pipefail
increment "$TEST_COUNTS_DIR/known-findings"
pr=$1
shift
head=""
output=""
while [[ $# -gt 0 ]]; do
    case "$1" in --head) head=$2; shift 2 ;; --output) output=$2; shift 2 ;; *) shift ;; esac
done
printf '{"version":1,"pr_number":%s,"head":"%s","findings":%s}\n' \
    "$pr" "$head" "$TEST_KNOWN_FINDINGS_JSON" > "$output"
`)
	writeExecutable(t, filepath.Join(seed, "scripts", "ai-review-codex"), `#!/usr/bin/env bash
set -euo pipefail
increment "$TEST_COUNTS_DIR/base-review"
`)
	writeExecutable(t, filepath.Join(seed, "scripts", "ai-review-known-findings"), `#!/usr/bin/env bash
set -euo pipefail
increment "$TEST_COUNTS_DIR/reconciliation"
bash "$(dirname "$0")/ai-review-codex"
[[ -n "${AI_REVIEW_KNOWN_FINDINGS:-}" && -f "$AI_REVIEW_KNOWN_FINDINGS" ]]
if [[ "$(jq '.findings | length' "$AI_REVIEW_KNOWN_FINDINGS")" -gt 0 ]]; then
    echo "known finding remains STILL_VALID" >&2
    exit 2
fi
`)
	writeExecutable(t, filepath.Join(seed, "scripts", "agent-check-pr"), `#!/usr/bin/env bash
set -euo pipefail
increment "$TEST_COUNTS_DIR/validation"
`)
	writeExecutable(t, filepath.Join(seed, "scripts", "agent-just"), `#!/usr/bin/env bash
set -euo pipefail
increment "$TEST_COUNTS_DIR/pre-commit"
precommit_count=$(<"$TEST_COUNTS_DIR/pre-commit")
if [[ "${TEST_TARGET_MUTATION:-}" == "before-exact-review" && "$precommit_count" == "1" ]] ||
   [[ "${TEST_TARGET_MUTATION:-}" == "during-final-precommit" && "$precommit_count" == "2" ]]; then
    git --git-dir="$TEST_REMOTE" update-ref refs/heads/release/v3.0 "$TEST_ADVANCED_BASE_SHA"
fi
`)
	writeExecutable(t, filepath.Join(seed, "scripts", "review-loop"), `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "$TEST_CAPTURE"
pwd > "$TEST_CAPTURE.cwd"
printf '%s|%s\n' "$AI_REVIEW_KNOWN_FINDINGS_PR" "$AI_REVIEW_KNOWN_FINDINGS_HEAD" > "$TEST_CAPTURE.known-binding"
review_cmd=""
validation_cmd=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --review-cmd) review_cmd=$2; shift 2 ;;
        --validation-cmd) validation_cmd=$2; shift 2 ;;
        *) shift ;;
    esac
done
bash -c "$review_cmd"
bash -c "$validation_cmd"
if [[ "${TEST_TARGET_MUTATION:-}" == "after-exact-review" ]]; then
    git --git-dir="$TEST_REMOTE" update-ref refs/heads/release/v3.0 "$TEST_ADVANCED_BASE_SHA"
fi
`)
	require.NoError(t, os.WriteFile(filepath.Join(seed, "base.txt"), []byte("base\n"), 0o644))
	runGit(t, seed, "add", ".")
	runGit(t, seed, "commit", "-m", "base")
	runGit(t, seed, "branch", "-M", "release/v3.0")
	baseSHA := runGitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "remote", "add", "origin", remote)
	runGit(t, seed, "push", "-u", "origin", "release/v3.0")
	require.NoError(t, os.WriteFile(filepath.Join(seed, "advanced-base.txt"), []byte("advanced base\n"), 0o644))
	runGit(t, seed, "add", "advanced-base.txt")
	runGit(t, seed, "commit", "-m", "advance target fixture")
	advancedBaseSHA := runGitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "push", "origin", "release/v3.0")
	runGit(t, seed, "reset", "--hard", baseSHA)
	runGit(t, seed, "push", "--force", "origin", "release/v3.0")

	runGit(t, seed, "switch", "-c", "feature")
	require.NoError(t, os.WriteFile(filepath.Join(seed, "feature.txt"), []byte("feature\n"), 0o644))
	runGit(t, seed, "add", "feature.txt")
	runGit(t, seed, "commit", "-m", "feature")
	headSHA := runGitOutput(t, seed, "rev-parse", "HEAD")
	runGit(t, seed, "push", "-u", "origin", "feature")

	runGit(t, root, "clone", "--branch", "release/v3.0", remote, checkout)
	runGit(t, checkout, "config", "user.name", "Adoption Test")
	runGit(t, checkout, "config", "user.email", "adoption@example.com")
	runGit(t, checkout, "fetch", "origin", "feature")
	runGit(t, checkout, "switch", "-c", "candidate", "origin/feature")
	require.NoError(t, os.WriteFile(filepath.Join(checkout, "fix.txt"), []byte("fix\n"), 0o644))
	if options.tamperCandidateGates {
		for _, name := range []string{
			"ai-pr-publication-preconditions", "ai-pr-triage", "ai-pr-known-findings", "ai-bugfix-gate",
			"ai-review-known-findings", "agent-check-pr", "agent-just", "review-loop",
		} {
			writeExecutable(t, filepath.Join(checkout, "scripts", name), "#!/usr/bin/env bash\nexit 0\n")
		}
	}
	runGit(t, checkout, "add", ".")
	runGit(t, checkout, "commit", "-m", "candidate")
	candidateSHA := runGitOutput(t, checkout, "rev-parse", "HEAD")
	runGit(t, checkout, "switch", "release/v3.0")

	fakeBin := filepath.Join(root, "bin")
	countsDir := filepath.Join(root, "counts")
	require.NoError(t, os.MkdirAll(fakeBin, 0o755))
	require.NoError(t, os.MkdirAll(countsDir, 0o755))
	writeExecutable(t, filepath.Join(fakeBin, "increment"), `#!/usr/bin/env bash
set -euo pipefail
path=$1
value=0
if [[ -f "$path" ]]; then value=$(<"$path"); fi
printf '%s\n' "$((value + 1))" > "$path"
`)
	writeExecutable(t, filepath.Join(fakeBin, "gh"), `#!/usr/bin/env bash
set -euo pipefail
case "$1 $2" in
    "repo view") printf 'owner/repo\n' ;;
    "pr view") printf '%s\n' "$TEST_PR_JSON" ;;
    *) exit 98 ;;
esac
`)
	writeExecutable(t, filepath.Join(fakeBin, "nix"), `#!/usr/bin/env bash
set -euo pipefail
all_arguments=" $* "
if [[ "$all_arguments" != *" build "* ]]; then
    while [[ $# -gt 0 && "$1" != "--command" ]]; do shift; done
    [[ $# -gt 0 ]]
    shift
    exec "$@"
fi
source_root=""
output=""
while [[ $# -gt 0 ]]; do
    case "$1" in -C) source_root=$2; shift 2 ;; -o) output=$2; shift 2 ;; *) shift ;; esac
done
[[ -n "$source_root" && -n "$output" ]]
cp "$source_root/scripts/review-loop" "$output"
chmod 755 "$output"
`)

	return adoptionFixture{
		root: root, remote: remote, seed: seed, checkout: checkout, fakeBin: fakeBin,
		baseSHA: baseSHA, headSHA: headSHA, candidateSHA: candidateSHA, advancedBaseSHA: advancedBaseSHA,
		capture: filepath.Join(root, "review-args"), countsDir: countsDir,
	}
}

func (fixture adoptionFixture) run(t *testing.T, options adoptionRun) (string, int) {
	t.Helper()

	if options.title == "" {
		options.title = "docs: improve contributor guidance"
	}
	if options.knownFindings == "" {
		options.knownFindings = "[]"
	}
	if options.triageDecision == "" {
		options.triageDecision = "KEEP"
	}
	if options.candidateSHA == "" {
		options.candidateSHA = fixture.candidateSHA
	}
	labels := make([]map[string]string, 0, len(options.labels))
	for _, label := range options.labels {
		labels = append(labels, map[string]string{"name": label})
	}
	metadata := map[string]any{
		"number": 123, "url": "https://github.com/owner/repo/pull/123", "state": "OPEN",
		"title": options.title, "body": options.body, "labels": labels,
		"baseRefName": "release/v3.0", "baseRefOid": fixture.baseSHA,
		"headRefName": "feature", "headRefOid": fixture.headSHA,
		"headRepositoryOwner": map[string]string{"login": "owner"},
		"headRepository":      map[string]string{"name": "repo"},
	}
	metadataJSON, err := json.Marshal(metadata)
	require.NoError(t, err)
	arguments := []string{filepath.Join(fixture.checkout, "scripts", "ai-pr-adopt-candidate"), "123", options.candidateSHA}
	if options.push {
		arguments = append(arguments, "--push")
	}
	command := exec.Command("bash", arguments...)
	command.Dir = fixture.checkout
	command.Env = append(os.Environ(),
		"PATH="+fixture.fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TEST_PR_JSON="+string(metadataJSON),
		"TEST_TRIAGE_DECISION="+options.triageDecision,
		"TEST_KNOWN_FINDINGS_JSON="+options.knownFindings,
		"TEST_COUNTS_DIR="+fixture.countsDir,
		"TEST_CAPTURE="+fixture.capture,
		"TEST_REMOTE="+fixture.remote,
		"TEST_ADVANCED_BASE_SHA="+fixture.advancedBaseSHA,
		"TEST_TARGET_MUTATION="+options.targetMutation,
	)
	if options.validationFailure {
		command.Env = append(command.Env, "VALIDATION_FAILURE=1")
	}
	output, runErr := command.CombinedOutput()
	if runErr == nil {
		return string(output), 0
	}
	var exitError *exec.ExitError
	require.ErrorAs(t, runErr, &exitError, string(output))

	return string(output), exitError.ExitCode()
}

func (fixture adoptionFixture) count(t *testing.T, name string) string {
	t.Helper()

	path := filepath.Join(fixture.countsDir, name)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return "0"
	}

	return strings.TrimSpace(readFile(t, path))
}

func (fixture adoptionFixture) advanceBase(t *testing.T) {
	t.Helper()

	runGit(t, fixture.root, "--git-dir", fixture.remote, "update-ref", "refs/heads/release/v3.0", fixture.advancedBaseSHA)
}

func preservedAdoptionWorktree(t *testing.T, output string) string {
	t.Helper()

	for line := range strings.SplitSeq(output, "\n") {
		if worktree, found := strings.CutPrefix(line, "CANDIDATE_WORKTREE="); found {
			return worktree
		}
	}
	require.FailNow(t, "preserved candidate worktree not reported", output)

	return ""
}

func sourceScriptPath(t *testing.T, name string) string {
	t.Helper()

	path, err := filepath.Abs(filepath.Join("..", name))
	require.NoError(t, err)

	return path
}

func copyFile(t *testing.T, source, destination string, mode os.FileMode) {
	t.Helper()

	content, err := os.ReadFile(source)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(destination, content, mode))
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()

	require.NoError(t, os.WriteFile(path, []byte(content), 0o755))
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()

	_ = runGitOutput(t, directory, arguments...)
}

func runGitOutput(t *testing.T, directory string, arguments ...string) string {
	t.Helper()

	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, fmt.Sprintf("git %s:\n%s", strings.Join(arguments, " "), output))

	return strings.TrimSpace(string(output))
}

func readFile(t *testing.T, path string) string {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(content)
}

func argumentValue(t *testing.T, arguments []string, name string) string {
	t.Helper()

	for index, argument := range arguments {
		if argument == name && index+1 < len(arguments) {
			return arguments[index+1]
		}
	}
	require.FailNow(t, "missing argument", name)

	return ""
}
