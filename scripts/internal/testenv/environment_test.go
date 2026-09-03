package testenv

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnvironmentDropsCurrentAndFutureOuterIdentity(t *testing.T) {
	guardOne := filepath.Join(t.TempDir(), gitGuardDirectory)
	guardTwo := filepath.Join(t.TempDir(), gitGuardDirectory)
	cleanBin := t.TempDir()
	t.Setenv("PATH", strings.Join([]string{guardOne, cleanBin, guardTwo}, string(os.PathListSeparator)))
	outerVariables := []string{
		"EXPECTED_PR_NUMBER",
		"EXPECTED_WORKTREE",
		"EXPECTED_HEAD",
		"EXPECTED_FUTURE_BINDING",
		"AI_WORKTREE_PR",
		"AI_WORKTREE_PATH",
		"AI_WORKTREE_EXPECTED_HEAD",
		"AI_WORKTREE_BINDING_FILE",
		"AI_WORKTREE_FUTURE_BINDING",
		"TRUSTED_ROOT_CHECKOUT",
		"AI_GIT_REAL_PATH",
		"AI_GIT_ORIGINAL_PATH",
		"AI_GIT_FUTURE_BINDING",
		"AI_PR_TRIAGE_EXPECT_BASE_SHA",
		"AI_REVIEW_HEAD",
		"AI_REVIEW_FUTURE_IDENTITY",
		"TARGET_BASE_SHA",
		"PR_HEAD_SHA",
		"CANDIDATE_SHA",
		"CANDIDATE_WORKTREE",
		"VALIDATION_RUN_DIR",
		"VALIDATION_RUN_ID",
		"VALIDATION_RUN_FUTURE_IDENTITY",
	}
	for _, name := range outerVariables {
		t.Setenv(name, "inherited-parent-value")
	}

	environment := environmentMap(Environment())
	for _, name := range outerVariables {
		if _, found := environment[name]; found {
			t.Errorf("%s escaped into the synthetic environment", name)
		}
	}
	if got := environment["PATH"]; got != cleanBin {
		t.Errorf("sanitized PATH = %q, want %q", got, cleanBin)
	}
}

func TestEnvironmentOnlyRestoresExplicitSyntheticIdentity(t *testing.T) {
	t.Setenv("EXPECTED_HEAD", "outer-head")
	t.Setenv("AI_WORKTREE_PATH", "/outer/worktree")

	environment := environmentMap(Environment(
		"EXPECTED_HEAD=synthetic-head",
		"AI_WORKTREE_PATH=/synthetic/worktree",
	))
	if got := environment["EXPECTED_HEAD"]; got != "synthetic-head" {
		t.Errorf("EXPECTED_HEAD = %q, want synthetic-head", got)
	}
	if got := environment["AI_WORKTREE_PATH"]; got != "/synthetic/worktree" {
		t.Errorf("AI_WORKTREE_PATH = %q, want /synthetic/worktree", got)
	}
}

func environmentMap(environment []string) map[string]string {
	values := make(map[string]string, len(environment))
	for _, item := range environment {
		name, value, found := strings.Cut(item, "=")
		if found {
			values[name] = value
		}
	}

	return values
}
