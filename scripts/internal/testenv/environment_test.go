package testenv

import (
	"strings"
	"testing"
)

func TestEnvironmentDropsCurrentAndFutureOuterIdentity(t *testing.T) {
	outerVariables := []string{
		"EXPECTED_PR_NUMBER",
		"EXPECTED_WORKTREE",
		"EXPECTED_HEAD",
		"EXPECTED_FUTURE_BINDING",
		"AI_REVIEW_HEAD",
		"AI_REVIEW_FUTURE_IDENTITY",
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
}

func TestEnvironmentOnlyRestoresExplicitSyntheticIdentity(t *testing.T) {
	t.Setenv("EXPECTED_HEAD", "outer-head")

	environment := environmentMap(Environment(
		"EXPECTED_HEAD=synthetic-head",
	))
	if got := environment["EXPECTED_HEAD"]; got != "synthetic-head" {
		t.Errorf("EXPECTED_HEAD = %q, want synthetic-head", got)
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
