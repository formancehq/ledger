// Package testenv provides hermetic process environments for AI-tooling tests.
package testenv

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const gitGuardDirectory = "git-guard-bin"

var exactOuterIdentityVariables = map[string]struct{}{
	"CANDIDATE_WORKTREE":    {},
	"TRUSTED_ROOT_CHECKOUT": {},
}

var outerIdentityPrefixes = []string{
	"EXPECTED_",
	"AI_WORKTREE_",
	"AI_GIT_",
	"AI_REVIEW_",
	"VALIDATION_RUN_",
}

// Environment returns the current environment without an enclosing AI run's
// worktree identity or Git guard. Overrides are applied last, so a fixture can
// explicitly install its own synthetic identity.
func Environment(overrides ...string) []string {
	values := make(map[string]string, len(os.Environ())+len(overrides))
	for _, item := range append(os.Environ(), overrides...) {
		name, value, found := strings.Cut(item, "=")
		if found {
			values[name] = value
		}
	}

	for name := range values {
		if isOuterIdentityVariable(name) {
			delete(values, name)
		}
	}
	if path, found := values["PATH"]; found {
		values["PATH"] = sanitizePath(path)
	}

	// Explicit fixture values are trusted only after the inherited environment
	// has been removed. This makes re-injection deliberate and visible.
	for _, item := range overrides {
		name, value, found := strings.Cut(item, "=")
		if found {
			values[name] = value
		}
	}
	if path, found := values["PATH"]; found {
		values["PATH"] = sanitizePath(path)
	}

	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	environment := make([]string, 0, len(names))
	for _, name := range names {
		environment = append(environment, name+"="+values[name])
	}

	return environment
}

// EnvironmentMap is Environment with map-shaped fixture overrides.
func EnvironmentMap(overrides map[string]string) []string {
	names := make([]string, 0, len(overrides))
	for name := range overrides {
		names = append(names, name)
	}
	sort.Strings(names)
	values := make([]string, 0, len(names))
	for _, name := range names {
		values = append(values, name+"="+overrides[name])
	}

	return Environment(values...)
}

// SanitizeProcess removes outer AI identity from the test binary itself. Use
// it from TestMain when production code called in-process resolves Git before a
// per-command fixture environment can be supplied.
func SanitizeProcess() error {
	environment := Environment()
	os.Clearenv()
	for _, item := range environment {
		name, value, found := strings.Cut(item, "=")
		if !found {
			continue
		}
		if err := os.Setenv(name, value); err != nil {
			return fmt.Errorf("restore sanitized environment variable %s: %w", name, err)
		}
	}

	return nil
}

// Command resolves a test tool through the sanitized PATH and gives it the
// sanitized environment. This matters for Git because exec.Command otherwise
// resolves the executable through the enclosing AI run's PATH before Cmd.Env
// can take effect.
func Command(t testing.TB, name string, arguments ...string) *exec.Cmd {
	t.Helper()
	environment := Environment()
	executable, err := lookPath(name, environmentValue(environment, "PATH"))
	if err != nil {
		t.Fatalf("resolve %s in sanitized test PATH: %v", name, err)
	}
	command := exec.Command(executable, arguments...)
	command.Env = environment

	return command
}

func isOuterIdentityVariable(name string) bool {
	if _, found := exactOuterIdentityVariables[name]; found {
		return true
	}
	for _, prefix := range outerIdentityPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}

	return false
}

func sanitizePath(path string) string {
	entries := strings.Split(path, string(os.PathListSeparator))
	filtered := entries[:0]
	for _, entry := range entries {
		if filepath.Base(filepath.Clean(entry)) == gitGuardDirectory {
			continue
		}
		filtered = append(filtered, entry)
	}

	return strings.Join(filtered, string(os.PathListSeparator))
}

func environmentValue(environment []string, name string) string {
	for _, item := range environment {
		key, value, found := strings.Cut(item, "=")
		if found && key == name {
			return value
		}
	}

	return ""
}

func lookPath(name, path string) (string, error) {
	if strings.ContainsRune(name, os.PathSeparator) {
		return name, nil
	}
	for _, directory := range filepath.SplitList(path) {
		if directory == "" {
			directory = "."
		}
		candidate := filepath.Join(directory, name)
		info, err := os.Stat(candidate)
		if err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0 {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("%w: %s", exec.ErrNotFound, name)
}
