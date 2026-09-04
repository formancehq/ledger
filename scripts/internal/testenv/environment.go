// Package testenv provides hermetic process environments for AI-tooling tests.
package testenv

import (
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
)

var outerIdentityPrefixes = []string{
	"EXPECTED_",
	"AI_REVIEW_",
	"VALIDATION_RUN_",
}

// Environment returns the current environment without an enclosing AI run's
// identity. Overrides are applied last, so a fixture can
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
	// Explicit fixture values are trusted only after the inherited environment
	// has been removed. This makes re-injection deliberate and visible.
	for _, item := range overrides {
		name, value, found := strings.Cut(item, "=")
		if found {
			values[name] = value
		}
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

// Command gives a test subprocess the sanitized environment.
func Command(t testing.TB, name string, arguments ...string) *exec.Cmd {
	t.Helper()
	command := exec.Command(name, arguments...)
	command.Env = Environment()

	return command
}

func isOuterIdentityVariable(name string) bool {
	for _, prefix := range outerIdentityPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}

	return false
}
