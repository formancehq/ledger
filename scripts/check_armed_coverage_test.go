package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const armedCoverageGuardedSource = `package stray

func f() {
	if assert.Enabled {
		_ = 1
	}
}
`

var armedCoverageTestTrees = []string{"internal/infra/state", "internal/query"}

func TestArmedCoverageAcceptsGuardsInsideTheTarget(t *testing.T) {
	t.Parallel()

	_, found := armedCoverageInSource(
		"internal/infra/state/machine.go",
		[]byte(armedCoverageGuardedSource),
		armedCoverageTestTrees,
	)
	require.False(t, found)
}

func TestArmedCoverageRejectsGuardOutsideTheTarget(t *testing.T) {
	t.Parallel()

	item, found := armedCoverageInSource(
		"internal/infra/health/healthcheck.go",
		[]byte(armedCoverageGuardedSource),
		armedCoverageTestTrees,
	)
	require.True(t, found)
	require.Contains(t, item.message, "ARMED_COVERAGE_UNREACHABLE")
	require.Contains(t, item.message, "internal/infra/health/healthcheck.go")
}

// A prefix that is not a path boundary must not count as coverage.
func TestArmedCoverageRejectsSiblingTreeWithSharedPrefix(t *testing.T) {
	t.Parallel()

	_, found := armedCoverageInSource(
		"internal/querytools/tool.go",
		[]byte(armedCoverageGuardedSource),
		armedCoverageTestTrees,
	)
	require.True(t, found)
}

func TestArmedCoverageIgnoresTestFilesAndUnguardedSources(t *testing.T) {
	t.Parallel()

	// A guard in a test file is compiled by the armed test build itself.
	_, found := armedCoverageInSource(
		"internal/infra/health/healthcheck_test.go",
		[]byte(armedCoverageGuardedSource),
		armedCoverageTestTrees,
	)
	require.False(t, found)

	// Reading the constant is not guarding a body with it.
	_, found = armedCoverageInSource(
		"internal/infra/health/healthcheck.go",
		[]byte("package stray\n\nfunc g() { _ = assert.Enabled }\n"),
		armedCoverageTestTrees,
	)
	require.False(t, found)
}

// The trees come from the recipe, so widening the target widens the check.
func TestArmedCoverageReadsTheTreesFromTheRecipe(t *testing.T) {
	t.Parallel()

	trees, err := armedCoverageTreesFromJustfile(`other:
    go test ./nope/...

test-antithesis-assertions:
    #!/usr/bin/env bash
    go test -race -tags enable_antithesis_sdk \
        ./internal/infra/state/... ./internal/query/...

next:
    go test ./also-nope/...
`)
	require.NoError(t, err)
	require.Equal(t, []string{"internal/infra/state", "internal/query"}, trees)

	_, err = armedCoverageTreesFromJustfile("other:\n    go test ./nope/...\n")
	require.Error(t, err)
}

// The real recipe must stay parseable: a rename or a reformat that yields no
// trees would leave the check reporting nothing at all. Go runs a package's
// tests from its own directory, so the repository root is one level up.
func TestArmedCoverageParsesTheRealRecipe(t *testing.T) {
	t.Parallel()

	source, err := os.ReadFile(filepath.Join("..", justfilePath))
	require.NoError(t, err)

	trees, err := armedCoverageTreesFromJustfile(string(source))
	require.NoError(t, err)
	require.NotEmpty(t, trees)
	require.Contains(t, trees, "internal/infra/state")

	for _, tree := range trees {
		require.DirExists(t, filepath.Join("..", tree))
	}
}
