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

// The workload module is armed by its own CI job and Dockerfile, so a guard
// there is reachable even though the justfile recipe says nothing about it.
func TestArmedCoverageAcceptsGuardsInTheWorkloadModule(t *testing.T) {
	t.Parallel()

	trees := append(append([]string{}, armedCoverageTestTrees...), armedCoverageExternalTrees...)

	_, found := armedCoverageInSource(
		"tests/antithesis/workload/bin/cmds/main/driver/main.go",
		[]byte(armedCoverageGuardedSource),
		trees,
	)
	require.False(t, found)
}

// The recipe name is matched with its colon, so a longer recipe sharing the
// prefix is a different recipe and must not be picked up.
func TestArmedCoverageRecipeNameIsNotMatchedByPrefix(t *testing.T) {
	t.Parallel()

	trees, err := armedCoverageTreesFromJustfile(`test-antithesis-assertions-fast:
    go test ./wrong/...

test-antithesis-assertions:
    go test ./right/...
`)
	require.NoError(t, err)
	require.Equal(t, []string{"right"}, trees)
}

// just allows dependencies after the colon; that is the same recipe.
func TestArmedCoverageRecipeAcceptsDependencies(t *testing.T) {
	t.Parallel()

	trees, err := armedCoverageTreesFromJustfile(
		"test-antithesis-assertions: build\n    go test ./right/...\n",
	)
	require.NoError(t, err)
	require.Equal(t, []string{"right"}, trees)
}

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
