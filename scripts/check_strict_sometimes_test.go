package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Only assert.Sometimes calls whose condition is IsTolerated(...) are
// invariants in disguise; a Sometimes over any other condition is a genuine
// existential and must not be pulled into the strict list.
func TestToleratedSometimesInSource_MatchesOnlyToleratedConditions(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

func Sample(err error, converged bool) {
	assert.Sometimes(IsTolerated(err), "should be able to create ledger", nil)
	assert.Sometimes(converged, "scaling converges within timeout", nil)
	assert.Sometimes(true, "a pure reachability probe", nil)
	assert.Reachable("not a sometimes at all", nil)
	other.Sometimes(IsTolerated(err), "a different package", nil)
}
`)

	got, err := toleratedSometimesInSource("sample.go", source)
	require.NoError(t, err)
	require.Equal(t, []string{"should be able to create ledger"}, got)
}

// A non-literal message cannot be pinned, so it is skipped rather than
// half-recorded; the assertion-name rule already forbids them.
func TestToleratedSometimesInSource_SkipsNonLiteralMessages(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

func Sample(err error, name string) {
	assert.Sometimes(IsTolerated(err), name, nil)
}
`)

	got, err := toleratedSometimesInSource("sample.go", source)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestParseJSONStringArray(t *testing.T) {
	t.Parallel()

	got, err := parseJSONStringArray(`["b thing","a thing"]`)
	require.NoError(t, err)
	require.Equal(t, []string{"a thing", "b thing"}, got, "sorted, so comparison is order-free")

	empty, err := parseJSONStringArray(`[]`)
	require.NoError(t, err)
	require.Empty(t, empty)

	_, err = parseJSONStringArray(`not an array`)
	require.Error(t, err)
}

// The declaration the live script carries must stay machine-readable: the
// whole invariant rests on finding and parsing it.
func TestParseStrictSometimes_ReadsTheLiveScript(t *testing.T) {
	t.Parallel()

	source, err := os.ReadFile(filepath.Join("..", strictSometimesScript))
	require.NoError(t, err)

	got, err := parseStrictSometimes(source)
	require.NoError(t, err)
	require.Contains(t, got, "should be able to create ledger")
}

func TestParseStrictSometimes_RejectsAMissingDeclaration(t *testing.T) {
	t.Parallel()

	_, err := parseStrictSometimes([]byte("#!/bin/sh\necho hi\n"))
	require.Error(t, err)
}

func TestMissingFrom(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{"b"}, missingFrom([]string{"a", "b"}, []string{"a"}))
	require.Empty(t, missingFrom([]string{"a"}, []string{"a", "b"}))
}
