package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckGoSourceDetectsImportedCallsWithoutTextFalsePositives(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import clock "time"

func TestExample() {
	clock.Sleep(1)
	_ = "time.Sleep(1)"
	// time.Sleep(1)
}
`)

	findings, err := checkGoSource("sample_test.go", source)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Equal(t, 6, findings[0].line)
}

func TestCheckGoSourceDetectsDotImportedEnvironmentReads(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import . "os"

func read() string {
	return Getenv("LOCAL_POLICY")
}
`)

	findings, err := checkGoSource("internal/infra/state/nested/sample.go", source)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Equal(t, 6, findings[0].line)
}

func TestCheckGoSourceIgnoresShadowedPackageNames(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import "time"

type sleeper struct{}

func (s sleeper) Sleep(int) {}

func TestExample() {
	time := sleeper{}
	time.Sleep(1)
}

var _ = time.Second
`)

	findings, err := checkGoSource("sample_test.go", source)
	require.NoError(t, err)
	require.Empty(t, findings)
}

func TestCheckProtoSourceDetectsMultilineAndInlineDeclarations(t *testing.T) {
	t.Parallel()

	source := []byte(`syntax = "proto3";

message Example {
	reserved
		2,
		4;
	string value = 1; reserved "old_value";
}
`)

	findings := checkProtoSource("misc/proto/example.proto", source)
	require.Len(t, findings, 2)
	require.Equal(t, 4, findings[0].line)
	require.Equal(t, 7, findings[1].line)
}

func TestCheckProtoSourceIgnoresCommentsAndStrings(t *testing.T) {
	t.Parallel()

	source := []byte(`syntax = "proto3";

// reserved 1;
/*
reserved 2;
*/
message Example {
	string note = 1 [default = "reserved 3;"];
}
`)

	require.Empty(t, checkProtoSource("misc/proto/example.proto", source))
}

func TestIsDeterministicFSMPathIncludesNestedPackages(t *testing.T) {
	t.Parallel()

	require.True(t, isDeterministicFSMPath("internal/domain/processing/numscript/example.go"))
	require.True(t, isDeterministicFSMPath("internal/infra/plan/planerr/example.go"))
	require.False(t, isDeterministicFSMPath("internal/application/admission/example.go"))
}
