package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiscoverFuzzTargetsUsesGoFuzzSignature(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import testpkg "testing"

 func FuzzIncluded(f *testpkg.F) {}
 func Fuzzlowercase(f *testpkg.F) {}
 func FuzzWrongParameter(t *testpkg.T) {}
 func FuzzWrongResult(f *testpkg.F) error { return nil }
 func (sample) FuzzMethod(f *testpkg.F) {}
`)

	targets, err := discoverFuzzTargets("internal/sample/fuzz_test.go", source)
	require.NoError(t, err)
	require.Equal(t, []locatedFuzzTarget{
		{
			target: fuzzTarget{
				packagePath: "./internal/sample/",
				name:        "FuzzIncluded",
			},
			location: finding{
				path:   "internal/sample/fuzz_test.go",
				line:   5,
				column: 7,
			},
		},
	}, targets)
}

func TestCompareFuzzTargetsDetectsMissingAndStaleEntries(t *testing.T) {
	t.Parallel()

	actual := []locatedFuzzTarget{
		{
			target:   fuzzTarget{packagePath: "./actual/", name: "FuzzMissing"},
			location: finding{path: "actual/fuzz_test.go", line: 7, column: 6},
		},
	}
	runner := []locatedFuzzTarget{
		{
			target:   fuzzTarget{packagePath: "./runner/", name: "FuzzStale"},
			location: finding{path: fuzzRunnerInventoryPath, line: 3, column: 1},
		},
	}

	findings := compareFuzzTargets(actual, runner)
	require.Len(t, findings, 2)
	require.Equal(t, "actual/fuzz_test.go", findings[0].path)
	require.Contains(t, findings[0].message, "FuzzMissing")
	require.Contains(t, findings[0].message, "add \"./actual/ FuzzMissing\"")
	require.Equal(t, fuzzRunnerInventoryPath, findings[1].path)
	require.Contains(t, findings[1].message, "FuzzStale")
	require.Contains(t, findings[1].message, "no matching Go fuzz declaration")
}
