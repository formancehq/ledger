package main

import (
	"os"
	"path/filepath"
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

func TestDiscoverRepositoryFuzzTargetsSkipsDeletedTrackedFiles(t *testing.T) {
	t.Parallel()

	targets, err := discoverRepositoryFuzzTargets([]string{
		filepath.Join(t.TempDir(), "deleted_test.go"),
	})
	require.NoError(t, err)
	require.Empty(t, targets)
}

func TestFuzzFileConstraintReasonRejectsBuildAndPlatformConstraints(t *testing.T) {
	t.Parallel()

	platformSuffixes := map[string]struct{}{
		"_linux":       {},
		"_linux_amd64": {},
	}

	require.Equal(t,
		"the declaration file has a Go build constraint",
		fuzzFileConstraintReason("fuzz_test.go", []byte("//go:build integration\n\npackage sample\n"), platformSuffixes),
	)
	require.Equal(t,
		"the declaration file has the platform suffix _linux_amd64",
		fuzzFileConstraintReason("fuzz_linux_amd64_test.go", []byte("package sample\n"), platformSuffixes),
	)
	require.Empty(t,
		fuzzFileConstraintReason("fuzz_test.go", []byte("package sample\n"), platformSuffixes),
	)
}

func TestNestedGoModuleForPathUsesOwningModule(t *testing.T) {
	t.Parallel()

	modules := nestedGoModuleDirectories([]string{
		"go.mod",
		"misc/operator/go.mod",
		"misc/operator/tools/go.mod",
		"misc/other/not-go.mod",
	})

	require.Equal(t, []string{"misc/operator/tools", "misc/operator"}, modules)
	require.Equal(t,
		"misc/operator/tools",
		nestedGoModuleForPath("misc/operator/tools/internal/fuzz_test.go", modules),
	)
	require.Equal(t,
		"misc/operator",
		nestedGoModuleForPath("misc/operator/internal/fuzz_test.go", modules),
	)
	require.Empty(t, nestedGoModuleForPath("internal/fuzz_test.go", modules))
}

func TestUnreachableFuzzTargetFindingsReportsNestedModule(t *testing.T) {
	t.Parallel()

	targets := []locatedFuzzTarget{
		{
			target:            fuzzTarget{packagePath: "./misc/operator/internal/", name: "FuzzNested"},
			location:          finding{path: "misc/operator/internal/fuzz_test.go", line: 7, column: 6},
			unreachableReason: "the declaration belongs to nested Go module misc/operator",
		},
	}
	findings := unreachableFuzzTargetFindings(targets)

	require.Len(t, findings, 1)
	require.Contains(t, findings[0].message, "nested Go module misc/operator")
	require.Contains(t, findings[0].message, "extend the runner to invoke targets from that module")
	require.Empty(t, compareFuzzTargets(targets, nil))
}

func TestReadFuzzRunnerInventoryAcceptsRootPackage(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "fuzz-targets.txt")
	require.NoError(t, os.WriteFile(path, []byte("./ FuzzRoot\n"), 0o600))

	targets, findings, err := readFuzzRunnerInventory(path)
	require.NoError(t, err)
	require.Empty(t, findings)
	require.Equal(t, []locatedFuzzTarget{
		{
			target:   fuzzTarget{packagePath: "./", name: "FuzzRoot"},
			location: finding{path: path, line: 1, column: 1},
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
