package main

import (
	"testing"

	"buf.build/go/bufplugin/check/checktest"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	t.Parallel()

	checktest.SpecTest(t, newSpec())
}

func TestValidFileHasNoAnnotations(t *testing.T) {
	t.Parallel()

	checktest.CheckTest{
		Request: &checktest.RequestSpec{
			Files: &checktest.ProtoFileSpec{DirPaths: []string{"testdata/valid"}, FilePaths: []string{"valid.proto"}},
		},
		Spec: newSpec(),
	}.Run(t)
}

func TestInvalidFileAnnotations(t *testing.T) {
	t.Parallel()

	at := func(ruleID string, startLine, startColumn, endLine, endColumn int) checktest.ExpectedAnnotation {
		return checktest.ExpectedAnnotation{
			RuleID: ruleID,
			FileLocation: &checktest.ExpectedFileLocation{
				FileName:    "invalid.proto",
				StartLine:   startLine,
				StartColumn: startColumn,
				EndLine:     endLine,
				EndColumn:   endColumn,
			},
		}
	}

	checktest.CheckTest{
		Request: &checktest.RequestSpec{
			Files: &checktest.ProtoFileSpec{DirPaths: []string{"testdata/invalid"}, FilePaths: []string{"invalid.proto"}},
		},
		Spec: newSpec(),
		ExpectedAnnotations: []checktest.ExpectedAnnotation{
			// Line and column numbers are zero-based in checktest. Reserved
			// and ReservedEnum are otherwise sequential: only their reserved
			// statements are reported.
			// checktest sorts annotations by rule ID, then location.
			at(enumNumbersSequentialRuleID, 19, 0, 24, 1),
			at(fieldNumbersSequentialRuleID, 10, 0, 13, 1),
			at(fieldNumbersSequentialRuleID, 15, 0, 17, 1),
			at(noReservedRuleID, 5, 11, 5, 12),
			at(noReservedRuleID, 6, 11, 6, 16),
			at(noReservedRuleID, 27, 11, 27, 12),
		},
	}.Run(t)
}

func TestFirstMissing(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		numbers []int
		start   int
		missing int
		ok      bool
	}{
		{name: "empty", numbers: nil, start: 1, ok: true},
		{name: "unordered permutation", numbers: []int{3, 1, 2}, start: 1, ok: true},
		{name: "gap", numbers: []int{1, 3}, start: 1, missing: 2},
		{name: "duplicate", numbers: []int{0, 1, 1}, start: 0, missing: 2},
		{name: "leading alias", numbers: []int{0, 0, 1}, start: 0, missing: 2},
		{name: "negative value", numbers: []int{0, -1, 1}, start: 0, missing: 2},
		{name: "wrong start", numbers: []int{2}, start: 1, missing: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			missing, ok := firstMissing(tc.numbers, tc.start)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.missing, missing)
		})
	}
}

func TestExistingTypesRetainNumbersAndReservations(t *testing.T) {
	t.Parallel()

	checktest.CheckTest{
		Request: &checktest.RequestSpec{
			Files: &checktest.ProtoFileSpec{DirPaths: []string{"testdata/existing"}, FilePaths: []string{"existing.proto"}},
		},
		Spec: newSpec(),
		ExpectedAnnotations: []checktest.ExpectedAnnotation{{
			RuleID: fieldNumbersSequentialRuleID,
			FileLocation: &checktest.ExpectedFileLocation{
				FileName:  "existing.proto",
				StartLine: 11, StartColumn: 2, EndLine: 13, EndColumn: 3,
			},
		}},
	}.Run(t)
}
