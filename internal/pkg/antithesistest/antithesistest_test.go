package antithesistest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The marker is the only thing standing between a renamed test and an
// expectation of no emission that passes because nothing ran.
func TestExecutedNamedTestRequiresAPassMarker(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		logs string
		run  string
		want bool
	}{
		{
			name: "ran",
			logs: "=== RUN   TestThing\n--- PASS: TestThing (0.01s)\nPASS\n",
			run:  "TestThing",
			want: true,
		},
		{
			name: "subtest marker is indented",
			logs: "=== RUN   TestThing/case\n--- PASS: TestThing (0.01s)\n    --- PASS: TestThing/case (0.00s)\nPASS\n",
			run:  "TestThing/case",
			want: true,
		},
		{
			// `go test` exits 0 here, which is the whole problem.
			name: "no test matched the pattern",
			logs: "testing: warning: no tests to run\nPASS\n",
			run:  "TestRenamedAway",
			want: false,
		},
		{
			name: "skipped is not executed",
			logs: "=== RUN   TestThing\n--- SKIP: TestThing (0.00s)\nPASS\n",
			run:  "TestThing",
			want: false,
		},
		{
			// A prefix match would accept the sibling and miss the rename.
			name: "another test with the name as a prefix",
			logs: "=== RUN   TestThingExtended\n--- PASS: TestThingExtended (0.01s)\nPASS\n",
			run:  "TestThing",
			want: false,
		},
		{
			name: "parent ran but the named subtest did not",
			logs: "=== RUN   TestThing\n--- PASS: TestThing (0.01s)\n",
			run:  "TestThing/case",
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, executedNamedTest([]byte(tc.logs), tc.run))
		})
	}
}
