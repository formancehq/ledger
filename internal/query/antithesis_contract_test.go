//go:build enable_antithesis_sdk

package query_test

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The SDK initializes before tests run. Re-exec only the selected ordinary
// regression with local output configured at process start; injected corrupt
// states remain confined to test binaries.
func TestAntithesisContractEmission(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		test, property     string
		condition, emitted bool
	}{
		{"TestAlignedIndexSnapshotRejectsMainSnapshotBehindReadBarrier", "linearizable query snapshot covers its read barrier", false, true},
		{"TestAlignedIndexSnapshotAcceptsCoveredOrAbsentReadBarrier", "linearizable query snapshot covers its read barrier", false, false},
		{"TestAlignedIndexSnapshotAlignsAfterObservedWait", "indexed snapshot aligned after waiting for projection", true, true},
		// The already-aligned return still evaluates the property, false: the
		// gate is assert.Enabled, not a branch, so Antithesis keeps the signal.
		{"TestAlignedIndexSnapshotAcceptsCoveredOrAbsentReadBarrier", "indexed snapshot aligned after waiting for projection", false, true},
		{"TestAlignedIndexSnapshot_WaitsOnlyAsLongAsTheCallerAllows", "indexed snapshot aligned after waiting for projection", true, false},
		{"TestAlignedIndexSnapshot_WaitsOnlyAsLongAsTheCallerAllows", "linearizable query snapshot covers its read barrier", false, false},
	} {
		t.Run(tc.test+"/"+tc.property, func(t *testing.T) {
			t.Parallel()
			output := filepath.Join(t.TempDir(), "assertions.jsonl")
			executable, err := os.Executable()
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^"+regexp.QuoteMeta(tc.test)+"$", "-test.count=1")
			cmd.Env = append(os.Environ(), "ANTITHESIS_SDK_LOCAL_OUTPUT="+output)
			logs, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", logs)
			file, err := os.Open(output)
			require.NoError(t, err)
			defer func() { _ = file.Close() }()
			found := false
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				var event struct {
					Assertion struct {
						Message   string `json:"message"`
						Hit       bool   `json:"hit"`
						Condition bool   `json:"condition"`
					} `json:"antithesis_assert"`
				}
				require.NoError(t, json.Unmarshal(scanner.Bytes(), &event))
				if event.Assertion.Message == tc.property && event.Assertion.Hit && event.Assertion.Condition == tc.condition {
					found = true
				}
			}
			require.NoError(t, scanner.Err())
			require.Equal(t, tc.emitted, found, "property %q, condition %v", tc.property, tc.condition)
		})
	}
}
