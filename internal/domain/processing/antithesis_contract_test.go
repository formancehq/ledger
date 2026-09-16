//go:build enable_antithesis_sdk

package processing

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
		{"TestProcessRevertTransaction_StateMissingIsInconsistent", "revert target has allocated transaction state", false, true},
		{"TestProcessRevertTransaction_EmptyPostingsIsInconsistent", "revert target has nonempty original postings", false, true},
		{"TestProcessRevertTransaction_AtEffectiveDate_MissingOriginalTimestamp", "effective-date revert target has a timestamp", false, true},
		{"TestProcessRevertTransaction_AlreadyReverted", "repeat revert rejected", true, true},
		{"TestProcessRevertTransaction_NotFound", "revert target has allocated transaction state", false, false},
		{"TestProcessRevertTransaction_Success", "revert target has nonempty original postings", false, false},
		{"TestProcessRevertTransaction_AtEffectiveDate", "effective-date revert target has a timestamp", false, false},
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
			defer func() { require.NoError(t, file.Close()) }()
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
