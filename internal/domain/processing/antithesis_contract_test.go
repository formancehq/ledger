//go:build enable_antithesis_sdk

package processing

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/antithesistest"
)

// Each case re-execs one ordinary regression and checks what it reported to
// the SDK; antithesistest.Emitted carries the reason a subprocess is required.
// Only ordinary regressions are named here, so injected corrupt states stay
// confined to test binaries.
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
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			found, err := antithesistest.Emitted(ctx, t.TempDir(), tc.test, tc.property, tc.condition)
			require.NoError(t, err)
			require.Equal(t, tc.emitted, found, "property %q, condition %v", tc.property, tc.condition)
		})
	}
}
