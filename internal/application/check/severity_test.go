package check

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestIsCoverageGapClassifiesEveryIncompleteClass is the anti-drift barrier on
// the classification, not a restatement of it.
//
// The list in IsCoverageGap is hand-written, and the cost of it falling behind
// the enum is asymmetric: a new *_VERIFICATION_INCOMPLETE class that is missed
// silently starts failing `restore validate` on healthy backups again, which is
// exactly the regression this classification exists to undo. Deriving the
// expectation from the enum's own names catches that at the point the member is
// added rather than at the point an operator hits it.
func TestIsCoverageGapClassifiesEveryIncompleteClass(t *testing.T) {
	t.Parallel()

	var incomplete int

	for value, name := range servicepb.CheckStoreErrorType_name {
		errorType := servicepb.CheckStoreErrorType(value)

		if !strings.HasSuffix(name, "_VERIFICATION_INCOMPLETE") {
			require.False(t, IsCoverageGap(errorType),
				"%s reports a divergence, so it must count toward the integrity verdict", name)

			continue
		}

		incomplete++

		require.True(t, IsCoverageGap(errorType),
			"%s says a pass could not be completed, not that the store is wrong: "+
				"leaving it out of IsCoverageGap makes it fail `restore validate` on a "+
				"healthy backup", name)
	}

	require.Equal(t, 3, incomplete,
		"the signing, log-bound and archived-state passes are the three that can "+
			"report incomplete coverage; a fourth means this test's expectation needs "+
			"revisiting alongside IsCoverageGap")
}
