package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// worldSendScript resolves without consulting any mutable state: @world is an
// unbounded source and the destination is a literal, so dependency discovery
// succeeds on a bare store.
const worldSendScript = `send [USD 1] (
	source = @world
	destination = @dst
)`

// TestExecutableReferenceVersionSelector pins the executable selector contract:
// a NumscriptReference carried into an audited order accepts only the literal
// "latest" or an exact full semver. Admission rejects everything else with
// NUMSCRIPT_INVALID_VERSION — an empty selector because the audited order must
// state the caller's intended version explicitly, a partial one because
// resolving it needs a Pebble scan the FSM apply path cannot make.
func TestExecutableReferenceVersionSelector(t *testing.T) {
	t.Parallel()

	t.Run("rejects a selector that is neither latest nor a full semver", func(t *testing.T) {
		t.Parallel()

		for _, version := range []string{"", "1", "1.2", "bogus", "1.0.0-rc1", "01.0.0", "LATEST"} {
			t.Run("version="+version, func(t *testing.T) {
				t.Parallel()

				store := createTestStore(t)
				admission, _ := createTestAdmission(t, store)
				writeNumscriptRef(t, admission, testLedgerName, "pay", "1.0.0", worldSendScript)

				err := runResolveProvenance(t, admission, []*raftcmdpb.Order{
					referenceOrder(testLedgerName, "pay", version),
				}, false)

				var invalid *domain.ErrNumscriptInvalidVersion
				require.ErrorAs(t, err, &invalid, "version %q must be rejected", version)
				require.Equal(t, version, invalid.Version)
				require.Equal(t, domain.ErrReasonNumscriptInvalidVersion, invalid.Reason())
			})
		}
	})

	t.Run("accepts latest and an exact full semver", func(t *testing.T) {
		t.Parallel()

		for _, version := range []string{"latest", "1.0.0"} {
			t.Run("version="+version, func(t *testing.T) {
				t.Parallel()

				store := createTestStore(t)
				admission, _ := createTestAdmission(t, store)
				writeNumscriptRef(t, admission, testLedgerName, "pay", "1.0.0", worldSendScript)

				orders := []*raftcmdpb.Order{referenceOrder(testLedgerName, "pay", version)}
				require.NoError(t, runResolveProvenance(t, admission, orders, false))

				// The selector reaches the audit chain exactly as submitted; only
				// planning resolves it.
				ref := orders[0].GetLedgerScoped().GetApply().GetCreateTransaction().GetNumscriptReference()
				require.Equal(t, version, ref.GetVersion())
			})
		}
	})
}
