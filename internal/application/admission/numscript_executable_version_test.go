package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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
// "latest" or an exact full semver. Everything else is NUMSCRIPT_INVALID_VERSION
// — an empty selector because the audited order must state the caller's intended
// version explicitly, a partial one because resolving it needs a Pebble scan the
// FSM apply path cannot make.
func TestExecutableReferenceVersionSelector(t *testing.T) {
	t.Parallel()

	t.Run("rejects a selector that is neither latest nor a full semver", func(t *testing.T) {
		t.Parallel()

		for _, version := range []string{"", "1", "1.2", "bogus", "1.0.0-rc1", "01.0.0", "LATEST"} {
			t.Run("version="+version, func(t *testing.T) {
				t.Parallel()

				err := validateOrder(referenceOrder(testLedgerName, "pay", version))

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

				require.NoError(t, validateOrder(referenceOrder(testLedgerName, "pay", version)))

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

// skippableRefRequest builds an Apply request whose CreateTransaction references
// a numscript, claims a transaction reference, and opts into the
// TRANSACTION_REFERENCE_CONFLICT skip.
func skippableRefRequest(ledger, txRef, scriptName, version string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Reference:       txRef,
							ScriptReference: &servicepb.ScriptReference{Name: scriptName, Version: version},
						},
					},
				},
				SkippableReasons: []commonpb.ErrorReason{
					commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
				},
			},
		},
	}
}

// postingsRequest builds an Apply request that claims a transaction reference
// with a plain postings transaction.
func postingsRequest(ledger, txRef string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Reference: txRef,
							Postings: []*commonpb.Posting{{
								Source:      "world",
								Destination: "dst",
								Asset:       "USD",
								Amount:      commonpb.NewUint256FromUint64(1),
							}},
						},
					},
				},
			},
		},
	}
}

// TestExecutableReferenceVersionSelector_SkippedOrder pins that the selector is
// validated even when the order is predicted to skip. A CreateTransaction whose
// transaction reference already exists — persisted, or claimed earlier in the
// same batch — short-circuits script resolution entirely (the FSM checks
// reference uniqueness in its dry prologue and emits an OrderSkippedLog), so a
// selector gated only during resolution would be accepted and audited.
func TestExecutableReferenceVersionSelector_SkippedOrder(t *testing.T) {
	t.Parallel()

	for _, version := range []string{"", "1", "1.2", "bogus"} {
		t.Run("persisted duplicate/version="+version, func(t *testing.T) {
			t.Parallel()

			store := createTestStore(t)
			admission, _ := createTestAdmissionWithReader(t, store, nil)
			writeReference(t, admission, testLedgerName, "dup-ref", 1)

			_, err := admission.Admit(context.Background(), servicepb.UnsignedApplyRequest("",
				skippableRefRequest(testLedgerName, "dup-ref", "pay", version)))

			var invalid *domain.ErrNumscriptInvalidVersion
			require.ErrorAs(t, err, &invalid, "version %q must be rejected on the skip path", version)
			require.Equal(t, version, invalid.Version)
		})

		t.Run("same-batch duplicate/version="+version, func(t *testing.T) {
			t.Parallel()

			store := createTestStore(t)
			admission, _ := createTestAdmissionWithReader(t, store, nil)

			// The first entry claims the reference, so the second is predicted
			// to skip on the intra-batch conflict with nothing persisted.
			_, err := admission.Admit(context.Background(), servicepb.UnsignedApplyRequest("",
				postingsRequest(testLedgerName, "r1"),
				skippableRefRequest(testLedgerName, "r1", "pay", version)))

			var invalid *domain.ErrNumscriptInvalidVersion
			require.ErrorAs(t, err, &invalid, "version %q must be rejected on the skip path", version)
			require.Equal(t, version, invalid.Version)
		})
	}
}
