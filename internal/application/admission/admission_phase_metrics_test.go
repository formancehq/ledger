package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// createTestAdmissionWithReader builds an Admission with metrics ENABLED against
// a real SDK meter provider fed by a ManualReader, so a test can assert exactly
// which phase histograms recorded a data point for a given Admit call. The
// default createTestAdmission wires a noop meter and cannot observe recordings.
func createTestAdmissionWithReader(t *testing.T, store *dal.Store) (*Admission, *sdkmetric.ManualReader) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	testCache, _ := cache.New(100, nil)
	attrs := attributes.New()
	testPreloader := plan.NewBuilder(node.NewIndexTracker(1), testCache, attrs, store, nil, logger, 0)

	ks := keystore.NewKeyStore()
	ss := state.NewSharedState()

	// Writes always allowed: these tests exercise the phase pipeline, not the
	// write gate. A nil gate would panic in Admit before the first phase.
	ctrl := gomock.NewController(t)
	writeGate := health.NewMockWriteGate(ctrl)
	writeGate.EXPECT().CheckWritesAllowed().Return(nil).AnyTimes()

	a := NewAdmission(
		store,
		logger,
		nil, // no proposer: these tests fail before the propose phase
		testPreloader,
		provider,
		writeGate,
		ks,
		ss,
		attrs,
		numscript.NewNumscriptCache(0),
		func(context.Context) error { return nil },
		WithMetrics(),
	)

	return a, reader
}

// recordedPhaseCounts collects, from the manual reader, the number of
// observations each admission phase histogram received. Absent instruments map
// to 0 (they were never Recorded, so the SDK never created the series).
func recordedPhaseCounts(t *testing.T, reader *sdkmetric.ManualReader) map[string]uint64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	counts := make(map[string]uint64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			hist, ok := m.Data.(metricdata.Histogram[int64])
			if !ok {
				continue
			}

			var total uint64
			for _, dp := range hist.DataPoints {
				total += dp.Count
			}
			counts[m.Name] = total
		}
	}

	return counts
}

// TestAdmitRecordsEnteredPhasesOnFailure asserts the P2 invariant flemzord
// raised on #1537: every phase histogram must cover the same request population
// as admission.command.duration. A command that fails mid-pipeline must still
// record the phases it actually entered — and must NOT record a spurious
// observation for phases it never reached.
func TestAdmitRecordsEnteredPhasesOnFailure(t *testing.T) {
	t.Parallel()

	// The full set of phase histograms whose population must match
	// command.duration. proposal_guard/propose are excluded: they are only
	// reachable with a live proposer (nil here) and are covered by the
	// propose-path reasoning in admission.go, not by this pre-propose test.
	const (
		mResolveBatch       = "admission.resolve_batch.duration"
		mOrdersPreparation  = "admission.orders_preparation.duration"
		mScripts            = "admission.scripts.duration"
		mResponseResolution = "admission.response_resolution.duration"
		mFSMFutureWait      = "admission.fsm_future.wait.duration"
	)

	t.Run("failure in orders_preparation records resolve_batch + orders_preparation only", func(t *testing.T) {
		t.Parallel()

		store := createTestStore(t)
		a, reader := createTestAdmissionWithReader(t, store)

		// A revert with transaction id 0 is rejected by resolveRevertTarget
		// (ErrTransactionTargetMissing) inside requestsToOrders — i.e. during the
		// orders_preparation phase, after resolve_batch has completed but before
		// the scripts phase is entered.
		_, err := a.Admit(context.Background(), servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: testLedgerName,
					Action: &servicepb.LedgerAction{
						Data: &servicepb.LedgerAction_RevertTransaction{
							RevertTransaction: &servicepb.RevertTransactionPayload{
								TransactionId: 0,
							},
						},
					},
				},
			},
		}))
		require.ErrorIs(t, err, domain.ErrTransactionTargetMissing)

		counts := recordedPhaseCounts(t, reader)

		// Entered phases recorded exactly once.
		require.Equal(t, uint64(1), counts[mResolveBatch], "resolve_batch was entered and must record")
		require.Equal(t, uint64(1), counts[mOrdersPreparation], "orders_preparation was entered (and failed) and must record")

		// Phases never reached must record nothing (no spurious zero).
		require.Zero(t, counts[mScripts], "scripts was never entered")
		require.Zero(t, counts[mFSMFutureWait], "fsm_future.wait was never entered")
		require.Zero(t, counts[mResponseResolution], "response_resolution was never entered")
	})

	t.Run("failure in scripts records resolve_batch + orders_preparation + scripts only", func(t *testing.T) {
		t.Parallel()

		store := createTestStore(t)
		a, reader := createTestAdmissionWithReader(t, store)

		// A CreateTransaction referencing a numscript that does not exist fails in
		// resolveScriptsAndEnrichNeeds (resolveNumscriptReference) — i.e. during
		// the scripts phase, after orders_preparation has completed.
		_, err := a.Admit(context.Background(), servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: testLedgerName,
					Action: &servicepb.LedgerAction{
						Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								ScriptReference: &servicepb.ScriptReference{
									Name: "does-not-exist",
								},
							},
						},
					},
				},
			},
		}))
		require.Error(t, err)

		counts := recordedPhaseCounts(t, reader)

		// Entered phases recorded exactly once.
		require.Equal(t, uint64(1), counts[mResolveBatch], "resolve_batch was entered and must record")
		require.Equal(t, uint64(1), counts[mOrdersPreparation], "orders_preparation completed and must record")
		require.Equal(t, uint64(1), counts[mScripts], "scripts was entered (and failed) and must record")

		// Phases never reached must record nothing.
		require.Zero(t, counts[mFSMFutureWait], "fsm_future.wait was never entered")
		require.Zero(t, counts[mResponseResolution], "response_resolution was never entered")
	})

	t.Run("command.duration is recorded on every failure path", func(t *testing.T) {
		t.Parallel()

		store := createTestStore(t)
		a, reader := createTestAdmissionWithReader(t, store)

		_, err := a.Admit(context.Background(), servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: testLedgerName,
					Action: &servicepb.LedgerAction{
						Data: &servicepb.LedgerAction_RevertTransaction{
							RevertTransaction: &servicepb.RevertTransactionPayload{TransactionId: 0},
						},
					},
				},
			},
		}))
		require.Error(t, err)

		counts := recordedPhaseCounts(t, reader)
		// The total-latency histogram fires on every return; its population is the
		// superset the phase histograms above must stay aligned with.
		require.Equal(t, uint64(1), counts["admission.command.duration"],
			"command.duration must record on the failure path")
	})
}
