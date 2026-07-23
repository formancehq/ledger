package admission

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript/numscriptmock"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// missingVarScript fails dependency resolution deterministically WITHOUT reading
// any mutable state: the variable $m has no origin and no value is provided, so
// numscript's bindVars returns MissingVariableErr before any balance/metadata
// lookup is delegated to the store (MutableReadAttempted stays false).
const missingVarScript = `vars {
	monetary $m
}
send $m (
	source = @world
	destination = @dst
)`

// metaAbsentScript attempts a mutable metadata read (meta(@cfg,"dest")) and then
// fails resolution because the key is absent, so MutableReadAttempted is true.
const metaAbsentScript = `vars {
	account $dst = meta(@cfg, "dest")
}
send [USD 1] (
	source = @world
	destination = $dst
)`

// writeNumscriptRef persists a numscript content entry and advances the latest
// pointer so resolveNumscriptReference resolves both exact and latest selectors.
func writeNumscriptRef(t *testing.T, admission *Admission, ledger, name, version, content string) {
	t.Helper()

	batch := admission.store.OpenWriteSession()
	_, err := admission.attrs.NumscriptContent.Set(
		batch,
		domain.NumscriptEntryKey{LedgerName: ledger, Name: name, Version: version}.Bytes(),
		&commonpb.NumscriptInfo{Name: name, Content: content, Version: version},
	)
	require.NoError(t, err)
	_, err = admission.attrs.NumscriptVersion.Set(
		batch,
		domain.NumscriptVersionKey{LedgerName: ledger, Name: name}.Bytes(),
		&commonpb.NumscriptVersionValue{Version: version},
	)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

// referenceOrder builds a ledger-scoped CreateTransaction order that resolves a
// numscript by reference (name + version selector) instead of an inline script.
func referenceOrder(ledger, name, version string) *raftcmdpb.Order {
	return applyOrder(ledger, &raftcmdpb.LedgerApplyOrder{
		Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
			CreateTransaction: &raftcmdpb.CreateTransactionOrder{
				NumscriptReference: &raftcmdpb.NumscriptReference{Name: name, Version: version},
			},
		},
	})
}

// runResolveProvenance runs the extractPreloadNeeds + resolveScriptsAndEnrichNeeds
// pipeline the same way the real admission path does, returning the terminal
// error (nil when every order was forwarded or resolved).
func runResolveProvenance(t *testing.T, admission *Admission, orders []*raftcmdpb.Order, hasIdempotencyKey bool) error {
	t.Helper()

	needs, perOrder, err := admission.extractPreloadNeeds(context.Background(), orders)
	require.NoError(t, err)

	return admission.resolveScriptsAndEnrichNeeds(context.Background(), orders, newBulkOverlay(), needs, perOrder, hasIdempotencyKey)
}

// TestForwardOrFail_ProvenanceClassification covers the EN-1557 decision table:
// admission classifies a dependency-resolution failure by selector mutability
// (latest vs inline/exact) plus read-attempt provenance (did resolution consult
// mutable balance/metadata before failing).
func TestForwardOrFail_ProvenanceClassification(t *testing.T) {
	t.Parallel()

	// REGRESSION (the EN-1557 fix): an inline script with a deterministic no-read
	// failure and an idempotency key must TERMINATE — surfacing the cause as a
	// *domain.BusinessError — and must NOT be forwarded as retryable
	// PRELOAD_UNAVAILABLE, which would loop forever since no frozen outcome can
	// ever exist.
	t.Run("inline deterministic no-read with key terminates (no forward)", func(t *testing.T) {
		t.Parallel()

		admission, _ := createTestAdmission(t, createTestStore(t))
		order := scriptOrder(testLedgerName, missingVarScript)

		err := runResolveProvenance(t, admission, []*raftcmdpb.Order{order}, true)

		var businessErr *domain.BusinessError
		require.ErrorAs(t, err, &businessErr, "an inline deterministic no-read failure must surface a terminal business error")
		require.False(t, order.GetTechnical().GetPreloadUnavailable(),
			"the order must NOT be forwarded as PRELOAD_UNAVAILABLE — that is the loop this fix stops")
	})

	// REGRESSION (EN-1557, flemzord's blocker): an inline script that reads a
	// balance SUCCESSFULLY and only THEN hits an unsupported asset-scaling source
	// must TERMINATE even with an idempotency key. Resolution binds the balance()
	// origin (a successful read → MutableReadAttempted=true) before walking the
	// scaling statement, so the provenance flag alone would misclassify this as
	// state-dependent and forward it as PRELOAD_UNAVAILABLE — an unbounded loop,
	// since no state change can make a scaling source succeed. The fix classifies
	// scaling as a freezable validation failure so the freezable branch terminates
	// it before the provenance branch is ever reached.
	t.Run("inline read-then-scaling with key terminates (no forward)", func(t *testing.T) {
		t.Parallel()

		admission, _ := createTestAdmission(t, createTestStore(t))
		order := scriptOrder(testLedgerName, `
vars {
	monetary $amt = balance(@wallet, USD/2)
}
send $amt (
	source = @alice with scaling through @pool
	destination = @bob
)`)

		err := runResolveProvenance(t, admission, []*raftcmdpb.Order{order}, true)

		var businessErr *domain.BusinessError
		require.ErrorAs(t, err, &businessErr,
			"a deterministic scaling failure after a successful read must surface a terminal business error")
		require.ErrorIs(t, err, domain.ErrNumscriptScalingUnsupported,
			"the surfaced cause is the freezable scaling sentinel, not a retryable discovery error")
		require.False(t, order.GetTechnical().GetPreloadUnavailable(),
			"a read preceded the failure, but scaling is deterministic — it must NOT forward as PRELOAD_UNAVAILABLE (the loop this fix stops)")
	})

	// Contrast with the regression: a `latest` reference with the SAME no-read
	// failure IS forwarded under a key, because a previously-saved version may
	// hold the frozen outcome.
	t.Run("latest reference no-read with key forwards", func(t *testing.T) {
		t.Parallel()

		admission, _ := createTestAdmission(t, createTestStore(t))
		writeNumscriptRef(t, admission, testLedgerName, "txlatest", "1.0.0", missingVarScript)
		order := referenceOrder(testLedgerName, "txlatest", "latest")

		err := runResolveProvenance(t, admission, []*raftcmdpb.Order{order}, true)

		require.NoError(t, err, "a latest reference must be forwarded, not terminated")
		require.True(t, order.GetTechnical().GetPreloadUnavailable(),
			"a latest no-read failure with a key must be forwarded as PRELOAD_UNAVAILABLE")
	})

	t.Run("exact reference deterministic no-read with key terminates", func(t *testing.T) {
		t.Parallel()

		admission, _ := createTestAdmission(t, createTestStore(t))
		writeNumscriptRef(t, admission, testLedgerName, "txexact", "1.0.0", missingVarScript)
		order := referenceOrder(testLedgerName, "txexact", "1.0.0")

		err := runResolveProvenance(t, admission, []*raftcmdpb.Order{order}, true)

		var businessErr *domain.BusinessError
		require.ErrorAs(t, err, &businessErr, "an exact immutable version is deterministic and must terminate")
		require.False(t, order.GetTechnical().GetPreloadUnavailable())
	})

	// An inline/exact no-read failure is deterministic and terminates by surfacing
	// the cause whether or not a key is present — the idempotency key only matters
	// for the state-dependent and `latest` branches. This row pins that
	// key-independence: no key still terminates on the surfaced cause (the
	// numscript runtime error, NOT the retryable ErrDependencyDiscoveryFailed).
	t.Run("inline no-read without key terminates on surfaced cause", func(t *testing.T) {
		t.Parallel()

		admission, _ := createTestAdmission(t, createTestStore(t))
		order := scriptOrder(testLedgerName, missingVarScript)

		err := runResolveProvenance(t, admission, []*raftcmdpb.Order{order}, false)

		var businessErr *domain.BusinessError
		require.ErrorAs(t, err, &businessErr)
		var runtimeErr *domain.ErrNumscriptRuntime
		require.ErrorAs(t, err, &runtimeErr, "the terminal inline branch surfaces the real numscript cause")
		var discoveryErr *domain.ErrDependencyDiscoveryFailed
		require.NotErrorAs(t, err, &discoveryErr, "an inline no-read failure is never the retryable ErrDependencyDiscoveryFailed")
		require.False(t, order.GetTechnical().GetPreloadUnavailable())
	})

	t.Run("latest reference no-read without key fails fast", func(t *testing.T) {
		t.Parallel()

		admission, _ := createTestAdmission(t, createTestStore(t))
		writeNumscriptRef(t, admission, testLedgerName, "txlatestnokey", "1.0.0", missingVarScript)
		order := referenceOrder(testLedgerName, "txlatestnokey", "latest")

		err := runResolveProvenance(t, admission, []*raftcmdpb.Order{order}, false)

		var discoveryErr *domain.ErrDependencyDiscoveryFailed
		require.ErrorAs(t, err, &discoveryErr)
		require.False(t, order.GetTechnical().GetPreloadUnavailable())
	})

	t.Run("read-then-fail with key forwards", func(t *testing.T) {
		t.Parallel()

		admission, _ := createTestAdmission(t, createTestStore(t))
		order := scriptOrder(testLedgerName, metaAbsentScript)

		err := runResolveProvenance(t, admission, []*raftcmdpb.Order{order}, true)

		require.NoError(t, err, "a state-dependent (read-attempted) failure with a key must be forwarded")
		require.True(t, order.GetTechnical().GetPreloadUnavailable(),
			"resolution attempted a mutable metadata read, so with a key it forwards as PRELOAD_UNAVAILABLE")
	})

	t.Run("read-then-fail without key fails fast", func(t *testing.T) {
		t.Parallel()

		admission, _ := createTestAdmission(t, createTestStore(t))
		order := scriptOrder(testLedgerName, metaAbsentScript)

		err := runResolveProvenance(t, admission, []*raftcmdpb.Order{order}, false)

		var discoveryErr *domain.ErrDependencyDiscoveryFailed
		require.ErrorAs(t, err, &discoveryErr)
		require.False(t, order.GetTechnical().GetPreloadUnavailable())
	})
}

// TestClassifyResolutionFailure exercises classifyResolutionFailure directly with
// synthetic causes for the decision-table rows that are impractical to trigger
// through the real-store resolution path: a recovered numscript panic and context
// cancellation. Both must stay loud (terminal *domain.BusinessError, never
// PRELOAD_UNAVAILABLE) per EN-1557 and invariant #7, regardless of the idempotency
// key — and, for cancellation, even when a mutable read was attempted (the guard
// runs before the provenance branch).
func TestClassifyResolutionFailure(t *testing.T) {
	t.Parallel()

	admission, _ := createTestAdmission(t, createTestStore(t))

	// panicCause produces a GENUINE numscript panic error the real way: a value
	// source whose Balance panics, driven through DiscoverNumscriptDependencies so
	// the numscript recover path marks it. panicError is unexported and IsPanic
	// keys off it, so it cannot be fabricated in this package — it must come from
	// the real path.
	panicCause := func(t *testing.T) error {
		t.Helper()

		ctrl := gomock.NewController(t)
		source := numscriptmock.NewMockValueSource(ctrl)
		source.EXPECT().Balance(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
			func(string, string, string) (*big.Int, error) { panic("boom: numscript store panic") })
		source.EXPECT().Metadata(gomock.Any(), gomock.Any()).AnyTimes().Return("", false, nil)

		_, err := numscript.DiscoverNumscriptDependencies(
			admission.numscriptCache,
			`vars {
	monetary $x = balance(@a, USD)
}
send $x (
	source = @a
	destination = @b
)`,
			nil, testLedgerName, source, false,
		)
		require.Error(t, err)
		require.True(t, numscript.IsPanic(err), "the driven cause must be a recovered numscript panic")

		return err
	}

	t.Run("panic stays loud regardless of idempotency key", func(t *testing.T) {
		t.Parallel()

		for _, hasKey := range []bool{true, false} {
			cause := panicCause(t)
			order := scriptOrder(testLedgerName, "")

			forwarded, err := admission.classifyResolutionFailure(order, cause, false, hasKey)

			require.False(t, forwarded, "a panic must never be forwarded (hasKey=%v)", hasKey)
			var businessErr *domain.BusinessError
			require.ErrorAs(t, err, &businessErr)
			require.False(t, order.GetTechnical().GetPreloadUnavailable(),
				"a panic must never be softened to PRELOAD_UNAVAILABLE — invariant #7 (hasKey=%v)", hasKey)
		}
	})

	t.Run("cancellation stays loud even with read attempted and key", func(t *testing.T) {
		t.Parallel()

		for _, ctxErr := range []error{context.Canceled, context.DeadlineExceeded} {
			// refIsLatest=true, MutableReadAttempted=true and hasIdempotencyKey=true
			// would forward via the provenance branch — but the cancellation guard
			// runs first and must terminate loudly instead.
			cause := &numscript.DependencyResolutionError{Cause: ctxErr, MutableReadAttempted: true}
			order := scriptOrder(testLedgerName, "")

			forwarded, err := admission.classifyResolutionFailure(order, cause, true, true)

			require.False(t, forwarded, "cancellation must not forward even with a read attempted + key (%v)", ctxErr)
			var businessErr *domain.BusinessError
			require.ErrorAs(t, err, &businessErr)
			require.False(t, order.GetTechnical().GetPreloadUnavailable(),
				"a cancellation must never be softened to PRELOAD_UNAVAILABLE (%v)", ctxErr)
		}
	})

	t.Run("freezable validation surfaces the sentinel", func(t *testing.T) {
		t.Parallel()

		sentinel := domain.ErrEmptyTransaction
		cause := &numscript.DependencyResolutionError{Cause: sentinel, MutableReadAttempted: false}
		order := scriptOrder(testLedgerName, "")

		forwarded, err := admission.classifyResolutionFailure(order, cause, false, true)

		require.False(t, forwarded)
		var businessErr *domain.BusinessError
		require.ErrorAs(t, err, &businessErr)
		require.Equal(t, sentinel, businessErr.Err,
			"a freezable validation failure surfaces the real sentinel, not the generic discovery error")
		require.False(t, order.GetTechnical().GetPreloadUnavailable())
	})
}
