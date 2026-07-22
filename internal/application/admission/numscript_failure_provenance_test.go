package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
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
