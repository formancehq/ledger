package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// TestIdempotencyFailureMessageMatchesAudit pins the equality that
// checker.compareIdempotencyOutcomes (internal/application/check/checker.go:5399)
// depends on: the failure message frozen into the SubIdempKeys projection by
// recordIdempotencyFailure MUST be byte-equal to the one buildAuditFailure wrote
// into the hash-chained AuditFailure for the same error.
//
// The two are derived by separate functions in separate files. If they drift, a
// perfectly healthy store starts reporting CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH
// on every frozen failure, and the audit side of the divergence is inside the hash
// chain, so it cannot be corrected afterwards.
func TestIdempotencyFailureMessageMatchesAudit(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)

	// A freezable business rejection: recordIdempotencyFailure only freezes
	// outcomes for which domain.IsFreezableFailure(domain.Kind(d)) holds.
	bizErr := &domain.ErrInsufficientFunds{
		Account: "user:alice",
		Asset:   "USD",
		Amount:  "100",
		Balance: "10",
	}
	require.True(t, domain.IsFreezableFailure(domain.Kind(bizErr)),
		"the fixture must be freezable or recordIdempotencyFailure is a no-op and this test proves nothing")

	audited := buildAuditFailure(bizErr)

	const key = "idempotency-key-1"

	batch := dataStore.OpenWriteSession()
	require.NoError(t, machine.recordIdempotencyFailure(batch, key, []byte("proposal-hash"), bizErr, 1700000000))
	require.NoError(t, batch.Commit())

	stored, ok := machine.Registry.Idempotency.Get(key)
	require.True(t, ok, "the failure outcome must have been frozen")
	require.NotNil(t, stored.GetFailure())

	require.Equal(t, audited.GetMessage(), stored.GetFailure().GetMessage(),
		"audit chain and idempotency projection must carry the same failure message")
	require.Equal(t, audited.GetReason(), stored.GetFailure().GetReason(),
		"audit chain and idempotency projection must carry the same failure reason")

	// The nil-vs-empty metadata tolerance the checker relies on. Spelled out
	// here rather than by calling checker.metadataEqual (internal/application/check
	// imports internal/infra/state, so importing it back is an import cycle):
	// buildAuditFailure copies into a non-nil map while recordIdempotencyFailure
	// passes Metadata() through as-is. Keep this in agreement with
	// checker.go:5419 by hand.
	require.Len(t, audited.GetContext(), len(stored.GetFailure().GetMetadata()))

	for k, v := range stored.GetFailure().GetMetadata() {
		require.Equal(t, v, audited.GetContext()[k], "metadata key %q diverges", k)
	}
}
