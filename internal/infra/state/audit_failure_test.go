package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestIdempotencyFailureMessageMatchesAudit pins the equality the checker's
// idempotencyMismatch depends on: the reason and message frozen into the
// SubIdempKeys projection by recordIdempotencyFailure MUST equal the ones
// buildAuditFailure wrote into the hash-chained AuditFailure for the same
// error. On a drift a perfectly healthy store reports
// CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH on every frozen failure, and the
// audit side of the divergence is inside the hash chain, so it cannot be
// corrected afterwards.
//
// Both sites derive those two fields from describeFailure, so on that half the
// test is a forcing function against a future re-split of the derivation, not
// an independent oracle. The independent half is the round trip through
// state.IdempotencyValueFromAudit — the same derivation
// check.expectedIdempotencyOutcome builds its expectation with — which crosses
// the auditpb.AuditFailure to commonpb.IdempotencyFailure field mapping
// (Context to Metadata included) that describeFailure does not cover.
func TestIdempotencyFailureMessageMatchesAudit(t *testing.T) {
	t.Parallel()

	const (
		proposalCreatedAt = uint64(1700000000)
		idempotencyKey    = "idempotency-key-1"
	)

	for _, tc := range []struct {
		name string
		err  domain.Describable
	}{
		{
			// Every field distinct and non-zero, so a projection bug cannot
			// hide behind a zero value that both sides happen to share.
			name: "populated metadata",
			err: &domain.ErrInsufficientFunds{
				Account:    "user:alice",
				Asset:      "USD/2",
				Color:      "RESERVED",
				ColorKnown: true,
				Amount:     "100",
				Balance:    "10",
			},
		},
		{
			// Metadata() is nil here, the only shape that exercises the
			// nil-vs-empty asymmetry: buildAuditFailure emits a non-nil empty
			// Context while recordIdempotencyFailure stores nil.
			name: "nil metadata",
			err:  domain.NewValidationSentinel("EN-1772 fixture: value must not be empty"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.True(t, domain.IsFreezableFailure(domain.Kind(tc.err)),
				"the fixture must be freezable or recordIdempotencyFailure is a no-op and this test proves nothing")

			machine, dataStore, _ := newTestMachine(t)

			// Side A — the hash-chained audit entry, round-tripped through the
			// wire because that is how the checker reads it: a proto3 map with
			// no entries comes back nil, so the empty Context does not survive.
			entry := &auditpb.AuditEntry{
				Timestamp: &commonpb.Timestamp{Data: proposalCreatedAt},
				Outcome:   &auditpb.AuditEntry_Failure{Failure: buildAuditFailure(tc.err)},
			}

			raw, err := entry.MarshalVT()
			require.NoError(t, err)

			audited := &auditpb.AuditEntry{}
			require.NoError(t, audited.UnmarshalVT(raw))

			// Side B — the SubIdempKeys projection, read back out of Pebble
			// rather than off the cache, so the commit is load-bearing and the
			// serialization the checker walks is covered.
			batch := dataStore.OpenWriteSession()
			require.NoError(t, machine.recordIdempotencyFailure(
				batch, idempotencyKey, []byte("proposal-hash"), tc.err, proposalCreatedAt))
			require.NoError(t, batch.Commit())

			handle, err := dataStore.NewDirectReadHandle()
			require.NoError(t, err)
			t.Cleanup(func() { _ = handle.Close() })

			stored, err := LoadIdempotencyKey(handle, idempotencyKey)
			require.NoError(t, err)
			require.NotNil(t, stored.GetFailure(), "the failure outcome must have been frozen")

			// IdempotencyFailure.EqualVT compares metadata by length then by
			// key, so it carries the same nil-vs-empty tolerance as
			// checker.metadataEqual — no hand-maintained copy of that rule.
			expected, ok := IdempotencyValueFromAudit(audited, nil)
			require.True(t, ok, "a freezable failure must yield an expectation")
			require.True(t, expected.GetFailure().EqualVT(stored.GetFailure()),
				"audit chain %+v and idempotency projection %+v must carry the same reason, message and metadata",
				expected.GetFailure(), stored.GetFailure())
		})
	}
}
