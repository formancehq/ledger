package check

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestVerifyAuditHashChain_DetectsTampering pins the integrity promise of
// the envelope: mutating ANY bound field on disk after the entry was
// written must trip CHECK_STORE_ERROR_TYPE_HASH_MISMATCH on the next
// verification. One sub-test per field bound in HashedHeaderPayload or
// PerItemPayload.
//
// Each sub-test is fully isolated: a fresh store, a freshly-built rich
// AuditEntry + 2 AuditItems persisted via the production builders, then
// exactly one field is mutated and the entry/item is rewritten in place.
// We invoke `verifyAuditHashChain` directly (package-private access) so
// other Check() phases (replay, balances) cannot mask the
// mismatch event we are looking for.
func TestVerifyAuditHashChain_DetectsTampering(t *testing.T) {
	t.Parallel()

	type tamperCase struct {
		name        string
		outcomeKind string // "success" or "failure"
		mutate      func(entry *auditpb.AuditEntry, items []*auditpb.AuditItem)
	}

	cases := []tamperCase{
		// AuditEntry header — top-level scalar fields.
		{"sequence", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) { e.Sequence = 999 }},
		{"timestamp", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
			e.Timestamp = &commonpb.Timestamp{Data: 1999999999}
		}},
		{"proposal_id", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) { e.ProposalId++ }},
		{"order_count", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) { e.OrderCount++ }},
		{"ledgers_add", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
			e.Ledgers = append(e.GetLedgers(), "ghost-ledger")
		}},
		{"ledgers_swap", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) { e.Ledgers = []string{"different-ledger"} }},
		{"hash_version", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) { e.HashVersion = 99 }},

		// Outcome flips — same `hash` field, different outcome semantics.
		{"outcome_flip_success_to_failure", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
			e.Outcome = &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{Reason: commonpb.ErrorReason_ERROR_REASON_VALIDATION, Message: "fake"}}
		}},
		{"outcome_flip_failure_to_success", "failure", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
			e.Outcome = &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 1, MaxLogSequence: 1}}
		}},

		// AuditSuccess sub-fields.
		{"success_min_log_sequence", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) { e.GetSuccess().MinLogSequence++ }},
		{"success_max_log_sequence", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) { e.GetSuccess().MaxLogSequence++ }},
		// AuditFailure sub-fields.
		{"failure_reason", "failure", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
			e.GetFailure().Reason = commonpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND
		}},
		{"failure_message", "failure", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) { e.GetFailure().Message = "tampered" }},
		{"failure_context_add", "failure", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
			e.GetFailure().GetContext()["new-key"] = "new-value"
		}},
		{"failure_context_value", "failure", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
			e.GetFailure().GetContext()["original-key"] = "changed"
		}},

		// CallerSnapshot sub-fields.
		{"caller_subject", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) { e.CallerSnapshot.Identity.Subject = "attacker" }},
		{"caller_source_swap_to_issuer", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
			e.CallerSnapshot.Identity.Source = &commonpb.CallerIdentity_Issuer{Issuer: "https://evil.example.com"}
		}},
		// Empty-string oneof variants must be distinguishable from
		// absent. These two cases pin that the source TAG (not just
		// the inner value) is bound in the envelope.
		{"caller_source_drop_to_nil", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
			e.CallerSnapshot.Identity.Source = nil
		}},
		{"caller_source_swap_to_empty_issuer", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
			e.CallerSnapshot.Identity.Source = &commonpb.CallerIdentity_Issuer{Issuer: ""}
		}},
		{"caller_god", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
			e.CallerSnapshot.God = !e.GetCallerSnapshot().GetGod()
		}},
		{"caller_scopes_add", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
			e.CallerSnapshot.Scopes = append(e.CallerSnapshot.GetScopes(), "admin")
		}},

		// Batch identity — bound into header_payload.
		{"idempotency_key", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) { e.Idempotency.Key = "tampered-key" }},
		{"signature_key_id", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) { e.Signature.KeyId = "evil-kid" }},
		{"signature_bytes", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) { e.Signature.Signature = []byte("forged") }},
		{"signature_payload", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) { e.Signature.Payload = []byte("swapped-batch") }},
		{"signature_drop_to_nil", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) { e.Signature = nil }},

		// AuditItem fields.
		{"item_order_index", "success", func(_ *auditpb.AuditEntry, items []*auditpb.AuditItem) { items[0].OrderIndex = 99 }},
		{"item_log_sequence", "success", func(_ *auditpb.AuditEntry, items []*auditpb.AuditItem) { items[0].LogSequence = 999 }},
		{"item_serialized_order", "success", func(_ *auditpb.AuditEntry, items []*auditpb.AuditItem) {
			items[0].SerializedOrder = []byte("tampered-order-bytes")
		}},

		// Smuggling: items embedded inside the persisted AuditEntry value
		// are NOT bound by the chain (the chain hashes items from their
		// own Pebble keys). The checker must flag any non-empty list on
		// the stored entry.
		{"embedded_items_in_entry_value", "success", func(e *auditpb.AuditEntry, items []*auditpb.AuditItem) {
			e.Items = []*auditpb.AuditItem{{OrderIndex: 99, SerializedOrder: []byte("smuggled-order")}}
		}},

		// Stripping the outcome leaves an entry that BuildHashedHeaderPayload
		// can no longer encode — the checker surfaces it as a mismatch
		// rather than silently re-hashing a half-built payload.
		{"outcome_wiped", "success", func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
			e.Outcome = nil
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := createTestStore(t)
			clusterID := "tamper-cluster"

			entry, items := newRichAuditEntry(tc.outcomeKind)

			// Persist the legitimate envelope (hash matches).
			persistAuditEntry(t, store, entry, items, clusterID)

			// Sanity: the legitimate chain verifies without HASH_MISMATCH.
			require.Empty(t, runChainVerifier(t, store, clusterID),
				"baseline before tampering should have no HASH_MISMATCH events")

			// Apply the targeted mutation and rewrite the entry / items
			// at the same Pebble keys WITHOUT recomputing the hash. This
			// simulates an attacker with disk-level write access.
			tc.mutate(entry, items)
			rewriteAuditEntry(t, store, entry, items)

			// Chain verification must now flag the tampered entry.
			mismatches := runChainVerifier(t, store, clusterID)
			require.NotEmpty(t, mismatches,
				"mutation of field %q was not detected by verifyAuditHashChain — the field is outside the envelope", tc.name)
		})
	}
}

// newRichAuditEntry returns a fully-populated AuditEntry (sequence 1,
// realistic timestamps, two ledgers, caller snapshot with key_id source,
// either a success outcome with transient + purged maps or a failure
// outcome with a context map) paired with two AuditItems. Designed so
// every bound field is non-zero, so a tamper-by-zero is also a real
// mutation.
func newRichAuditEntry(outcomeKind string) (*auditpb.AuditEntry, []*auditpb.AuditItem) {
	entry := &auditpb.AuditEntry{
		Sequence:    1,
		Timestamp:   &commonpb.Timestamp{Data: 1700000000},
		ProposalId:  77,
		OrderCount:  2,
		Ledgers:     []string{"ledger-a", "ledger-b"},
		HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		CallerSnapshot: &commonpb.CallerSnapshot{
			Identity: &commonpb.CallerIdentity{
				Subject: "alice",
				Source:  &commonpb.CallerIdentity_KeyId{KeyId: "kid-1"},
			},
			Scopes: []string{"read", "write"},
			God:    false,
		},
		Idempotency: &commonpb.Idempotency{Key: "batch-key-1"},
		Signature: &signaturepb.SignedApplyBatch{
			KeyId:     "sign-kid",
			Signature: []byte("sig-bytes"),
			Payload:   []byte("batch-payload"),
		},
	}

	switch outcomeKind {
	case "success":
		entry.Outcome = &auditpb.AuditEntry_Success{
			Success: &auditpb.AuditSuccess{
				MinLogSequence: 100,
				MaxLogSequence: 101,
			},
		}
	case "failure":
		entry.Outcome = &auditpb.AuditEntry_Failure{
			Failure: &auditpb.AuditFailure{
				Reason:  commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
				Message: "balance too low",
				Context: map[string]string{
					"original-key": "original-value",
					"ledger":       "ledger-a",
				},
			},
		}
	default:
		panic("newRichAuditEntry: unknown outcomeKind " + outcomeKind)
	}

	// Real serialized Orders, not opaque marker bytes: the chain binds these
	// payloads either way, but verifyAuditHashChain decodes the items of a
	// chain-verified entry (signing fold) and treats an undecodable order as a
	// broken invariant. A fixture carrying non-proto bytes would trip that hard
	// failure on the pre-tampering baseline run and never reach the mutation
	// under test.
	items := []*auditpb.AuditItem{
		{OrderIndex: 0, LogSequence: 100, SerializedOrder: richAuditOrder("ledger-a")},
		{OrderIndex: 1, LogSequence: 101, SerializedOrder: richAuditOrder("ledger-b")},
	}

	return entry, items
}

// richAuditOrder builds the serialized business order an audit item carries. Its
// payload is non-empty on every bound field so a tamper-by-zero is still a real
// mutation of the hashed bytes.
func richAuditOrder(ledger string) []byte {
	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{},
				},
			},
		},
	}

	return order.MarshalDeterministicVT(nil)
}

// persistAuditEntry computes the envelope + chain hash via the production
// builders, assigns them on the entry, then writes the entry + items to
// Pebble at their canonical keys.
func persistAuditEntry(t *testing.T, store *dal.Store, entry *auditpb.AuditEntry, items []*auditpb.AuditItem, clusterID string) {
	t.Helper()

	gen := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, clusterID)

	headerPayload, err := state.BuildHashedHeaderPayload(entry)
	require.NoError(t, err)

	hashSlices := make([][]byte, 0, 1+len(items))
	hashSlices = append(hashSlices, headerPayload)

	for _, item := range items {
		hashSlices = append(hashSlices, state.BuildPerItemPayload(item))
	}

	_, entry.Hash = gen.Compute(nil, nil, hashSlices)

	rewriteAuditEntry(t, store, entry, items)
}

// rewriteAuditEntry writes entry + items at their canonical keys WITHOUT
// recomputing the hash. Used both to persist a legitimate entry (after
// persistAuditEntry has filled the hash) and to simulate a tampering
// adversary that does not bother recomputing the chain.
func rewriteAuditEntry(t *testing.T, store *dal.Store, entry *auditpb.AuditEntry, items []*auditpb.AuditItem) {
	t.Helper()

	batch := store.OpenWriteSession()
	batch.KeyBuilder.
		PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).
		PutUint64(entry.GetSequence())
	require.NoError(t, batch.SetProto(batch.KeyBuilder.Consume(), entry))

	for _, item := range items {
		batch.KeyBuilder.
			PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAuditItem).
			PutUint64(entry.GetSequence()).
			PutUint32(item.GetOrderIndex())
		require.NoError(t, batch.SetProto(batch.KeyBuilder.Consume(), item))
	}

	require.NoError(t, batch.Commit())
}

// TestVerifyAuditHashChain_DetectsIdempotencyOutcomeTampering covers the
// SubIdempKeys projection check: a frozen idempotency outcome is compared to
// the value re-derived from the hash-chained audit entry that wrote it. A
// faithful entry passes; tampering with the failure reason/message, the
// outcome kind, or the proposal hash is flagged — without breaking the chain.
func TestVerifyAuditHashChain_DetectsIdempotencyOutcomeTampering(t *testing.T) {
	t.Parallel()

	const (
		clusterID = "idem-outcome-cluster"
		idemKey   = "batch-key-1"
		createdAt = 1700000000
	)

	collectIdempotencyMismatches := func(store *dal.Store) []*servicepb.CheckStoreError {
		attrs := attributes.New()
		checker := NewChecker(store, attrs, clusterID, nil, logging.Testing())

		handle, err := store.NewReadHandle()
		require.NoError(t, err)

		defer func() { _ = handle.Close() }()

		var got []*servicepb.CheckStoreError

		_, err = checker.verifyAuditHashChain(context.Background(), handle, newChainBoundState(), newSigningVerifier(), newClusterPolicyVerifier(), func(event *servicepb.CheckStoreEvent) {
			if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok &&
				e.Error.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH {
				got = append(got, e.Error)
			}
		})
		require.NoError(t, err)

		return got
	}

	store := createTestStore(t)

	// A failure proposal that froze its outcome under idemKey: one real order,
	// its audit failure entry, and the matching SubIdempKeys projection.
	orders := []*raftcmdpb.Order{{}}
	serialized := orders[0].MarshalDeterministicVT(nil)
	proposalHash := processing.HashOrders(orders)

	entry := &auditpb.AuditEntry{
		Sequence:    1,
		Timestamp:   &commonpb.Timestamp{Data: createdAt},
		ProposalId:  7,
		OrderCount:  1,
		HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		Idempotency: &commonpb.Idempotency{Key: idemKey},
		Outcome: &auditpb.AuditEntry_Failure{
			Failure: &auditpb.AuditFailure{
				Reason:  commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
				Message: "balance too low",
				Context: map[string]string{"account": "bank"},
			},
		},
	}
	items := []*auditpb.AuditItem{{OrderIndex: 0, SerializedOrder: serialized}}
	persistAuditEntry(t, store, entry, items, clusterID)

	faithful := &commonpb.IdempotencyKeyValue{
		CreatedAt: createdAt,
		Hash:      proposalHash,
		Failure: &commonpb.IdempotencyFailure{
			Reason:   commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
			Message:  "balance too low",
			Metadata: map[string]string{"account": "bank"},
		},
	}

	writeIdempotencyEntry(t, store, idemKey, faithful)
	require.Empty(t, collectIdempotencyMismatches(store),
		"a frozen outcome matching its audit entry must not be flagged")

	tampered := faithful.CloneVT()
	tampered.Failure.Message = "you have plenty of money"
	writeIdempotencyEntry(t, store, idemKey, tampered)
	require.NotEmpty(t, collectIdempotencyMismatches(store),
		"a tampered frozen failure message must be flagged")

	tampered = faithful.CloneVT()
	tampered.Failure.Reason = commonpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND
	writeIdempotencyEntry(t, store, idemKey, tampered)
	require.NotEmpty(t, collectIdempotencyMismatches(store),
		"a tampered frozen failure reason must be flagged")

	tampered = faithful.CloneVT()
	tampered.Hash = []byte("forged-hash")
	writeIdempotencyEntry(t, store, idemKey, tampered)
	require.NotEmpty(t, collectIdempotencyMismatches(store),
		"a tampered proposal hash must be flagged")

	// The audit entry froze expires_at 0 (no policy TTL), so a stored entry
	// whose retention window was tampered to a non-zero value must be flagged.
	tampered = faithful.CloneVT()
	tampered.ExpiresAt = createdAt + 1_000
	writeIdempotencyEntry(t, store, idemKey, tampered)
	require.NotEmpty(t, collectIdempotencyMismatches(store),
		"a tampered frozen expires_at must be flagged")

	// Tampering created_at would otherwise dodge the (keyHash, created_at)
	// lookup and skip verification; the completeness guard reports it (no
	// audit entry froze the key at this timestamp).
	tampered = faithful.CloneVT()
	tampered.CreatedAt = createdAt + 5
	writeIdempotencyEntry(t, store, idemKey, tampered)
	require.NotEmpty(t, collectIdempotencyMismatches(store),
		"a live entry whose created_at matches no audit entry must be flagged")

	// The audit range covers the whole history, so a created_at below every
	// audit entry's timestamp is just as fabricated as any other miss.
	tampered = faithful.CloneVT()
	tampered.CreatedAt = 1
	writeIdempotencyEntry(t, store, idemKey, tampered)
	require.NotEmpty(t, collectIdempotencyMismatches(store),
		"an entry whose created_at precedes every audit entry must be flagged")
}

// writeIdempotencyEntry persists a frozen idempotency value at its canonical
// SubIdempKeys location (the layout state.SaveIdempotencyKey uses).
func writeIdempotencyEntry(t *testing.T, store *dal.Store, key string, value *commonpb.IdempotencyKeyValue) {
	t.Helper()

	keyHash := state.HashIdempotencyKey(key)

	pebbleKey := make([]byte, 2+16)
	pebbleKey[0] = dal.ZoneIdempotency
	pebbleKey[1] = dal.SubIdempKeys
	copy(pebbleKey[2:], keyHash[:])

	data, err := value.MarshalVT()
	require.NoError(t, err)

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes(pebbleKey, data))
	require.NoError(t, batch.Commit())
}

// runChainVerifier calls verifyAuditHashChain directly (package-private)
// and returns only the HASH_MISMATCH events. Other check phases are not
// exercised — this isolates the chain property under test.
func runChainVerifier(t *testing.T, store *dal.Store, clusterID string) []*servicepb.CheckStoreError {
	t.Helper()

	mismatches, _ := runChainVerifierWithSigning(t, store, clusterID)

	return mismatches
}

// runChainVerifierWithSigning is runChainVerifier plus the signing verifier the
// walk folded into, so a test can assert on the coverage state the walk left
// behind and not only on the events it emitted.
func runChainVerifierWithSigning(
	t *testing.T,
	store *dal.Store,
	clusterID string,
) ([]*servicepb.CheckStoreError, *signingVerifier) {
	t.Helper()

	attrs := attributes.New()
	checker := NewChecker(store, attrs, clusterID, nil, logging.Testing())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	var mismatches []*servicepb.CheckStoreError

	signing := newSigningVerifier()

	// This test isolates HASH_MISMATCH; the idempotency TTL is irrelevant.
	_, err = checker.verifyAuditHashChain(context.Background(), handle, newChainBoundState(), signing, newClusterPolicyVerifier(), func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok && e.Error.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH {
			mismatches = append(mismatches, e.Error)
		}
	})
	require.NoError(t, err)

	return mismatches, signing
}

// TestVerifyAuditHashChain_MarksSigningFoldTruncatedOnEveryBreak pins the wiring
// between a chain break and the signing pass's coverage state.
//
// verifyAuditHashChain returns early on a break so Check() can still report other
// projections, which leaves the signing expectation a PREFIX of the real history.
// Compared as-is it reports every registration past the break as injected and
// every revocation past it as a lost row — false positives against a store whose
// only real problem is the break already reported as HASH_MISMATCH. Each early
// exit must therefore mark the fold truncated; suppression itself is covered by
// the compare cases in signing_test.go.
//
// One case per exit, because they sit at different points of the loop body: the
// embedded-items check runs before the entry's items are even read, while the
// other two run after.
func TestVerifyAuditHashChain_MarksSigningFoldTruncatedOnEveryBreak(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		mutate func(*auditpb.AuditEntry, []*auditpb.AuditItem)
	}{
		{
			name: "hash mismatch",
			mutate: func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
				e.Hash = []byte("not-the-real-hash")
			},
		},
		{
			// Wiping the outcome leaves an entry BuildHashedHeaderPayload can no
			// longer encode, which is the second exit.
			name: "header cannot be re-hashed",
			mutate: func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
				e.Outcome = nil
			},
		},
		{
			name: "items smuggled into the entry value",
			mutate: func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) {
				e.Items = []*auditpb.AuditItem{{OrderIndex: 99, SerializedOrder: []byte("smuggled-order")}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := createTestStore(t)
			clusterID := "truncation-cluster"

			entry, items := newRichAuditEntry("success")
			persistAuditEntry(t, store, entry, items, clusterID)

			// The untampered walk reaches the end of the range, so the fold is whole.
			_, clean := runChainVerifierWithSigning(t, store, clusterID)
			require.False(t, clean.liveTruncated,
				"a chain that verifies to the end leaves the signing fold complete")

			tc.mutate(entry, items)
			rewriteAuditEntry(t, store, entry, items)

			mismatches, broken := runChainVerifierWithSigning(t, store, clusterID)
			require.NotEmpty(t, mismatches, "the break itself must still be reported")
			require.True(t, broken.liveTruncated,
				"an early exit on a chain break must mark the signing fold truncated, or the pass compares a prefix")
		})
	}
}
