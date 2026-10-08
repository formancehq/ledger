package check

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	internalstatepb "github.com/formancehq/ledger/v3/internal/proto/internalstatepb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
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
		mutate      func(entry *ledgerpb.AuditEntry, items []*ledgerpb.AuditItem)
	}

	cases := []tamperCase{
		// AuditEntry header — top-level scalar fields.
		{"sequence", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) { e.Sequence = 999 }},
		{"timestamp", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
			e.Timestamp = &ledgerpb.Timestamp{Data: 1999999999}
		}},
		{"proposal_id", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) { e.ProposalId++ }},
		{"order_count", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) { e.OrderCount++ }},
		{"ledgers_add", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
			e.Ledgers = append(e.GetLedgers(), "ghost-ledger")
		}},
		{"ledgers_swap", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) { e.Ledgers = []string{"different-ledger"} }},
		{"hash_version", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) { e.HashVersion = 99 }},

		// Outcome flips — same `hash` field, different outcome semantics.
		{"outcome_flip_success_to_failure", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
			e.Outcome = &ledgerpb.AuditEntry_Failure{Failure: &ledgerpb.AuditFailure{Reason: ledgerpb.ErrorReason_ERROR_REASON_VALIDATION, Message: "fake"}}
		}},
		{"outcome_flip_failure_to_success", "failure", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
			e.Outcome = &ledgerpb.AuditEntry_Success{Success: &ledgerpb.AuditSuccess{MinLogSequence: 1, MaxLogSequence: 1}}
		}},

		// AuditSuccess sub-fields.
		{"success_min_log_sequence", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) { e.GetSuccess().MinLogSequence++ }},
		{"success_max_log_sequence", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) { e.GetSuccess().MaxLogSequence++ }},
		// AuditFailure sub-fields.
		{"failure_reason", "failure", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
			e.GetFailure().Reason = ledgerpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND
		}},
		{"failure_message", "failure", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) { e.GetFailure().Message = "tampered" }},
		{"failure_context_add", "failure", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
			e.GetFailure().GetContext()["new-key"] = "new-value"
		}},
		{"failure_context_value", "failure", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
			e.GetFailure().GetContext()["original-key"] = "changed"
		}},

		// CallerSnapshot sub-fields.
		{"caller_subject", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
			e.CallerSnapshot.GetAuthenticated().Identity.Subject = "attacker"
		}},
		{"caller_source_swap_to_issuer", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
			e.CallerSnapshot.GetAuthenticated().Identity.Source = &ledgerpb.CallerIdentity_Issuer{Issuer: "https://evil.example.com"}
		}},
		// Empty-string oneof variants must be distinguishable from
		// absent. These two cases pin that the source TAG (not just
		// the inner value) is bound in the envelope.
		{"caller_source_drop_to_nil", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
			e.CallerSnapshot.GetAuthenticated().Identity.Source = nil
		}},
		{"caller_source_swap_to_empty_issuer", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
			e.CallerSnapshot.GetAuthenticated().Identity.Source = &ledgerpb.CallerIdentity_Issuer{Issuer: ""}
		}},
		{"caller_god", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
			caller := e.GetCallerSnapshot().GetAuthenticated()
			caller.God = !caller.GetGod()
		}},
		{"caller_scopes_add", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
			caller := e.GetCallerSnapshot().GetAuthenticated()
			caller.Scopes = append(caller.GetScopes(), "admin")
		}},
		{"caller_principal_swap", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
			e.CallerSnapshot.Principal = &ledgerpb.CallerSnapshot_Anonymous{
				Anonymous: &ledgerpb.AnonymousCaller{Scopes: []string{"read", "write"}},
			}
		}},

		// Batch identity — bound into header_payload.
		{"idempotency_key", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) { e.Idempotency.Key = "tampered-key" }},
		{"signature_key_id", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) { e.Signature.KeyId = "evil-kid" }},
		{"signature_bytes", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) { e.Signature.Signature = []byte("forged") }},
		{"signature_payload", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) { e.Signature.Payload = []byte("swapped-batch") }},
		{"signature_drop_to_nil", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) { e.Signature = nil }},

		// AuditItem fields.
		{"item_order_index", "success", func(_ *ledgerpb.AuditEntry, items []*ledgerpb.AuditItem) { items[0].OrderIndex = 99 }},
		{"item_log_sequence", "success", func(_ *ledgerpb.AuditEntry, items []*ledgerpb.AuditItem) { items[0].LogSequence = 999 }},
		{"item_serialized_order", "success", func(_ *ledgerpb.AuditEntry, items []*ledgerpb.AuditItem) {
			items[0].SerializedOrder = []byte("tampered-order-bytes")
		}},

		// Smuggling: items embedded inside the persisted AuditEntry value
		// are NOT bound by the chain (the chain hashes items from their
		// own Pebble keys). The checker must flag any non-empty list on
		// the stored entry.
		{"embedded_items_in_entry_value", "success", func(e *ledgerpb.AuditEntry, items []*ledgerpb.AuditItem) {
			e.Items = []*ledgerpb.AuditItem{{OrderIndex: 99, SerializedOrder: []byte("smuggled-order")}}
		}},

		// Stripping the outcome leaves an entry that BuildHashedHeaderPayload
		// can no longer encode — the checker surfaces it as a mismatch
		// rather than silently re-hashing a half-built payload.
		{"outcome_wiped", "success", func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
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

func TestVerifyAuditHashChain_RejectsValidHashWithInvalidAttribution(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	const clusterID = "invalid-attribution-cluster"

	entry, items := newRichAuditEntry("success")
	entry.CallerSnapshot = &ledgerpb.CallerSnapshot{}
	// Compute a legitimate hash over the malformed replicated value. This pins
	// the semantic validation used by restore/check independently of tamper
	// detection: possession of a matching hash cannot legitimize attribution.
	persistAuditEntry(t, store, entry, items, clusterID)

	mismatches := runChainVerifier(t, store, clusterID)
	require.Len(t, mismatches, 1)
	require.Contains(t, mismatches[0].GetMessage(), "invalid caller attribution")
}

func TestVerifyAuditHashChain_InvalidAttributionPreservesHashChain(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	const clusterID = "invalid-attribution-chain-cluster"

	first, firstItems := newRichAuditEntry("success")
	first.CallerSnapshot = &ledgerpb.CallerSnapshot{}
	persistAuditEntry(t, store, first, firstItems, clusterID)

	second, secondItems := newRichAuditEntry("success")
	second.Sequence = 2
	second.Timestamp.Data++
	second.GetSuccess().MinLogSequence = 3
	second.GetSuccess().MaxLogSequence = 4
	secondItems[0].LogSequence = 3
	secondItems[1].LogSequence = 4
	persistAuditEntryAfter(t, store, second, secondItems, clusterID, first.GetHash())

	mismatches := runChainVerifier(t, store, clusterID)
	require.Len(t, mismatches, 1)
	require.Contains(t, mismatches[0].GetMessage(), "invalid caller attribution")
}

// newRichAuditEntry returns a fully-populated AuditEntry (sequence 1,
// realistic timestamps, two ledgers, caller snapshot with key_id source,
// either a success outcome with transient + purged maps or a failure
// outcome with a context map) paired with two AuditItems. Designed so
// every bound field is non-zero, so a tamper-by-zero is also a real
// mutation.
func newRichAuditEntry(outcomeKind string) (*ledgerpb.AuditEntry, []*ledgerpb.AuditItem) {
	entry := &ledgerpb.AuditEntry{
		Sequence:    1,
		Timestamp:   &ledgerpb.Timestamp{Data: 1700000000},
		ProposalId:  77,
		OrderCount:  2,
		Ledgers:     []string{"ledger-a", "ledger-b"},
		HashVersion: uint32(ledgerpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		CallerSnapshot: &ledgerpb.CallerSnapshot{
			Principal: &ledgerpb.CallerSnapshot_Authenticated{
				Authenticated: &ledgerpb.AuthenticatedCaller{
					Identity: &ledgerpb.CallerIdentity{
						Subject: "alice",
						Source:  &ledgerpb.CallerIdentity_KeyId{KeyId: "kid-1"},
					},
					Scopes: []string{"read", "write"},
					God:    false,
				},
			},
		},
		Idempotency: &ledgerpb.Idempotency{Key: "batch-key-1"},
		Signature: &ledgerpb.SignedApplyBatch{
			KeyId:     "sign-kid",
			Signature: []byte("sig-bytes"),
			Payload:   []byte("batch-payload"),
		},
	}

	switch outcomeKind {
	case "success":
		// Contiguous from sequence 1, which is the only range shape the
		// producer can emit for the first entry of a history: logBoundsVerifier
		// treats a hole below the range as a broken premise and suppresses the
		// bound, so a fixture starting at 100 would report incomplete coverage
		// on the untampered baseline run and mask the truncation wiring under
		// test.
		entry.Outcome = &ledgerpb.AuditEntry_Success{
			Success: &ledgerpb.AuditSuccess{
				MinLogSequence: 1,
				MaxLogSequence: 2,
			},
		}
	case "failure":
		entry.Outcome = &ledgerpb.AuditEntry_Failure{
			Failure: &ledgerpb.AuditFailure{
				Reason:  ledgerpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
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
	items := []*ledgerpb.AuditItem{
		{OrderIndex: 0, LogSequence: 1, SerializedOrder: richAuditOrder("ledger-a")},
		{OrderIndex: 1, LogSequence: 2, SerializedOrder: richAuditOrder("ledger-b")},
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
func persistAuditEntry(t *testing.T, store *dal.Store, entry *ledgerpb.AuditEntry, items []*ledgerpb.AuditItem, clusterID string) {
	persistAuditEntryAfter(t, store, entry, items, clusterID, nil)
}

func persistAuditEntryAfter(t *testing.T, store *dal.Store, entry *ledgerpb.AuditEntry, items []*ledgerpb.AuditItem, clusterID string, lastHash []byte) {
	t.Helper()
	_ = clusterID
	if entry.GetCallerSnapshot() == nil {
		entry.CallerSnapshot = testCallerSnapshot()
	}

	gen := processing.NewHashGenerator(ledgerpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, checkerTestAuditKey)

	headerPayload, err := state.BuildHashedHeaderPayload(entry)
	require.NoError(t, err)

	hashSlices := make([][]byte, 0, 1+len(items))
	hashSlices = append(hashSlices, headerPayload)

	for _, item := range items {
		hashSlices = append(hashSlices, state.BuildPerItemPayload(item))
	}

	_, entry.Hash = gen.Compute(nil, lastHash, hashSlices)

	rewriteAuditEntry(t, store, entry, items)
}

func testCallerSnapshot() *ledgerpb.CallerSnapshot {
	return commands.SystemCallerSnapshot(commands.ComponentClusterPolicy)
}

// rewriteAuditEntry writes entry + items at their canonical keys WITHOUT
// recomputing the hash. Used both to persist a legitimate entry (after
// persistAuditEntry has filled the hash) and to simulate a tampering
// adversary that does not bother recomputing the chain.
func rewriteAuditEntry(t *testing.T, store *dal.Store, entry *ledgerpb.AuditEntry, items []*ledgerpb.AuditItem) {
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

	collectIdempotencyMismatches := func(store *dal.Store) []*ledgerpb.CheckStoreError {
		attrs := attributes.New()
		checker := NewChecker(store, attrs, nil, logging.Testing())

		handle, err := store.NewReadHandle()
		require.NoError(t, err)

		defer func() { _ = handle.Close() }()

		var got []*ledgerpb.CheckStoreError

		_, err = checker.verifyAuditHashChain(context.Background(), handle, checkerTestAuditKey, newChainBoundState(), newChainVerifierFolds(), func(event *ledgerpb.CheckStoreEvent) {
			if e, ok := event.GetType().(*ledgerpb.CheckStoreEvent_Error); ok &&
				e.Error.GetErrorType() == ledgerpb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH {
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

	entry := &ledgerpb.AuditEntry{
		Sequence:    1,
		Timestamp:   &ledgerpb.Timestamp{Data: createdAt},
		ProposalId:  7,
		OrderCount:  1,
		HashVersion: uint32(ledgerpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		Idempotency: &ledgerpb.Idempotency{Key: idemKey},
		Outcome: &ledgerpb.AuditEntry_Failure{
			Failure: &ledgerpb.AuditFailure{
				Reason:  ledgerpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
				Message: "balance too low",
				Context: map[string]string{"account": "bank"},
			},
		},
	}
	items := []*ledgerpb.AuditItem{{OrderIndex: 0, SerializedOrder: serialized}}
	persistAuditEntry(t, store, entry, items, clusterID)

	faithful := &internalstatepb.IdempotencyKeyValue{
		CreatedAt: createdAt,
		Hash:      proposalHash,
		Failure: &internalstatepb.IdempotencyFailure{
			Reason:   ledgerpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
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
	tampered.Failure.Reason = ledgerpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND
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
func writeIdempotencyEntry(t *testing.T, store *dal.Store, key string, value *internalstatepb.IdempotencyKeyValue) {
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
func runChainVerifier(t *testing.T, store *dal.Store, clusterID string) []*ledgerpb.CheckStoreError {
	t.Helper()

	mismatches, _ := runChainVerifierWithFolds(t, store, clusterID)

	return mismatches
}

// runChainVerifierWithFolds is runChainVerifier plus the verifiers the walk
// folded into, so a test can assert on the coverage state the walk left behind
// and not only on the events it emitted.
func runChainVerifierWithFolds(
	t *testing.T,
	store *dal.Store,
	clusterID string,
) ([]*ledgerpb.CheckStoreError, chainVerifierFolds) {
	t.Helper()
	_ = clusterID

	attrs := attributes.New()
	checker := NewChecker(store, attrs, nil, logging.Testing())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	var mismatches []*ledgerpb.CheckStoreError

	folds := newChainVerifierFolds()

	// This test isolates HASH_MISMATCH; the idempotency TTL is irrelevant.
	_, err = checker.verifyAuditHashChain(context.Background(), handle, checkerTestAuditKey, newChainBoundState(), folds, func(event *ledgerpb.CheckStoreEvent) {
		if e, ok := event.GetType().(*ledgerpb.CheckStoreEvent_Error); ok && e.Error.GetErrorType() == ledgerpb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH {
			mismatches = append(mismatches, e.Error)
		}
	})
	require.NoError(t, err)

	return mismatches, folds
}

// TestVerifyAuditHashChain_MarksSigningFoldTruncatedOnEveryBreak pins the wiring
// between a chain break and the coverage state of every pass that folds inside
// that walk: signing, cluster policy and stored log bounds.
//
// verifyAuditHashChain returns early on a break so Check() can still report other
// projections, which leaves each expectation a PREFIX of the real history.
// Compared as-is the signing pass reports every registration past the break as
// injected and every revocation past it as a lost row, the cluster-policy pass
// compares a policy any later update may have replaced, and the log-bound pass
// compares a partial expectedMax against the stored head and emits a false
// LOG_UNAUDITED or SEQUENCE_GAP — false positives against a store whose only
// real problem is the break already reported as HASH_MISMATCH. Each early exit
// must therefore mark all three folds truncated; suppression itself is covered by
// the compare cases in signing_test.go, clusterpolicy_test.go and
// checker_log_bounds_test.go.
//
// One case per exit, because they sit at different points of the loop body: the
// embedded-items check runs before the entry's items are even read, while the
// other two run after.
func TestVerifyAuditHashChain_MarksSigningFoldTruncatedOnEveryBreak(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		mutate func(*ledgerpb.AuditEntry, []*ledgerpb.AuditItem)
	}{
		{
			name: "hash mismatch",
			mutate: func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
				e.Hash = []byte("not-the-real-hash")
			},
		},
		{
			// Wiping the outcome leaves an entry BuildHashedHeaderPayload can no
			// longer encode, which is the second exit.
			name: "header cannot be re-hashed",
			mutate: func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
				e.Outcome = nil
			},
		},
		{
			name: "items smuggled into the entry value",
			mutate: func(e *ledgerpb.AuditEntry, _ []*ledgerpb.AuditItem) {
				e.Items = []*ledgerpb.AuditItem{{OrderIndex: 99, SerializedOrder: []byte("smuggled-order")}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := createTestStore(t)
			clusterID := "truncation-cluster"

			entry, items := newRichAuditEntry("success")
			persistAuditEntry(t, store, entry, items, clusterID)

			// The untampered walk reaches the end of the range, so every fold is whole.
			_, clean := runChainVerifierWithFolds(t, store, clusterID)
			require.False(t, clean.signing.liveTruncated,
				"a chain that verifies to the end leaves the signing fold complete")
			require.False(t, clean.policy.liveTruncated,
				"a chain that verifies to the end leaves the cluster-policy fold complete")
			require.Empty(t, clean.bounds.incompleteReason,
				"a chain that verifies to the end leaves the log-bound derivation complete")

			tc.mutate(entry, items)
			rewriteAuditEntry(t, store, entry, items)

			mismatches, broken := runChainVerifierWithFolds(t, store, clusterID)
			require.NotEmpty(t, mismatches, "the break itself must still be reported")
			require.True(t, broken.signing.liveTruncated,
				"an early exit on a chain break must mark the signing fold truncated, or the pass compares a prefix")
			require.True(t, broken.policy.liveTruncated,
				"an early exit on a chain break must mark the cluster-policy fold truncated, or the pass compares a policy a later update may have replaced")
			// Matched on the chain-break wording, not merely on non-emptiness: the
			// contiguity guard in observeSuccess sets the same field, and this case
			// protects the markLiveTruncated exit specifically.
			require.Contains(t, broken.bounds.incompleteReason, "cut short by a hash chain break",
				"an early exit on a chain break must mark the log-bound derivation incomplete, or the pass compares a partial bound against the stored head")
		})
	}
}
