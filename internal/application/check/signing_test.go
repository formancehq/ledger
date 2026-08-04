package check

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// signingTestKey returns a deterministic public key of the right length, filled
// with a single distinguishable byte so a tampered row is obvious in a failure
// message and so the "never render key bytes" assertions have something
// unambiguous to look for.
func signingTestKey(seed byte) []byte {
	key := make([]byte, ed25519.PublicKeySize)
	for i := range key {
		key[i] = seed
	}

	return key
}

// writeSigningKey persists one SubGlobSigningKey row through the same helper the
// FSM commit path uses, so the row layout under test is the production one.
func writeSigningKey(t *testing.T, store *dal.Store, keyID string, publicKey []byte, parentKeyID string) {
	t.Helper()

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveSigningKey(batch, keyID, publicKey, parentKeyID))
	require.NoError(t, batch.Commit())
}

// writeRawSigningKeyRow plants a SubGlobSigningKey row verbatim, bypassing
// SaveSigningKey. Needed for the truncated-value case: SaveSigningKey always
// writes at least a full public key, so a short row can only be produced by
// direct disk access — which is exactly the tampering being modelled.
func writeRawSigningKeyRow(t *testing.T, store *dal.Store, keyID string, value []byte) {
	t.Helper()

	key := append([]byte{dal.ZoneGlobal, dal.SubGlobSigningKey}, keyID...)

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes(key, value))
	require.NoError(t, batch.Commit())
}

// writeSigningConfig persists the require-signatures flag the way the FSM does.
func writeSigningConfig(t *testing.T, store *dal.Store, requireSignatures bool) {
	t.Helper()

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveSigningConfig(batch, requireSignatures))
	require.NoError(t, batch.Commit())
}

// openSigningReader pins the snapshot the comparison reads.
//
// It MUST be called after every fixture write. Store.NewReadHandle is backed by
// a real Pebble snapshot, so a handle opened before the writes sees an empty
// store and every expected row reads as missing — a fixture bug that looks
// exactly like a compare bug.
func openSigningReader(t *testing.T, store *dal.Store) dal.PebbleReader {
	t.Helper()

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })

	return handle
}

// collectSigningErrors runs the comparison and returns the errors it emitted, in
// emission order. Every event a signing finding produces is an error event, so a
// non-error event is a bug rather than something to filter out.
func collectSigningErrors(t *testing.T, verifier *signingVerifier, reader dal.PebbleReader) []*servicepb.CheckStoreError {
	t.Helper()

	var got []*servicepb.CheckStoreError

	require.NoError(t, verifier.compare(reader, func(event *servicepb.CheckStoreEvent) {
		errEvent, ok := event.GetType().(*servicepb.CheckStoreEvent_Error)
		require.True(t, ok, "signing findings must be error events")

		got = append(got, errEvent.Error)
	}))

	return got
}

func registerSigningKeyOrder(keyID string, publicKey []byte, parentKeyID string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_RegisterSigningKey{
					RegisterSigningKey: &raftcmdpb.RegisterSigningKeyOrder{
						KeyId:       keyID,
						PublicKey:   publicKey,
						ParentKeyId: parentKeyID,
					},
				},
			},
		},
	}
}

func revokeSigningKeyOrder(keyID string, cascade bool) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_RevokeSigningKey{
					RevokeSigningKey: &raftcmdpb.RevokeSigningKeyOrder{
						KeyId:   keyID,
						Cascade: cascade,
					},
				},
			},
		},
	}
}

func setSigningConfigOrder(requireSignatures bool) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_SetSigningConfig{
					SetSigningConfig: &raftcmdpb.SetSigningConfigOrder{
						RequireSignatures: requireSignatures,
					},
				},
			},
		},
	}
}

// foldSigningOrders builds a verifier over the given orders. coldComplete is set
// so the fold-semantics tests are not entangled with the incomplete-coverage
// finding, which every zero-value verifier reports by design.
func foldSigningOrders(orders ...*raftcmdpb.Order) *signingVerifier {
	verifier := newSigningVerifier()
	verifier.coldComplete = true

	for _, order := range orders {
		verifier.applyOrder(order)
	}

	return verifier
}

// TestSigningVerifier_ApplyOrder pins the fold semantics against
// processing.processRegisterSigningKey / processRevokeSigningKey /
// processSetSigningConfig — the FSM handlers this expected-state model
// re-derives.
func TestSigningVerifier_ApplyOrder(t *testing.T) {
	t.Parallel()

	var (
		rootKey        = signingTestKey(0x01)
		childKey       = signingTestKey(0x02)
		grandchildKey  = signingTestKey(0x03)
		siblingKey     = signingTestKey(0x04)
		replacementKey = signingTestKey(0x05)
	)

	cases := []struct {
		name                  string
		orders                []*raftcmdpb.Order
		wantKeys              map[string]signingKeyExpectation
		wantRequireSignatures bool
	}{
		{
			name: "register root and child",
			orders: []*raftcmdpb.Order{
				registerSigningKeyOrder("root", rootKey, ""),
				registerSigningKeyOrder("child", childKey, "root"),
			},
			wantKeys: map[string]signingKeyExpectation{
				"root":  {publicKey: rootKey},
				"child": {publicKey: childKey, parentKeyID: "root"},
			},
		},
		{
			// The FSM rejects no duplicate key ID, so a second registration
			// legitimately replaces both the material and the parent link.
			name: "re-registration overwrites material and parent",
			orders: []*raftcmdpb.Order{
				registerSigningKeyOrder("root", rootKey, ""),
				registerSigningKeyOrder("child", childKey, "root"),
				registerSigningKeyOrder("child", replacementKey, ""),
			},
			wantKeys: map[string]signingKeyExpectation{
				"root":  {publicKey: rootKey},
				"child": {publicKey: replacementKey},
			},
		},
		{
			// Without cascade the descendants survive with a dangling parent
			// link, exactly as the FSM leaves them.
			name: "non-cascade revoke removes only the target",
			orders: []*raftcmdpb.Order{
				registerSigningKeyOrder("root", rootKey, ""),
				registerSigningKeyOrder("child", childKey, "root"),
				registerSigningKeyOrder("grandchild", grandchildKey, "child"),
				revokeSigningKeyOrder("child", false),
			},
			wantKeys: map[string]signingKeyExpectation{
				"root":       {publicKey: rootKey},
				"grandchild": {publicKey: grandchildKey, parentKeyID: "child"},
			},
		},
		{
			name: "cascade revoke removes the whole subtree",
			orders: []*raftcmdpb.Order{
				registerSigningKeyOrder("root", rootKey, ""),
				registerSigningKeyOrder("child", childKey, "root"),
				registerSigningKeyOrder("grandchild", grandchildKey, "child"),
				registerSigningKeyOrder("sibling", siblingKey, ""),
				revokeSigningKeyOrder("root", true),
			},
			wantKeys: map[string]signingKeyExpectation{
				"sibling": {publicKey: siblingKey},
			},
		},
		{
			name: "config toggles both ways",
			orders: []*raftcmdpb.Order{
				setSigningConfigOrder(true),
				setSigningConfigOrder(false),
				setSigningConfigOrder(true),
			},
			wantKeys:              map[string]signingKeyExpectation{},
			wantRequireSignatures: true,
		},
		{
			name: "revoking an unknown key is a no-op",
			orders: []*raftcmdpb.Order{
				registerSigningKeyOrder("root", rootKey, ""),
				revokeSigningKeyOrder("ghost", true),
			},
			wantKeys: map[string]signingKeyExpectation{
				"root": {publicKey: rootKey},
			},
		},
		{
			name: "orders that are not system-scoped signing orders are ignored",
			orders: []*raftcmdpb.Order{
				{
					Type: &raftcmdpb.Order_LedgerScoped{
						LedgerScoped: &raftcmdpb.LedgerScopedOrder{
							Ledger: "ledger-a",
							Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
								CreateLedger: &raftcmdpb.CreateLedgerOrder{},
							},
						},
					},
				},
				{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{}}},
				{},
				registerSigningKeyOrder("root", rootKey, ""),
			},
			wantKeys: map[string]signingKeyExpectation{
				"root": {publicKey: rootKey},
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			verifier := foldSigningOrders(testCase.orders...)

			require.Equal(t, testCase.wantKeys, verifier.keys)
			require.Equal(t, testCase.wantRequireSignatures, verifier.requireSignatures)
		})
	}
}

// TestSigningVerifier_ApplyOrderCopiesPublicKey pins that the fold does not
// alias the order's byte slice: an order may be reused or mutated after apply,
// and an aliased expectation would silently follow that mutation and compare a
// tampered row against itself.
func TestSigningVerifier_ApplyOrderCopiesPublicKey(t *testing.T) {
	t.Parallel()

	publicKey := signingTestKey(0x01)
	order := registerSigningKeyOrder("root", publicKey, "")

	verifier := foldSigningOrders(order)

	publicKey[0] = 0xFF

	require.Equal(t, signingTestKey(0x01), verifier.keys["root"].publicKey)
}

// TestSigningVerifier_CascadeIsOrderIndependent is the regression guard for the
// decision NOT to reproduce the FSM's child-traversal ordering: the reachable
// descendant set is fully determined by the parent relation, so the same
// hierarchy built in opposite registration orders must fold identically.
func TestSigningVerifier_CascadeIsOrderIndependent(t *testing.T) {
	t.Parallel()

	registrations := []*raftcmdpb.Order{
		registerSigningKeyOrder("root", signingTestKey(0x01), ""),
		registerSigningKeyOrder("branch-a", signingTestKey(0x02), "root"),
		registerSigningKeyOrder("branch-b", signingTestKey(0x03), "root"),
		registerSigningKeyOrder("leaf-a", signingTestKey(0x04), "branch-a"),
		registerSigningKeyOrder("leaf-b", signingTestKey(0x05), "branch-b"),
	}

	revokeRoot := revokeSigningKeyOrder("root", true)

	reversed := slices.Clone(registrations)
	slices.Reverse(reversed)

	// Both folds append onto a fresh clone, so neither can grow into the other's
	// backing array and reorder the fixture under the second run.
	forward := foldSigningOrders(append(slices.Clone(registrations), revokeRoot)...)
	backward := foldSigningOrders(append(reversed, revokeRoot)...)

	require.Empty(t, forward.keys)
	require.Empty(t, backward.keys)
	require.Equal(t, forward.keys, backward.keys)
}

// TestSigningVerifier_CascadeTerminatesOnAParentCycle pins the visited set in
// descendantsOf. A parent cycle is reachable through ordinary audited orders:
// registration is an upsert and neither admission nor the FSM validates that a
// parent exists or that the graph stays acyclic, so registering "a" under "b" and
// then "b" under "a" is accepted end to end. Without the visited set the BFS
// alternates between them forever, and the hang lands inside Check().
//
// A cascade revoke of either key must therefore remove the whole cycle and
// return. If this test ever hangs instead of failing, the visited set is gone.
func TestSigningVerifier_CascadeTerminatesOnAParentCycle(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		revoke string
	}{
		{name: "revoke one side of the cycle", revoke: "a"},
		{name: "revoke the other side", revoke: "b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// "a" parents "b" and "b" parents "a" — each is the other's descendant.
			verifier := foldSigningOrders(
				registerSigningKeyOrder("a", signingTestKey(0x01), "b"),
				registerSigningKeyOrder("b", signingTestKey(0x02), "a"),
				registerSigningKeyOrder("outside", signingTestKey(0x03), ""),
				revokeSigningKeyOrder(tc.revoke, true),
			)

			require.NotContains(t, verifier.keys, "a", "the cycle must be fully revoked")
			require.NotContains(t, verifier.keys, "b", "the cycle must be fully revoked")
			require.Contains(t, verifier.keys, "outside",
				"a cascade must not reach keys outside the revoked subtree")
		})
	}
}

// TestSigningVerifier_CascadeTerminatesOnASelfParent covers the degenerate cycle:
// one key registered as its own parent. descendantsOf seeds visited with the
// revoked key itself, so the key is never re-enqueued as its own child.
func TestSigningVerifier_CascadeTerminatesOnASelfParent(t *testing.T) {
	t.Parallel()

	verifier := foldSigningOrders(
		registerSigningKeyOrder("self", signingTestKey(0x01), "self"),
		revokeSigningKeyOrder("self", true),
	)

	require.Empty(t, verifier.keys)
}

// TestSigningVerifier_Compare walks the tamper classes the pass exists to
// detect. Each case seeds the audit-derived expectation by folding orders (never
// by poking the map, so the fold stays part of what is verified), then writes a
// possibly-divergent store, then compares.
func TestSigningVerifier_Compare(t *testing.T) {
	t.Parallel()

	var (
		rootKey     = signingTestKey(0x01)
		childKey    = signingTestKey(0x02)
		tamperedKey = signingTestKey(0xAA)
	)

	cases := []struct {
		name string
		// orders are the chain-bound orders the expectation is folded from.
		orders []*raftcmdpb.Order
		// coldIncomplete leaves the verifier's cold-coverage flag unset, which is
		// the fail-closed default a zero-value verifier carries.
		coldIncomplete bool
		// write lays down the persisted projection under judgement.
		write func(t *testing.T, store *dal.Store)
		// wantTypes is the exact emitted error-type sequence.
		wantTypes []servicepb.CheckStoreErrorType
		// wantSubstrings[i] must all appear in the i-th emitted message.
		wantSubstrings [][]string
		// wantAbsent must appear in no emitted message at all.
		wantAbsent []string
	}{
		{
			name: "clean store emits nothing",
			orders: []*raftcmdpb.Order{
				registerSigningKeyOrder("root", rootKey, ""),
				registerSigningKeyOrder("child", childKey, "root"),
			},
			write: func(t *testing.T, store *dal.Store) {
				writeSigningKey(t, store, "root", rootKey, "")
				writeSigningKey(t, store, "child", childKey, "root")
			},
		},
		{
			// An absent config row decodes as false, which is also the initial
			// expected value, so a store with no config row is clean.
			name:  "absent config row matches the initial expectation",
			write: func(_ *testing.T, _ *dal.Store) {},
		},
		{
			name: "injected key with no audited registration",
			orders: []*raftcmdpb.Order{
				registerSigningKeyOrder("root", rootKey, ""),
			},
			write: func(t *testing.T, store *dal.Store) {
				writeSigningKey(t, store, "root", rootKey, "")
				writeSigningKey(t, store, "ghost", tamperedKey, "root")
			},
			wantTypes: []servicepb.CheckStoreErrorType{
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH,
			},
			wantSubstrings: [][]string{{"ghost", "no audited registration"}},
		},
		{
			name: "audited key missing from the store",
			orders: []*raftcmdpb.Order{
				registerSigningKeyOrder("root", rootKey, ""),
				registerSigningKeyOrder("child", childKey, "root"),
			},
			write: func(t *testing.T, store *dal.Store) {
				writeSigningKey(t, store, "root", rootKey, "")
			},
			wantTypes: []servicepb.CheckStoreErrorType{
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH,
			},
			wantSubstrings: [][]string{{"child", "missing from the store"}},
		},
		{
			name: "public-key bytes replaced",
			orders: []*raftcmdpb.Order{
				registerSigningKeyOrder("root", rootKey, ""),
			},
			write: func(t *testing.T, store *dal.Store) {
				writeSigningKey(t, store, "root", tamperedKey, "")
			},
			wantTypes: []servicepb.CheckStoreErrorType{
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH,
			},
			wantSubstrings: [][]string{{"root", "public-key bytes"}},
			// The key ID plus the diverging field name is all an operator needs;
			// the material itself never belongs in an event message.
			wantAbsent: []string{
				hex.EncodeToString(rootKey),
				hex.EncodeToString(tamperedKey),
				fmt.Sprintf("%v", rootKey),
				fmt.Sprintf("%v", tamperedKey),
			},
		},
		{
			name: "parent re-pointed",
			orders: []*raftcmdpb.Order{
				registerSigningKeyOrder("root", rootKey, ""),
				registerSigningKeyOrder("child", childKey, "root"),
			},
			write: func(t *testing.T, store *dal.Store) {
				writeSigningKey(t, store, "root", rootKey, "")
				writeSigningKey(t, store, "child", childKey, "attacker")
			},
			wantTypes: []servicepb.CheckStoreErrorType{
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH,
			},
			wantSubstrings: [][]string{{"child", "parent", "attacker", "root"}},
		},
		{
			name:   "config byte flipped on",
			orders: []*raftcmdpb.Order{setSigningConfigOrder(false)},
			write: func(t *testing.T, store *dal.Store) {
				writeSigningConfig(t, store, true)
			},
			wantTypes: []servicepb.CheckStoreErrorType{
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_CONFIG_MISMATCH,
			},
			wantSubstrings: [][]string{{"require-signatures", "true", "false"}},
		},
		{
			name:   "config byte flipped off",
			orders: []*raftcmdpb.Order{setSigningConfigOrder(true)},
			write: func(t *testing.T, store *dal.Store) {
				writeSigningConfig(t, store, false)
			},
			wantTypes: []servicepb.CheckStoreErrorType{
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_CONFIG_MISMATCH,
			},
			wantSubstrings: [][]string{{"require-signatures", "false", "true"}},
		},
		{
			// A truncated row is two symptoms of one corruption: the row does not
			// decode, and the reader skips it so the audited key also reads as
			// absent. Both are reported, undecodable first.
			name: "row truncated below a full public key",
			orders: []*raftcmdpb.Order{
				registerSigningKeyOrder("root", rootKey, ""),
			},
			write: func(t *testing.T, store *dal.Store) {
				writeRawSigningKeyRow(t, store, "root", []byte{0x01, 0x02, 0x03})
			},
			wantTypes: []servicepb.CheckStoreErrorType{
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH,
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH,
			},
			wantSubstrings: [][]string{
				{"root", "undecodable", "shorter than an Ed25519 public key", "3"},
				{"root", "missing from the store"},
			},
		},
		{
			name:           "incomplete cold coverage is reported on an otherwise clean store",
			coldIncomplete: true,
			write:          func(_ *testing.T, _ *dal.Store) {},
			wantTypes: []servicepb.CheckStoreErrorType{
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_VERIFICATION_INCOMPLETE,
			},
			wantSubstrings: [][]string{{"could not be verified over the whole history"}},
		},
		{
			// Both directions of the key comparison are unsound under incomplete
			// coverage — a revoke in an unread chapter leaves its key expected, a
			// register in an unread chapter leaves its row unexpected — so a store
			// that would produce findings under complete coverage must produce only
			// the incomplete-coverage event. Reporting those findings would flag a
			// healthy store as tampered.
			name:           "incomplete cold coverage suppresses the unsound key comparison",
			coldIncomplete: true,
			orders: []*raftcmdpb.Order{
				registerSigningKeyOrder("root", rootKey, ""),
			},
			write: func(t *testing.T, store *dal.Store) {
				writeSigningKey(t, store, "ghost", tamperedKey, "")
			},
			wantTypes: []servicepb.CheckStoreErrorType{
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_VERIFICATION_INCOMPLETE,
			},
			wantSubstrings: [][]string{
				{"could not be verified over the whole history", "skipped for this run"},
			},
		},
		{
			// The malformed-row class is a fact about the row itself and needs no
			// audit oracle, so it survives the suppression above.
			name:           "incomplete cold coverage still reports undecodable rows",
			coldIncomplete: true,
			write: func(t *testing.T, store *dal.Store) {
				writeRawSigningKeyRow(t, store, "stub", []byte{0x01, 0x02})
			},
			wantTypes: []servicepb.CheckStoreErrorType{
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH,
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_VERIFICATION_INCOMPLETE,
			},
			wantSubstrings: [][]string{
				{"stub", "undecodable stored row"},
				{"could not be verified over the whole history"},
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			verifier := newSigningVerifier()
			verifier.coldComplete = !testCase.coldIncomplete

			for _, order := range testCase.orders {
				verifier.applyOrder(order)
			}

			store := createTestStore(t)
			testCase.write(t, store)

			got := collectSigningErrors(t, verifier, openSigningReader(t, store))

			gotTypes := make([]servicepb.CheckStoreErrorType, 0, len(got))
			for _, event := range got {
				gotTypes = append(gotTypes, event.GetErrorType())
			}

			require.Equal(t, testCase.wantTypes, nonEmptyErrorTypes(gotTypes), "emitted messages: %s", renderSigningMessages(got))
			require.Len(t, testCase.wantSubstrings, len(got), "one substring set per emitted event")

			for i, substrings := range testCase.wantSubstrings {
				for _, substring := range substrings {
					require.Contains(t, got[i].GetMessage(), substring)
				}
			}

			for _, event := range got {
				for _, absent := range testCase.wantAbsent {
					require.NotContains(t, event.GetMessage(), absent)
				}
			}
		})
	}
}

// signingChapter builds a chapter fixture with only the fields foldArchived
// reads. closeSequence is the last LOG sequence and closeAuditSequence the last
// AUDIT sequence: distinct fields, deliberately given different values here so a
// swap between them cannot pass unnoticed.
func signingChapter(id uint64, status commonpb.ChapterStatus, closeSequence, closeAuditSequence uint64) *commonpb.Chapter {
	return &commonpb.Chapter{
		Id:                 id,
		Status:             status,
		CloseSequence:      closeSequence,
		CloseAuditSequence: closeAuditSequence,
	}
}

// TestSigningVerifier_FoldArchivedCoverage pins the coverage verdict and the
// archive boundary foldArchived derives from the chapter list alone, with no cold
// reader: the boundary must come from CloseSequence (the log sequence the live
// fold compares against), never from CloseAuditSequence, and must be the maximum
// over the ARCHIVED chapters rather than the last one seen.
func TestSigningVerifier_FoldArchivedCoverage(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name              string
		chapters          []*commonpb.Chapter
		wantColdComplete  bool
		wantArchiveEndSeq uint64
	}{
		{
			// Nothing archived: the live audit range spans the whole history, so the
			// expectation is complete without reading cold storage at all.
			name: "no archived chapters means complete coverage",
			chapters: []*commonpb.Chapter{
				signingChapter(1, commonpb.ChapterStatus_CHAPTER_OPEN, 500, 400),
			},
			wantColdComplete: true,
		},
		{
			name:             "nil chapter list means complete coverage",
			chapters:         nil,
			wantColdComplete: true,
		},
		{
			// Archived history with no cold reader is the restore / CLI case: the
			// pre-boundary keys are unverifiable, which compare reports as
			// incomplete rather than clean.
			name: "one archived chapter without a cold reader is incomplete",
			chapters: []*commonpb.Chapter{
				signingChapter(1, commonpb.ChapterStatus_CHAPTER_ARCHIVED, 500, 400),
			},
			wantColdComplete:  false,
			wantArchiveEndSeq: 500,
		},
		{
			// The highest boundary wins, not the last one iterated — the list is not
			// ordered, so a last-seen assignment would leave the live fold re-applying
			// already-folded registers.
			name: "the boundary is the max close sequence across archived chapters",
			chapters: []*commonpb.Chapter{
				signingChapter(3, commonpb.ChapterStatus_CHAPTER_ARCHIVED, 900, 800),
				signingChapter(1, commonpb.ChapterStatus_CHAPTER_ARCHIVED, 1200, 1100),
				signingChapter(2, commonpb.ChapterStatus_CHAPTER_ARCHIVED, 700, 600),
			},
			wantColdComplete:  false,
			wantArchiveEndSeq: 1200,
		},
		{
			// A non-archived chapter's logs are still in the live range, so its close
			// sequence must not raise the boundary — doing so would make the live fold
			// skip orders nothing else folded.
			name: "only archived chapters contribute to the boundary",
			chapters: []*commonpb.Chapter{
				signingChapter(1, commonpb.ChapterStatus_CHAPTER_ARCHIVED, 400, 300),
				signingChapter(2, commonpb.ChapterStatus_CHAPTER_CLOSED, 800, 700),
				signingChapter(3, commonpb.ChapterStatus_CHAPTER_ARCHIVING, 900, 850),
				signingChapter(4, commonpb.ChapterStatus_CHAPTER_OPEN, 1500, 1400),
			},
			wantColdComplete:  false,
			wantArchiveEndSeq: 400,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			verifier := newSigningVerifier()

			require.NoError(t, verifier.foldArchived(context.Background(), testCase.chapters, nil, logging.Testing()))

			require.Equal(t, testCase.wantColdComplete, verifier.coldComplete)
			require.Equal(t, testCase.wantArchiveEndSeq, verifier.archiveEndSeq)
			require.Empty(t, verifier.keys, "a fold with no cold reader can register nothing")
		})
	}
}

// signingArchivedSST builds the SST an archived chapter would hold for the given
// audit items, keyed by (audit sequence, order index) exactly as the live store
// keys them. logSequence is carried on the item itself, which is what
// foldChapter's zero-sequence guard reads.
func signingArchivedSST(t *testing.T, orders map[uint64]*raftcmdpb.Order, logSequences map[uint64]uint64) []byte {
	t.Helper()

	items := make(map[uint64][]*auditpb.AuditItem, len(orders))

	for auditSeq, order := range orders {
		serialized, err := order.MarshalVT()
		require.NoError(t, err)

		items[auditSeq] = []*auditpb.AuditItem{{
			OrderIndex:      0,
			SerializedOrder: serialized,
			LogSequence:     logSequences[auditSeq],
		}}
	}

	return buildColdAuditSST(t, nil, items)
}

// TestSigningVerifier_FoldArchivedReadsColdStorage folds real archived chapters
// back through a ColdReader. The multi-chapter case is the reason the walk is
// ordered oldest-first: the revoke lives in the newer chapter and can only remove
// the key the older chapter registered if that registration was folded first.
func TestSigningVerifier_FoldArchivedReadsColdStorage(t *testing.T) {
	t.Parallel()

	const bucketID = "signing-cold-bucket"

	t.Run("register order in a single archived chapter", func(t *testing.T) {
		t.Parallel()

		sst := signingArchivedSST(t,
			map[uint64]*raftcmdpb.Order{
				1: registerSigningKeyOrder("archived-root", signingTestKey(0x01), ""),
				2: setSigningConfigOrder(true),
			},
			map[uint64]uint64{1: 10, 2: 11},
		)

		verifier := newSigningVerifier()
		coldReader := coldReaderWithChapters(t, bucketID, map[uint64][]byte{7: sst})

		require.NoError(t, verifier.foldArchived(context.Background(),
			[]*commonpb.Chapter{signingChapter(7, commonpb.ChapterStatus_CHAPTER_ARCHIVED, 20, 2)},
			coldReader, logging.Testing()))

		require.True(t, verifier.coldComplete)
		require.Equal(t, uint64(20), verifier.archiveEndSeq)
		require.Equal(t, map[string]signingKeyExpectation{
			"archived-root": {publicKey: signingTestKey(0x01)},
		}, verifier.keys)
		require.True(t, verifier.requireSignatures)
	})

	t.Run("a later chapter revokes a key registered by an earlier one", func(t *testing.T) {
		t.Parallel()

		older := signingArchivedSST(t,
			map[uint64]*raftcmdpb.Order{
				1: registerSigningKeyOrder("root", signingTestKey(0x01), ""),
				2: registerSigningKeyOrder("child", signingTestKey(0x02), "root"),
			},
			map[uint64]uint64{1: 10, 2: 11},
		)
		newer := signingArchivedSST(t,
			map[uint64]*raftcmdpb.Order{
				5: revokeSigningKeyOrder("root", true),
				6: registerSigningKeyOrder("successor", signingTestKey(0x03), ""),
			},
			map[uint64]uint64{5: 30, 6: 31},
		)

		verifier := newSigningVerifier()
		coldReader := coldReaderWithChapters(t, bucketID, map[uint64][]byte{3: older, 9: newer})

		// Listed newest-first so a fold that walked the list as given would apply
		// the revoke before the registration and leave both keys expected.
		require.NoError(t, verifier.foldArchived(context.Background(),
			[]*commonpb.Chapter{
				signingChapter(9, commonpb.ChapterStatus_CHAPTER_ARCHIVED, 40, 6),
				signingChapter(3, commonpb.ChapterStatus_CHAPTER_ARCHIVED, 20, 2),
			},
			coldReader, logging.Testing()))

		require.True(t, verifier.coldComplete)
		require.Equal(t, uint64(40), verifier.archiveEndSeq)
		require.Equal(t, map[string]signingKeyExpectation{
			"successor": {publicKey: signingTestKey(0x03)},
		}, verifier.keys)
	})

	t.Run("items with no log sequence are skipped", func(t *testing.T) {
		t.Parallel()

		// LogSequence 0 marks a failure-side item: it changed no state, so folding
		// it would manufacture a key the projection legitimately never held.
		sst := signingArchivedSST(t,
			map[uint64]*raftcmdpb.Order{
				1: registerSigningKeyOrder("rejected", signingTestKey(0x0F), ""),
			},
			map[uint64]uint64{1: 0},
		)

		verifier := newSigningVerifier()
		coldReader := coldReaderWithChapters(t, bucketID, map[uint64][]byte{4: sst})

		require.NoError(t, verifier.foldArchived(context.Background(),
			[]*commonpb.Chapter{signingChapter(4, commonpb.ChapterStatus_CHAPTER_ARCHIVED, 20, 1)},
			coldReader, logging.Testing()))

		require.True(t, verifier.coldComplete)
		require.Empty(t, verifier.keys)
	})

	t.Run("an unreadable chapter leaves the coverage incomplete", func(t *testing.T) {
		t.Parallel()

		// The reader holds chapter 4 only, so chapter 8's archive is missing: the
		// fold must report a gap instead of presenting chapter 4's keys as the whole
		// truth, and must not fail Check().
		sst := signingArchivedSST(t,
			map[uint64]*raftcmdpb.Order{1: registerSigningKeyOrder("root", signingTestKey(0x01), "")},
			map[uint64]uint64{1: 10},
		)

		verifier := newSigningVerifier()
		coldReader := coldReaderWithChapters(t, bucketID, map[uint64][]byte{4: sst})

		require.NoError(t, verifier.foldArchived(context.Background(),
			[]*commonpb.Chapter{
				signingChapter(4, commonpb.ChapterStatus_CHAPTER_ARCHIVED, 20, 1),
				signingChapter(8, commonpb.ChapterStatus_CHAPTER_ARCHIVED, 40, 5),
			},
			coldReader, logging.Testing()))

		require.False(t, verifier.coldComplete)
		require.Equal(t, uint64(40), verifier.archiveEndSeq)
	})
}

// nonEmptyErrorTypes normalizes an empty slice to nil so a case that expects no
// events can leave wantTypes unset.
func nonEmptyErrorTypes(types []servicepb.CheckStoreErrorType) []servicepb.CheckStoreErrorType {
	if len(types) == 0 {
		return nil
	}

	return types
}

func renderSigningMessages(events []*servicepb.CheckStoreError) string {
	messages := make([]string, 0, len(events))
	for _, event := range events {
		messages = append(messages, event.GetMessage())
	}

	return fmt.Sprintf("%q", messages)
}

// TestSigningVerifier_CompareEmissionIsDeterministic is the regression guard for
// the map-iteration nondeterminism: both sides of the comparison are Go maps, so
// without the sort-then-emit step two runs over the same store would produce
// different event streams and Check() output would not be reproducible.
func TestSigningVerifier_CompareEmissionIsDeterministic(t *testing.T) {
	t.Parallel()

	verifier := newSigningVerifier()
	for _, order := range []*raftcmdpb.Order{
		registerSigningKeyOrder("root", signingTestKey(0x01), ""),
		registerSigningKeyOrder("missing-a", signingTestKey(0x02), "root"),
		registerSigningKeyOrder("missing-b", signingTestKey(0x03), "root"),
		registerSigningKeyOrder("missing-c", signingTestKey(0x04), "root"),
		registerSigningKeyOrder("wrong-material", signingTestKey(0x05), "root"),
		registerSigningKeyOrder("wrong-parent", signingTestKey(0x06), "root"),
		setSigningConfigOrder(true),
	} {
		verifier.applyOrder(order)
	}

	store := createTestStore(t)
	writeSigningKey(t, store, "root", signingTestKey(0x01), "")
	writeSigningKey(t, store, "wrong-material", signingTestKey(0xAA), "root")
	writeSigningKey(t, store, "wrong-parent", signingTestKey(0x06), "attacker")
	writeSigningKey(t, store, "ghost-a", signingTestKey(0xAB), "")
	writeSigningKey(t, store, "ghost-b", signingTestKey(0xAC), "")
	writeRawSigningKeyRow(t, store, "truncated", []byte{0x01})
	writeSigningConfig(t, store, false)

	reader := openSigningReader(t, store)

	const runs = 5

	var reference []string

	for run := range runs {
		got := collectSigningErrors(t, verifier, reader)

		rendered := make([]string, 0, len(got))
		for _, event := range got {
			rendered = append(rendered, fmt.Sprintf("%d|%s", event.GetErrorType(), event.GetMessage()))
		}

		require.NotEmpty(t, rendered, "the fixture must produce divergences for this to prove anything")

		if run == 0 {
			reference = rendered

			continue
		}

		require.Equal(t, reference, rendered, "run %d diverged from the first run", run)
	}
}

// TestCheck_SigningProjections_EmptyAuditWiring pins that the comparison actually
// runs on the lastSequence == 0 path, which returns before the replay and
// therefore before the compare phase every other projection pass lives in.
//
// The signing projections are cluster-global, so a store with no logs can still
// hold rows — and since every successful signing order writes a log, a zero-log
// store proves the audit registered no key. An injected row there is unaudited by
// construction, and reporting it clean would leave the exact tamper class this
// pass exists to detect undetected on a freshly bootstrapped cluster.
func TestCheck_SigningProjections_EmptyAuditWiring(t *testing.T) {
	t.Parallel()

	runCheck := func(t *testing.T, seed func(*dal.Store)) []*servicepb.CheckStoreError {
		t.Helper()

		store := createTestStore(t)
		if seed != nil {
			seed(store)
		}

		// No readstore handle: the reverse-map pass on this same path skips itself
		// loudly, keeping the events attributable to the signing comparison alone.
		checker := NewChecker(store, attributes.New(), "test-cluster", nil, nil, nil, logging.Testing())

		var got []*servicepb.CheckStoreError

		require.NoError(t, checker.Check(context.Background(), func(event *servicepb.CheckStoreEvent) {
			if errEvent, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
				got = append(got, errEvent.Error)
			}
		}))

		return got
	}

	t.Run("injected signing key is reported", func(t *testing.T) {
		t.Parallel()

		got := runCheck(t, func(store *dal.Store) {
			writeSigningKey(t, store, "injected", signingTestKey(0x11), "")
		})

		require.Len(t, got, 1, "a signing key with no audited registration must be reported")
		require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH, got[0].GetErrorType())
		require.Contains(t, got[0].GetMessage(), "no audited registration")
		require.Contains(t, got[0].GetMessage(), "injected")
		require.NotContains(t, got[0].GetMessage(), hex.EncodeToString(signingTestKey(0x11)),
			"public-key bytes must never be rendered into an event message")
	})

	t.Run("tampered require-signatures flag is reported", func(t *testing.T) {
		t.Parallel()

		got := runCheck(t, func(store *dal.Store) {
			writeSigningConfig(t, store, true)
		})

		require.Len(t, got, 1, "a require-signatures flag with no audited order behind it must be reported")
		require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_CONFIG_MISMATCH, got[0].GetErrorType())
	})

	t.Run("untouched store stays clean", func(t *testing.T) {
		t.Parallel()

		require.Empty(t, runCheck(t, nil),
			"an empty store holds no signing rows, so the empty expectation must match it exactly")
	})
}
