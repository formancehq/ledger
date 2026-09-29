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

// foldSigningOrders builds a verifier over the given orders.
// finding, which every zero-value verifier reports by design.
func foldSigningOrders(orders ...*raftcmdpb.Order) *signingVerifier {
	verifier := newSigningVerifier()

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
		// liveTruncated marks the fold as cut short, which is what
		// verifyAuditHashChain does at every non-error early exit on a chain break.
		liveTruncated bool
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
			// A hash chain break makes verifyAuditHashChain return before the end
			// of the range, so the expectation is a prefix — and unsound. The
			// store here is clean relative to the FULL history; only the fold is
			// short.
			name:          "a truncated fold is reported on an otherwise clean store",
			liveTruncated: true,
			write:         func(_ *testing.T, _ *dal.Store) {},
			wantTypes: []servicepb.CheckStoreErrorType{
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_VERIFICATION_INCOMPLETE,
			},
			wantSubstrings: [][]string{
				{"could not be verified over the whole history", "hash chain break", "skipped for this run"},
			},
		},
		{
			// The suppression that matters: without it these two rows report as
			// "injected" and "missing" against a store whose only real problem is
			// the chain break the caller already emitted a HASH_MISMATCH for.
			name:          "a truncated live fold suppresses the unsound key comparison",
			liveTruncated: true,
			orders: []*raftcmdpb.Order{
				registerSigningKeyOrder("root", rootKey, ""),
			},
			write: func(t *testing.T, store *dal.Store) {
				writeSigningKey(t, store, "past-the-break", tamperedKey, "")
			},
			wantTypes: []servicepb.CheckStoreErrorType{
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_VERIFICATION_INCOMPLETE,
			},
			wantSubstrings: [][]string{
				{"could not be verified over the whole history", "hash chain break"},
			},
		},
		{
			// The malformed-row class is a fact about the row itself and needs no
			// audit oracle, so it survives a truncated fold.
			name:          "a truncated live fold still reports undecodable rows",
			liveTruncated: true,
			write: func(t *testing.T, store *dal.Store) {
				writeRawSigningKeyRow(t, store, "stub", []byte{0x01, 0x02})
			},
			wantTypes: []servicepb.CheckStoreErrorType{
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH,
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_VERIFICATION_INCOMPLETE,
			},
			wantSubstrings: [][]string{
				{"stub", "undecodable stored row"},
				{"could not be verified over the whole history", "hash chain break"},
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			verifier := newSigningVerifier()
			verifier.liveTruncated = testCase.liveTruncated

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

// TestCheck_SigningProjections_EmptyAuditWiring pins that the comparison
// actually runs when the store holds no logs. It used to sit on a dedicated
// lastSequence == 0 branch that returned before the replay; EN-1526 removed that
// branch, so the assertion now guards the single normal-path call site.
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
		checker := NewChecker(store, attributes.New(), "test-cluster", nil, logging.Testing())

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

// TestSigningVerifier_CascadeFollowsTheEffectiveParent pins the cascade against
// the FSM's child relation, which resolves every key to the parent its LAST
// registration named.
//
// state.WriteSet.GetSigningKeyChildren folds pendingSigningKeyUpdates over the
// committed key store in slice order, so a key the proposal reparented is a child
// of its NEW parent and of nothing else. The replay must agree: cascading a key
// whose edge the proposal moved away would report a false SIGNING_KEY_MISMATCH
// against a store that legitimately kept it.
//
// The pairs below are the point of the test. Each scenario runs twice — once with
// the orders in one proposal, once split across two — and both must agree. The
// fold has no per-proposal state, and the FSM has none either once the effective
// parent is what decides, so a batch boundary cannot change which keys a cascade
// reaches.
func TestSigningVerifier_CascadeFollowsTheEffectiveParent(t *testing.T) {
	t.Parallel()

	var (
		parentKey = signingTestKey(0x01)
		otherKey  = signingTestKey(0x02)
		childKey  = signingTestKey(0x03)
	)

	// seedThreeKeys folds "parent", "other" and "child" (parented to "parent"), one
	// proposal each, so they are all committed before the proposal under test.
	seedThreeKeys := func() *signingVerifier {
		verifier := newSigningVerifier()

		for _, order := range []*raftcmdpb.Order{
			registerSigningKeyOrder("parent", parentKey, ""),
			registerSigningKeyOrder("other", otherKey, ""),
			registerSigningKeyOrder("child", childKey, "parent"),
		} {
			verifier.applyOrder(order)
		}

		return verifier
	}

	t.Run("a reparented key is not cascaded from its old parent", func(t *testing.T) {
		t.Parallel()

		for _, split := range []string{"one proposal", "two proposals"} {
			t.Run(split, func(t *testing.T) {
				t.Parallel()

				verifier := seedThreeKeys()

				verifier.applyOrder(registerSigningKeyOrder("child", childKey, "other"))
				verifier.applyOrder(revokeSigningKeyOrder("parent", true))

				require.NotContains(t, verifier.keys, "parent", "the revoke target must be gone")
				require.Contains(t, verifier.keys, "child",
					"the reassignment moved the key out of the revoked subtree, so the cascade must not reach it")
				require.Equal(t, "other", verifier.keys["child"].parentKeyID)
				require.Contains(t, verifier.keys, "other", "an unrelated root key must survive")
			})
		}
	})

	t.Run("a key re-registered as a root is not cascaded from its old parent", func(t *testing.T) {
		t.Parallel()

		verifier := seedThreeKeys()

		// The empty-parent variant. keystore.AddPublicKey CLEARS parents[keyID] when
		// the parent is empty, matching the parent-less persisted row, so the live
		// store and a restarted replica agree that the key is a root.
		verifier.applyOrder(registerSigningKeyOrder("child", childKey, ""))
		verifier.applyOrder(revokeSigningKeyOrder("parent", true))

		require.NotContains(t, verifier.keys, "parent")
		require.Contains(t, verifier.keys, "child",
			"a key re-registered as a root must not be cascaded from its old parent")
		require.Empty(t, verifier.keys["child"].parentKeyID,
			"re-registration with no parent must leave the key parentless")
		require.Contains(t, verifier.keys, "other")
	})

	// A key first registered inside the revoking proposal and then re-registered in
	// that same proposal: only the SECOND registration decides. The first edge is
	// superseded, so it contributes nothing to the cascade.
	for _, tc := range []struct {
		name       string
		reregister *raftcmdpb.Order
		newParent  string
	}{
		{
			name:       "a root re-registration",
			reregister: registerSigningKeyOrder("fresh", childKey, ""),
			newParent:  "",
		},
		{
			name:       "a reparenting re-registration",
			reregister: registerSigningKeyOrder("fresh", childKey, "other"),
			newParent:  "other",
		},
	} {
		t.Run("an in-proposal edge superseded by "+tc.name+" does not cascade", func(t *testing.T) {
			t.Parallel()

			verifier := seedThreeKeys()

			verifier.applyOrder(registerSigningKeyOrder("fresh", childKey, "parent"))
			verifier.applyOrder(tc.reregister)
			verifier.applyOrder(revokeSigningKeyOrder("parent", true))

			require.NotContains(t, verifier.keys, "parent", "the revoke target must be gone")
			require.Contains(t, verifier.keys, "fresh",
				"only the last registration decides the parent, so the superseded edge cannot cascade")
			require.Equal(t, tc.newParent, verifier.keys["fresh"].parentKeyID)
			require.Contains(t, verifier.keys, "other", "an unrelated root key must survive")
			require.NotContains(t, verifier.keys, "child",
				"the untouched committed child of the revoked parent is still cascaded")
		})
	}

	t.Run("an untouched committed child is cascaded", func(t *testing.T) {
		t.Parallel()

		verifier := seedThreeKeys()

		// The baseline the cases above are contrasted against: with no registration
		// moving it, "child" is resolved from the committed key store and cascaded.
		verifier.applyOrder(revokeSigningKeyOrder("parent", true))

		require.NotContains(t, verifier.keys, "parent")
		require.NotContains(t, verifier.keys, "child",
			"a cascade must still reach the committed children nothing reassigned")
		require.Contains(t, verifier.keys, "other")
	})
}

// TestSigningVerifier_CascadeSeesAReregisteredChild is the regression guard for
// EN-2011 on the replay side.
//
// A key revoked and then re-registered under the revoke target, all inside one
// signed batch, is a child of that target: state.WriteSet.GetSigningKeyChildren
// folds pendingSigningKeyUpdates in order, so the later registration supersedes
// the earlier removal and the cascade reaches the key. The replay must delete it
// too, or it reports a false unaudited-row finding against a store that correctly
// dropped it.
//
// Every scenario is run both as one proposal and as two, because the batch
// boundary must not change the outcome.
func TestSigningVerifier_CascadeSeesAReregisteredChild(t *testing.T) {
	t.Parallel()

	var (
		parentKey = signingTestKey(0x01)
		otherKey  = signingTestKey(0x02)
		childKey  = signingTestKey(0x03)
		grandKey  = signingTestKey(0x04)
	)

	// seedFourKeys commits "parent", "other", "child" (under "parent") and
	// "grandchild" (under "child") so the proposal under test starts from fully
	// committed state.
	seedFourKeys := func() *signingVerifier {
		verifier := newSigningVerifier()

		for _, order := range []*raftcmdpb.Order{
			registerSigningKeyOrder("parent", parentKey, ""),
			registerSigningKeyOrder("other", otherKey, ""),
			registerSigningKeyOrder("child", childKey, "parent"),
			registerSigningKeyOrder("grandchild", grandKey, "child"),
		} {
			verifier.applyOrder(order)
		}

		return verifier
	}

	// The re-registration's parent is what varies, and it is what decides: pointing
	// "child" back at the key being cascade-revoked puts it in the subtree, pointing
	// it anywhere else takes it out.
	for _, tc := range []struct {
		name      string
		newParent string
		cascaded  bool
	}{
		{name: "re-registered under the revoked parent", newParent: "parent", cascaded: true},
		{name: "re-registered under an unrelated parent", newParent: "other", cascaded: false},
		{name: "re-registered as a root", newParent: "", cascaded: false},
	} {
		t.Run("revoked then "+tc.name, func(t *testing.T) {
			t.Parallel()

			verifier := seedFourKeys()

			verifier.applyOrder(revokeSigningKeyOrder("child", false))
			verifier.applyOrder(registerSigningKeyOrder("child", childKey, tc.newParent))
			verifier.applyOrder(revokeSigningKeyOrder("parent", true))

			require.NotContains(t, verifier.keys, "parent", "the revoke target must be gone")
			require.Contains(t, verifier.keys, "other", "an unrelated root key must survive")

			if tc.cascaded {
				require.NotContains(t, verifier.keys, "child",
					"the re-registration put the key back under the revoke target, so the cascade must reach it")
				require.NotContains(t, verifier.keys, "grandchild",
					"and it must reach through it into the key's own subtree")

				return
			}

			require.Contains(t, verifier.keys, "child",
				"the re-registration moved the key out of the revoked subtree")
			require.Equal(t, tc.newParent, verifier.keys["child"].parentKeyID,
				"the surviving key keeps the parent its re-registration assigned")
			require.Contains(t, verifier.keys, "grandchild",
				"a surviving key keeps its own subtree")
		})
	}

	t.Run("splitting the batch does not change the outcome", func(t *testing.T) {
		t.Parallel()

		// The control the ticket calls for: the same ordered operations committed as
		// separate batches already removed the key before this fix, and must still.
		// One fold with no per-proposal state is what makes the two agree by
		// construction.
		verifier := seedFourKeys()

		verifier.applyOrder(revokeSigningKeyOrder("child", false))
		verifier.applyOrder(registerSigningKeyOrder("child", childKey, "parent"))
		verifier.applyOrder(revokeSigningKeyOrder("parent", true))

		require.NotContains(t, verifier.keys, "parent")
		require.NotContains(t, verifier.keys, "child")
		require.NotContains(t, verifier.keys, "grandchild")
		require.Contains(t, verifier.keys, "other")
	})

	t.Run("a cascade removal is superseded by a later re-registration", func(t *testing.T) {
		t.Parallel()

		verifier := seedFourKeys()

		// "grandchild" is removed as a descendant of "child", re-registered under
		// "other", and must then be reached by the cascade revoke of "other": the
		// registration is the last word on where it sits.
		verifier.applyOrder(revokeSigningKeyOrder("child", true))
		verifier.applyOrder(registerSigningKeyOrder("grandchild", grandKey, "other"))
		verifier.applyOrder(revokeSigningKeyOrder("other", true))

		require.NotContains(t, verifier.keys, "child", "the first revoke target must be gone")
		require.NotContains(t, verifier.keys, "other", "the second revoke target must be gone")
		require.NotContains(t, verifier.keys, "grandchild",
			"the re-registration put it under the second revoke target, so that cascade reaches it")
		require.Contains(t, verifier.keys, "parent", "an unrelated committed key must survive")
	})

	t.Run("a revoke does not exclude its own cascade targets", func(t *testing.T) {
		t.Parallel()

		verifier := seedFourKeys()

		// The walk runs before any removal is staged, so a plain cascade revoke still
		// reaches its whole committed subtree.
		verifier.applyOrder(revokeSigningKeyOrder("parent", true))

		require.NotContains(t, verifier.keys, "parent")
		require.NotContains(t, verifier.keys, "child",
			"a revoke's own cascade must still reach its committed children")
		require.NotContains(t, verifier.keys, "grandchild",
			"a revoke's own cascade must still reach transitively")
		require.Contains(t, verifier.keys, "other")
	})

	t.Run("a revoke with no re-registration keeps the key gone", func(t *testing.T) {
		t.Parallel()

		verifier := seedFourKeys()

		// The counterpart to the re-registration cases: with nothing putting "child"
		// back, the later cascade has no edge to follow and must not resurrect it.
		verifier.applyOrder(revokeSigningKeyOrder("child", false))
		verifier.applyOrder(revokeSigningKeyOrder("parent", true))

		require.NotContains(t, verifier.keys, "parent")
		require.NotContains(t, verifier.keys, "child", "the explicit revoke target stays gone")
		require.Contains(t, verifier.keys, "grandchild",
			"a non-cascade revoke does not remove the subtree, and the removed key carries no edge to walk")
		require.Contains(t, verifier.keys, "other")
	})
}
