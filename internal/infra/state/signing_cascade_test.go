package state

import (
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// signingKeypair returns a real Ed25519 keypair. The cascade decides who can
// still sign after a batch, so the assertions have to run against material a
// signature actually verifies under.
func signingKeypair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	return pub, priv
}

func registerOrder(keyID string, publicKey []byte, parentKeyID string) *raftcmdpb.Order {
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

func revokeOrder(keyID string, cascade bool) *raftcmdpb.Order {
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

// signingBatchRunner applies signing orders the way the FSM does: the real
// processor against a real WriteSet, then one Merge per batch. Going through
// Merge is the point — the defect was a disagreement between what the cascade
// read and what Merge replayed, so a test that only inspected staged updates
// could not see it.
type signingBatchRunner struct {
	t         *testing.T
	processor *processing.RequestProcessor
	machine   *Machine
	store     *dal.Store
	clock     uint64
}

func newSigningBatchRunner(t *testing.T) *signingBatchRunner {
	t.Helper()

	machine, store, _ := newTestMachine(t)

	processor, err := processing.NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	return &signingBatchRunner{t: t, processor: processor, machine: machine, store: store, clock: 1700000000}
}

// commit applies one batch: every order in sequence against a single WriteSet,
// then Merge. It returns the emitted log payloads so cascaded_key_ids — which is
// chain-hashed and consumed verbatim by the restore path — can be asserted.
func (r *signingBatchRunner) commit(orders ...*raftcmdpb.Order) []*commonpb.LogPayload {
	r.t.Helper()

	r.clock++

	writeSet := NewWriteSet(r.machine)
	writeSet.Reset(&commonpb.Timestamp{Data: r.clock})

	// The production factory rather than the bare WriteSet: orders reach the
	// engine through the coverage gate, and signing orders declare no coverage.
	factory := NewScopeFactory(writeSet, nil, logging.Testing(), nil, 0)

	payloads := make([]*commonpb.LogPayload, 0, len(orders))

	for _, order := range orders {
		scope, err := factory.NewScope(order.GetTechnical().GetCoverageBits())
		require.NoError(r.t, err)

		payload, describable := r.processor.ProcessOrder(order, scope)
		require.Nil(r.t, describable)
		payloads = append(payloads, payload)
	}

	batch := r.store.OpenWriteSession()
	require.NoError(r.t, writeSet.Merge(batch, nil))
	require.NoError(r.t, batch.Commit())

	return payloads
}

// authenticates reports whether a batch signed by privKey under keyID would be
// admitted, reproducing the two steps admission takes in resolveBatch: look the
// key ID up in the live key store, then verify the envelope against the public
// key that lookup returned. A revoked key fails at the lookup, which is why the
// signature being cryptographically sound is not enough to get in.
func (r *signingBatchRunner) authenticates(keyID string, privKey ed25519.PrivateKey) bool {
	r.t.Helper()

	envelope, err := signing.Sign(&servicepb.ApplyBatch{IdempotencyKey: "cascade-probe"}, keyID, privKey)
	require.NoError(r.t, err)

	pubKey := r.machine.keyStore.GetPublicKey(keyID)
	if pubKey == nil {
		return false
	}

	return signing.Verify(envelope, pubKey) == nil
}

// persistedKeyIDs reads the SubGlobSigningKey rows back out of Pebble. The live
// key store and the persisted rows are written by the same Merge loop and must
// agree; a restart rebuilds the store from these rows alone.
func (r *signingBatchRunner) persistedKeyIDs() []string {
	r.t.Helper()

	handle, err := r.store.NewReadHandle()
	require.NoError(r.t, err)

	defer func() { _ = handle.Close() }()

	rows, malformed, err := query.ReadSigningKeys(handle)
	require.NoError(r.t, err)
	require.Empty(r.t, malformed)

	ids := make([]string, 0, len(rows))
	for keyID := range rows {
		ids = append(ids, keyID)
	}

	return ids
}

// TestSigningCascadeReachesAReregisteredChild is the EN-2011 regression guard.
//
// Setup: P and C are committed, C parented to P. A single batch signed by P then
// carries, in order, a non-cascade revoke of C, a re-registration of C with fresh
// public key bytes — admission derives its parent from the batch signer, so it
// lands back under P — and a cascade revoke of P.
//
// The documented cascade contract is that revoking P removes P's whole descendant
// subtree. C is in that subtree when the cascade runs, because the registration
// supersedes the earlier removal, so C's replacement key must not survive the
// batch. Before the fix GetSigningKeyChildren excluded C on the strength of the
// removal alone and the cascade never reached it, leaving a working credential
// behind with an empty cascaded_key_ids to show for it.
func TestSigningCascadeReachesAReregisteredChild(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		split bool
	}{
		// The control the same three operations give when they are committed
		// separately — already correct before the fix, and the behaviour the single
		// batch has to match. The batch boundary must not decide who keeps a key.
		{name: "one batch", split: false},
		{name: "one batch per order", split: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			runner := newSigningBatchRunner(t)

			parentPub, parentPriv := signingKeypair(t)
			childPub, childPriv := signingKeypair(t)
			replacementPub, replacementPriv := signingKeypair(t)

			runner.commit(registerOrder("P", parentPub, ""))
			runner.commit(registerOrder("C", childPub, "P"))

			require.True(t, runner.authenticates("P", parentPriv), "P must be usable before the batch")
			require.True(t, runner.authenticates("C", childPriv), "C must be usable before the batch")

			orders := []*raftcmdpb.Order{
				revokeOrder("C", false),
				registerOrder("C", replacementPub, "P"),
				revokeOrder("P", true),
			}

			var payloads []*commonpb.LogPayload

			if tc.split {
				for _, order := range orders {
					payloads = append(payloads, runner.commit(order)...)
				}
			} else {
				payloads = runner.commit(orders...)
			}

			require.False(t, runner.authenticates("P", parentPriv),
				"the revoke target must no longer authenticate")
			require.False(t, runner.authenticates("C", replacementPriv),
				"C's replacement key must not survive a cascade revoke of its parent")
			require.False(t, runner.authenticates("C", childPriv),
				"and neither must the key the re-registration replaced")

			require.Empty(t, runner.persistedKeyIDs(),
				"the live key store and the persisted rows are written by one loop and must agree")

			cascaded := payloads[len(payloads)-1].GetRevokeSigningKey().GetCascadedKeyIds()
			require.Equal(t, []string{"C"}, cascaded,
				"cascaded_key_ids is chain-hashed and replayed verbatim on restore, so it must name C")
		})
	}
}

// restart boots a second Machine over the same store through the production
// recovery path, which repopulates the key store from the persisted signing rows
// and nothing else. It is what a replica that went down and came back sees.
func (r *signingBatchRunner) restart() *keystore.KeyStore {
	r.t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meterProvider := noop.NewMeterProvider()

	c, err := cache.New(1000, meterProvider.Meter("test"))
	require.NoError(r.t, err)

	registry := NewStateRegistry(c, attributes.New())
	recovered := keystore.NewKeyStore()

	machine, err := NewMachine(
		logger, registry, NewCacheSnapshotter(logger, registry, nil), r.store,
		dal.NewSentinelFactory(r.store, false), meterProvider, recovered,
		NewSharedState(), newNoopNotifier(r.t), nil, "test-cluster", 0, noopConfChangeHandler,
	)
	require.NoError(r.t, err)
	require.NoError(r.t, NewRecovery(machine, r.store).RecoverState())

	return recovered
}

// TestSigningCascadeSurvivesRestart pins that a replica which rebuilt its key
// store from the persisted rows agrees with one that never restarted.
//
// The cascade reads the key store, so a divergence here would be invariant #1 and
// #2: two replicas emitting different cascaded_key_ids for the same order. The
// restarted store is built by the real recovery path rather than asserted about,
// because the two representations are written by different code — Merge writes the
// live store and the Pebble row side by side, and recovery reads only the row back.
func TestSigningCascadeSurvivesRestart(t *testing.T) {
	t.Parallel()

	runner := newSigningBatchRunner(t)

	parentPub, parentPriv := signingKeypair(t)
	childPub, _ := signingKeypair(t)
	replacementPub, replacementPriv := signingKeypair(t)
	survivorPub, survivorPriv := signingKeypair(t)

	runner.commit(registerOrder("P", parentPub, ""))
	runner.commit(registerOrder("C", childPub, "P"))
	runner.commit(registerOrder("survivor", survivorPub, ""))

	runner.commit(
		revokeOrder("C", false),
		registerOrder("C", replacementPub, "P"),
		revokeOrder("P", true),
	)

	require.ElementsMatch(t, []string{"survivor"}, runner.persistedKeyIDs())

	recovered := runner.restart()

	for _, keyID := range []string{"P", "C"} {
		require.Nil(t, recovered.GetPublicKey(keyID),
			"a restarted replica must not resurrect %q", keyID)
		require.Nil(t, runner.machine.keyStore.GetPublicKey(keyID),
			"and the un-restarted replica must agree")
	}

	require.Equal(t, survivorPub, recovered.GetPublicKey("survivor"),
		"an unrelated root key survives the restart with its material intact")

	// The cascade walks the parent relation, so the two stores have to agree on
	// the edges too, not just on which keys exist.
	require.Empty(t, recovered.GetChildren("P"),
		"the revoked parent has no children left to cascade on a restarted replica")
	require.Equal(t, runner.machine.keyStore.GetChildren("P"), recovered.GetChildren("P"))
	require.Equal(t, runner.machine.keyStore.GetChildren("survivor"), recovered.GetChildren("survivor"))

	require.False(t, runner.authenticates("C", replacementPriv))
	require.False(t, runner.authenticates("P", parentPriv))
	require.True(t, runner.authenticates("survivor", survivorPriv),
		"an unrelated root key is untouched by the cascade")
}

// TestSigningCascadeFollowsTheEffectiveParent covers the rest of the ordered-fold
// contract: within one batch a key belongs to the parent its LAST registration
// named, and the cascade reaches exactly the subtree that implies.
//
// Each case is run as one batch and as one batch per order, because the effective
// parent is the only thing that decides — a batch boundary must not change the
// answer.
func TestSigningCascadeFollowsTheEffectiveParent(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// orders are applied after P, other and C (parented to P) are committed.
		orders func(replacement []byte) []*raftcmdpb.Order
		// survives lists the key IDs still usable once the batch is applied, beyond
		// the "other" root key, which every case keeps.
		survives []string
		cascaded []string
	}{
		{
			name: "an untouched committed child is cascaded",
			orders: func([]byte) []*raftcmdpb.Order {
				return []*raftcmdpb.Order{revokeOrder("P", true)}
			},
			survives: nil,
			cascaded: []string{"C"},
		},
		{
			name: "a child reparented in the batch leaves the subtree",
			orders: func(replacement []byte) []*raftcmdpb.Order {
				return []*raftcmdpb.Order{
					registerOrder("C", replacement, "other"),
					revokeOrder("P", true),
				}
			},
			survives: []string{"C"},
			cascaded: nil,
		},
		{
			// Admission only ever emits an empty ParentKeyId for the unsigned
			// bootstrap registration, but recovery and RebuildDelta both rebuild
			// roots from parent-less rows, so the FSM must resolve one the same way.
			name: "a child re-registered as a root leaves the subtree",
			orders: func(replacement []byte) []*raftcmdpb.Order {
				return []*raftcmdpb.Order{
					registerOrder("C", replacement, ""),
					revokeOrder("P", true),
				}
			},
			survives: []string{"C"},
			cascaded: nil,
		},
		{
			name: "a superseded in-batch edge does not cascade",
			orders: func(replacement []byte) []*raftcmdpb.Order {
				return []*raftcmdpb.Order{
					registerOrder("fresh", replacement, "P"),
					registerOrder("fresh", replacement, "other"),
					revokeOrder("P", true),
				}
			},
			survives: []string{"fresh"},
			cascaded: []string{"C"},
		},
		{
			name: "the last of several registrations decides",
			orders: func(replacement []byte) []*raftcmdpb.Order {
				return []*raftcmdpb.Order{
					registerOrder("fresh", replacement, "other"),
					registerOrder("fresh", replacement, "P"),
					revokeOrder("P", true),
				}
			},
			survives: nil,
			cascaded: []string{"C", "fresh"},
		},
		{
			name: "duplicate registrations under one parent are reported once",
			orders: func(replacement []byte) []*raftcmdpb.Order {
				return []*raftcmdpb.Order{
					registerOrder("fresh", replacement, "P"),
					registerOrder("fresh", replacement, "P"),
					revokeOrder("P", true),
				}
			},
			survives: nil,
			cascaded: []string{"C", "fresh"},
		},
		{
			name: "a re-registration under a descendant still terminates",
			orders: func(replacement []byte) []*raftcmdpb.Order {
				return []*raftcmdpb.Order{
					registerOrder("P", replacement, "C"),
					revokeOrder("P", true),
				}
			},
			survives: nil,
			cascaded: []string{"C"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			runner := newSigningBatchRunner(t)

			parentPub, _ := signingKeypair(t)
			otherPub, otherPriv := signingKeypair(t)
			childPub, _ := signingKeypair(t)
			replacementPub, replacementPriv := signingKeypair(t)

			runner.commit(registerOrder("P", parentPub, ""))
			runner.commit(registerOrder("other", otherPub, ""))
			runner.commit(registerOrder("C", childPub, "P"))

			payloads := runner.commit(tc.orders(replacementPub)...)

			expected := append([]string{"other"}, tc.survives...)
			require.ElementsMatch(t, expected, runner.persistedKeyIDs())

			require.True(t, runner.authenticates("other", otherPriv))

			for _, keyID := range tc.survives {
				require.True(t, runner.authenticates(keyID, replacementPriv),
					"%s must still authenticate under the key its last registration assigned", keyID)
			}

			cascaded := payloads[len(payloads)-1].GetRevokeSigningKey().GetCascadedKeyIds()
			require.Equal(t, tc.cascaded, cascaded,
				"cascaded_key_ids is chain-hashed, so its contents and its order are both pinned")
		})
	}
}

// TestSigningCascadeReachesThroughAReassignedParent pins that reassignment only
// saves a key when the new parent is outside the revoked subtree.
//
// Reassignment drops the old edge, so the key is no longer reached that way — but
// the walk is transitive. With "sibling" itself a child of the revoke target,
// moving "child" under "sibling" leaves it inside the subtree and the cascade
// arrives through the new edge instead of the old one.
func TestSigningCascadeReachesThroughAReassignedParent(t *testing.T) {
	t.Parallel()

	runner := newSigningBatchRunner(t)

	parentPub, _ := signingKeypair(t)
	siblingPub, siblingPriv := signingKeypair(t)
	childPub, _ := signingKeypair(t)
	replacementPub, replacementPriv := signingKeypair(t)

	runner.commit(registerOrder("P", parentPub, ""))
	runner.commit(registerOrder("sibling", siblingPub, "P"))
	runner.commit(registerOrder("C", childPub, "P"))

	payloads := runner.commit(
		registerOrder("C", replacementPub, "sibling"),
		revokeOrder("P", true),
	)

	require.Equal(t, []string{"sibling", "C"},
		payloads[len(payloads)-1].GetRevokeSigningKey().GetCascadedKeyIds(),
		"the cascade reaches the reassigned key through its new parent")

	require.False(t, runner.authenticates("C", replacementPriv),
		"a key moved to another key inside the revoked subtree does not escape the cascade")
	require.False(t, runner.authenticates("sibling", siblingPriv))
	require.Empty(t, runner.persistedKeyIDs())
}

// TestSigningCascadeTerminatesOnACycle pins termination against the graph
// registration can actually build. Nothing rejects a cycle — registration is an
// upsert that only shape-checks the two key IDs and never verifies the parent
// exists — and this walk runs inside Raft apply, so a non-terminating cascade
// would wedge every replica at once and replay on every restart.
func TestSigningCascadeTerminatesOnACycle(t *testing.T) {
	t.Parallel()

	runner := newSigningBatchRunner(t)

	aPub, _ := signingKeypair(t)
	bPub, _ := signingKeypair(t)

	runner.commit(registerOrder("a", aPub, ""))
	runner.commit(registerOrder("b", bPub, "a"))
	runner.commit(registerOrder("a", aPub, "b"))

	payloads := runner.commit(revokeOrder("a", true))

	require.Equal(t, []string{"b"}, payloads[0].GetRevokeSigningKey().GetCascadedKeyIds(),
		"each key in the cycle is revoked exactly once, and the target is never repeated")
	require.Empty(t, runner.persistedKeyIDs())
}

// TestSigningCascadeRollbackLeavesCommittedStateIntact pins that a batch which
// never reaches Merge changes nothing.
//
// The cascade stages its removals on the WriteSet, and Reset drops the pending
// slice wholesale, so an abandoned proposal must leave both the live key store
// and the persisted rows exactly as the last committed batch left them — and the
// next batch must see that committed state rather than the discarded one.
func TestSigningCascadeRollbackLeavesCommittedStateIntact(t *testing.T) {
	t.Parallel()

	runner := newSigningBatchRunner(t)

	parentPub, parentPriv := signingKeypair(t)
	childPub, childPriv := signingKeypair(t)
	replacementPub, replacementPriv := signingKeypair(t)

	runner.commit(registerOrder("P", parentPub, ""))
	runner.commit(registerOrder("C", childPub, "P"))

	// The EN-2011 batch, staged in full and then abandoned without a Merge.
	writeSet := NewWriteSet(runner.machine)
	writeSet.Reset(&commonpb.Timestamp{Data: 1700001000})

	factory := NewScopeFactory(writeSet, nil, logging.Testing(), nil, 0)

	for _, order := range []*raftcmdpb.Order{
		revokeOrder("C", false),
		registerOrder("C", replacementPub, "P"),
		revokeOrder("P", true),
	} {
		scope, err := factory.NewScope(order.GetTechnical().GetCoverageBits())
		require.NoError(t, err)

		_, describable := runner.processor.ProcessOrder(order, scope)
		require.Nil(t, describable)
	}

	require.NotEmpty(t, writeSet.pendingSigningKeyUpdates, "the batch staged updates")

	writeSet.Reset(&commonpb.Timestamp{Data: 1700002000})
	require.Empty(t, writeSet.pendingSigningKeyUpdates, "Reset drops the whole pending slice")

	require.True(t, runner.authenticates("P", parentPriv), "an abandoned batch revokes nothing")
	require.True(t, runner.authenticates("C", childPriv))
	require.False(t, runner.authenticates("C", replacementPriv),
		"and registers nothing either")
	require.ElementsMatch(t, []string{"P", "C"}, runner.persistedKeyIDs())

	// The next batch starts from the committed state, not the discarded one: the
	// plain cascade revoke of P must still find C under it.
	payloads := runner.commit(revokeOrder("P", true))

	require.Equal(t, []string{"C"}, payloads[0].GetRevokeSigningKey().GetCascadedKeyIds())
	require.Empty(t, runner.persistedKeyIDs())
}

// TestSigningCascadeFailedOrderStagesNothing pins that an order rejected by
// validation contributes no pending update.
//
// processRegisterSigningKey validates both key IDs before it touches the scope,
// so a rejected registration cannot leave a half-staged edge for a later cascade
// in the same batch to walk.
func TestSigningCascadeFailedOrderStagesNothing(t *testing.T) {
	t.Parallel()

	runner := newSigningBatchRunner(t)

	parentPub, _ := signingKeypair(t)
	childPub, _ := signingKeypair(t)
	replacementPub, _ := signingKeypair(t)

	runner.commit(registerOrder("P", parentPub, ""))
	runner.commit(registerOrder("C", childPub, "P"))

	writeSet := NewWriteSet(runner.machine)
	writeSet.Reset(&commonpb.Timestamp{Data: 1700001000})

	factory := NewScopeFactory(writeSet, nil, logging.Testing(), nil, 0)

	scope, err := factory.NewScope(nil)
	require.NoError(t, err)

	// A key ID the validator rejects — a control byte is outside the printable
	// ASCII range ValidateSigningKeyID requires — registered under the key about
	// to be cascade-revoked.
	_, describable := runner.processor.ProcessOrder(registerOrder("bad\x01id", replacementPub, "P"), scope)
	require.NotNil(t, describable, "the invalid key ID must be rejected")
	require.Empty(t, writeSet.pendingSigningKeyUpdates, "a rejected order stages nothing")

	payload, describable := runner.processor.ProcessOrder(revokeOrder("P", true), scope)
	require.Nil(t, describable)
	require.Equal(t, []string{"C"}, payload.GetRevokeSigningKey().GetCascadedKeyIds(),
		"the cascade sees the committed child and nothing from the rejected order")

	batch := runner.store.OpenWriteSession()
	require.NoError(t, writeSet.Merge(batch, nil))
	require.NoError(t, batch.Commit())

	require.Empty(t, runner.persistedKeyIDs())
}
