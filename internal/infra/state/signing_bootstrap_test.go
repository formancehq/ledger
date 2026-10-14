package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// attempt applies one batch like commit, but stops at the first rejected order
// and then discards the whole batch, as the FSM does for a rejected proposal.
// It returns the rejection, or nil when the batch was merged.
func (r *signingBatchRunner) attempt(orders ...*raftcmdpb.Order) domain.SerializableError {
	r.t.Helper()

	r.clock++

	writeSet := NewWriteSet(r.machine)
	writeSet.Reset(&commonpb.Timestamp{Data: r.clock})

	factory := NewScopeFactory(writeSet, nil, logging.Testing(), nil, 0)

	for _, order := range orders {
		scope, err := factory.NewScope(order.GetTechnical().GetCoverageBits())
		require.NoError(r.t, err)

		if _, describable := r.processor.ProcessOrder(order, scope); describable != nil {
			return describable
		}
	}

	batch := r.store.OpenWriteSession()
	require.NoError(r.t, writeSet.Merge(batch, nil))
	require.NoError(r.t, batch.Commit())

	return nil
}

// TestSigningBootstrapAdmitsASingleRootKey is the regression guard for Heal
// defect s3-concurrent-bootstrap-registers-two-root-keys-1.
//
// Admission lets a parent-less (unsigned) RegisterSigningKey through while its
// key store is empty, but it reads that store before the order is applied. Two
// unsigned registrations sent together are therefore both admitted and reach the
// FSM one after the other. Before the fix the FSM applied both, and the cluster
// ended up with two root keys. The FSM now re-checks the rule on replicated
// state, so only the first registration becomes a root.
func TestSigningBootstrapAdmitsASingleRootKey(t *testing.T) {
	t.Parallel()

	t.Run("a second root admitted concurrently is rejected at apply", func(t *testing.T) {
		t.Parallel()

		runner := newSigningBatchRunner(t)

		firstPub, _ := signingKeypair(t)
		secondPub, _ := signingKeypair(t)

		require.Nil(t, runner.attempt(registerOrder("first", firstPub, "")))

		err := runner.attempt(registerOrder("second", secondPub, ""))
		require.ErrorIs(t, err, domain.ErrUnsignedSigningKeyRegistration)

		require.ElementsMatch(t, []string{"first"}, runner.persistedKeyIDs())
		require.Nil(t, runner.machine.keyStore.GetPublicKey("second"))
	})

	t.Run("two roots in one unsigned batch are rejected", func(t *testing.T) {
		t.Parallel()

		runner := newSigningBatchRunner(t)

		firstPub, _ := signingKeypair(t)
		secondPub, _ := signingKeypair(t)

		err := runner.attempt(
			registerOrder("first", firstPub, ""),
			registerOrder("second", secondPub, ""),
		)
		require.ErrorIs(t, err, domain.ErrUnsignedSigningKeyRegistration)

		require.Empty(t, runner.persistedKeyIDs(), "a rejected proposal commits nothing")
	})

	t.Run("an unsigned re-registration of the root is rejected", func(t *testing.T) {
		t.Parallel()

		runner := newSigningBatchRunner(t)

		rootPub, _ := signingKeypair(t)
		replacementPub, _ := signingKeypair(t)

		require.Nil(t, runner.attempt(registerOrder("root", rootPub, "")))

		err := runner.attempt(registerOrder("root", replacementPub, ""))
		require.ErrorIs(t, err, domain.ErrUnsignedSigningKeyRegistration)

		require.Equal(t, rootPub, runner.machine.keyStore.GetPublicKey("root"),
			"an unsigned request must not replace the root key material")
	})

	t.Run("a signed registration under the root is still accepted", func(t *testing.T) {
		t.Parallel()

		runner := newSigningBatchRunner(t)

		rootPub, _ := signingKeypair(t)
		childPub, _ := signingKeypair(t)

		require.Nil(t, runner.attempt(registerOrder("root", rootPub, "")))
		require.Nil(t, runner.attempt(registerOrder("child", childPub, "root")))

		require.ElementsMatch(t, []string{"root", "child"}, runner.persistedKeyIDs())
	})

	t.Run("the bootstrap reopens once every key is revoked", func(t *testing.T) {
		t.Parallel()

		runner := newSigningBatchRunner(t)

		firstPub, _ := signingKeypair(t)
		nextPub, _ := signingKeypair(t)

		require.Nil(t, runner.attempt(registerOrder("first", firstPub, "")))
		require.Nil(t, runner.attempt(revokeOrder("first", true)))
		require.Nil(t, runner.attempt(registerOrder("next", nextPub, "")))

		require.ElementsMatch(t, []string{"next"}, runner.persistedKeyIDs())
	})
}
