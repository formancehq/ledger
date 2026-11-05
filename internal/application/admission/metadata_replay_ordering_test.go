package admission

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestAdmitMetadataReplayWaitsForPreviouslyProposedOutcome(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newMetadataReplayHarness(t)
		ctx := attributedTestContext(context.Background())
		h.pendingApplies = make(chan func(), 3)
		type outcome struct {
			result *domain.ApplyResult
			err    error
		}

		// Queue the tighter policy without applying it. The original request
		// still prepares against the old policy, but its real FSM application
		// will observe the tighter one and freeze a definitive failure.
		policy := proto.Clone(h.machine.State.ClusterPolicy).(*commonpb.ClusterPolicy)
		policy.Revision++
		policy.MetadataMaxValueBytes = 512
		policyDone := make(chan error, 1)
		go func() {
			_, err := h.admission.AdmitClusterPolicy(ctx, policy)
			policyDone <- err
		}()
		applyPolicy := <-h.pendingApplies
		request := metadataReplayRequest("in-flight", strings.Repeat("v", 1024))
		originalDone := make(chan outcome, 1)
		go func() {
			result, err := h.admission.Admit(ctx, request)
			originalDone <- outcome{result, err}
		}()
		applyOriginal := <-h.pendingApplies
		applyPolicy()
		require.NoError(t, <-policyDone)

		// The retry now fails the local limit check while its original is
		// proposed but still unapplied. The barrier must queue behind it.
		retryDone := make(chan outcome, 1)
		go func() {
			result, err := h.admission.Admit(ctx, request)
			retryDone <- outcome{result, err}
		}()
		applyBarrier := <-h.pendingApplies
		synctest.Wait()
		require.Empty(t, originalDone)
		require.Empty(t, retryDone, "retry must wait for durable apply, not just proposal acceptance")
		require.Len(t, h.proposals, 3)
		require.NotEmpty(t, h.proposals[1].GetOrders())
		require.Empty(t, h.proposals[2].GetOrders())
		require.Nil(t, h.proposals[2].GetIdempotency())

		applyOriginal()
		original := <-originalDone
		require.Nil(t, original.result)
		var limit *domain.ErrMetadataLimitExceeded
		require.ErrorAs(t, original.err, &limit)
		var originalFailure domain.SerializableError
		require.ErrorAs(t, original.err, &originalFailure)
		var replayed *domain.ReplayedFailure
		require.NotErrorAs(t, original.err, &replayed)
		audit, sequence := h.machine.State.NextAuditSequenceID, h.machine.State.NextSequenceID
		synctest.Wait()
		require.Empty(t, retryDone, "the original's commit does not complete the retry's barrier")

		applyBarrier()
		retry := <-retryDone
		require.Nil(t, retry.result)
		require.ErrorAs(t, retry.err, &replayed, "retry must return the retained FSM failure, not a fresh admission rejection")
		require.Equal(t, domain.ErrReasonMetadataLimitExceeded, replayed.Reason())
		require.Equal(t, domain.FailureFactsOf(originalFailure), domain.FailureFactsOf(replayed))
		require.Equal(t, audit, h.machine.State.NextAuditSequenceID)
		require.Equal(t, sequence, h.machine.State.NextSequenceID)
	})
}
