package admission

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func kvPutRequest(idempotencyKey, key string, value []byte) *servicepb.ApplyRequest {
	request := &servicepb.Request{Type: &servicepb.Request_PutKv{PutKv: &servicepb.PutKVRequest{
		Ledger: "metadata-replay", Key: key, Value: value,
	}}}

	return servicepb.UnsignedApplyRequest(idempotencyKey, request)
}

func TestKVPolicyAtApplyAfterIdempotencyLookup(t *testing.T) {
	t.Parallel()
	h := newMetadataReplayHarness(t)
	ctx := attributedTestContext(context.Background())
	original := kvPutRequest("retained-kv", "long-key", []byte("value"))
	first, err := h.admission.Admit(ctx, original)
	require.NoError(t, err)

	h.changePolicy(t, func(policy *commonpb.ClusterPolicy) {
		policy.KvMaxKeyBytes = 2
		policy.KvMaxValueBytes = 4
	})
	replayed, err := h.admission.Admit(ctx, original)
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, first.Logs[0].GetSequence(), replayed.Logs[0].GetSequence())

	_, err = h.admission.Admit(ctx, kvPutRequest("", "long-key", []byte("value")))
	require.ErrorIs(t, err, domain.ErrKVLimitExceeded)

	deleteRequest := &servicepb.Request{Type: &servicepb.Request_DeleteKv{DeleteKv: &servicepb.DeleteKVRequest{
		Ledger: "metadata-replay", Key: "long-key",
	}}}
	_, err = h.admission.Admit(ctx, servicepb.UnsignedApplyRequest("", deleteRequest))
	require.NoError(t, err)
}

func TestKVPolicyChangedBetweenProposalAndApply(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newMetadataReplayHarness(t)
		ctx := attributedTestContext(context.Background())
		h.pendingApplies = make(chan func(), 2)
		policy := proto.Clone(h.machine.State.ClusterPolicy).(*commonpb.ClusterPolicy)
		policy.Revision++
		policy.KvMaxValueBytes = 4
		policyDone := make(chan error, 1)
		go func() {
			_, err := h.admission.AdmitClusterPolicy(ctx, policy)
			policyDone <- err
		}()
		applyPolicy := <-h.pendingApplies
		putDone := make(chan error, 1)
		go func() {
			_, err := h.admission.Admit(ctx, kvPutRequest("", "key", []byte("value")))
			putDone <- err
		}()
		applyPut := <-h.pendingApplies
		applyPolicy()
		require.NoError(t, <-policyDone)
		applyPut()
		require.ErrorIs(t, <-putDone, domain.ErrKVLimitExceeded)
	})
}
