package admission

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestAdmitMetadataReplayApplyGuards(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"maintenance", "drain", "audit exhausted", "missing original log"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			h := newMetadataReplayHarness(t)
			ctx := attributedTestContext(context.Background())
			request := metadataReplayRequest("retained", strings.Repeat("x", 1024))
			original, err := h.admission.Admit(ctx, request)
			require.NoError(t, err)
			h.changePolicy(t, func(p *commonpb.ClusterPolicy) { p.MetadataMaxValueBytes = 512 })
			if stage == "audit exhausted" {
				h.machine.State.NextAuditSequenceID = math.MaxUint64
			}
			beforeAudit := h.machine.State.NextAuditSequenceID
			h.beforeApply = func(p *raftcmdpb.Proposal) {
				require.NotEmpty(t, p.GetBatch().GetOrders())
				switch stage {
				case "maintenance":
					h.admission.sharedState.SetMaintenanceMode(true)
				case "drain":
					h.machine.State.DrainState = &raftcmdpb.DrainState{Epoch: "shutdown", Index: 1, Members: []uint64{1}}
				}
			}
			h.afterApply = func(*raftcmdpb.Proposal) {
				if stage == "missing original log" {
					batch := h.admission.store.OpenWriteSession()
					key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(original.Logs[0].GetSequence()).Build()
					require.NoError(t, batch.DeleteKey(key))
					require.NoError(t, batch.Commit())
				}
			}
			result, err := h.admission.Admit(ctx, request)
			require.Nil(t, result)
			switch stage {
			case "maintenance", "drain":
				require.ErrorIs(t, err, domain.ErrMaintenanceMode)
			case "audit exhausted":
				var exhausted *domain.ErrSequenceExhausted
				require.ErrorAs(t, err, &exhausted)
			case "missing original log":
				require.ErrorContains(t, err, "references missing log")
			}
			require.Equal(t, beforeAudit, h.machine.State.NextAuditSequenceID)
			// A leaked response snapshot would block the exclusive lifecycle lock.
			require.NoError(t, h.admission.store.Close())
		})
	}
}

func TestAdmitMetadataReplayProposalFailures(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"propose", "acceptance", "apply", "cancellation", "store closed"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			h := newMetadataReplayHarness(t)
			ctx, cancel := context.WithCancel(attributedTestContext(context.Background()))
			defer cancel()
			request := metadataReplayRequest("retained", strings.Repeat("x", 1024))
			_, err := h.admission.Admit(ctx, request)
			require.NoError(t, err)
			h.changePolicy(t, func(p *commonpb.ClusterPolicy) { p.MetadataMaxValueBytes = 512 })
			sentinel := errors.New("injected proposal failure")
			originalProposer := h.admission.proposer
			proposer := NewMockProposer(gomock.NewController(t))
			proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, p *node.Proposal) (*futures.Future[state.ApplyResult], error) {
				if stage == "store closed" {
					future, err := originalProposer.Propose(ctx, p)
					require.NoError(t, err)
					require.NoError(t, h.admission.store.Close())

					return future, nil
				}
				if stage == "propose" {
					return nil, sentinel
				}
				future := futures.New[state.ApplyResult]()
				if stage == "acceptance" {
					p.Resolve(nil, sentinel)

					return future, nil
				}
				p.Resolve(nil, nil)
				if stage == "cancellation" {
					cancel()
				} else {
					future.Resolve(state.ApplyResult{}, sentinel)
				}

				return future, nil
			})
			h.admission.proposer = proposer
			beforeAudit := h.machine.State.NextAuditSequenceID
			result, err := h.admission.Admit(ctx, request)
			require.Nil(t, result)
			switch stage {
			case "cancellation":
				require.ErrorIs(t, err, context.Canceled)
			case "store closed":
				require.ErrorContains(t, err, "opening read handle for idempotent response")
			default:
				require.ErrorIs(t, err, sentinel)
			}
			require.Equal(t, beforeAudit, h.machine.State.NextAuditSequenceID)
		})
	}
}

func TestAdmitMetadataReplayDoesNotReplaceRetainedOutcome(t *testing.T) {
	t.Parallel()
	h := newMetadataReplayHarness(t)
	ctx := attributedTestContext(context.Background())
	request := metadataReplayRequest("retained", strings.Repeat("x", 1024))
	_, err := h.admission.Admit(ctx, request)
	require.NoError(t, err)
	h.changePolicy(t, func(p *commonpb.ClusterPolicy) { p.MetadataMaxValueBytes = 512 })
	handle, err := h.admission.store.NewReadHandle()
	require.NoError(t, err)
	before, err := state.LoadIdempotencyKey(handle, "retained")
	require.NoError(t, err)
	audit, err := query.ReadLastAuditEntry(handle)
	require.NoError(t, err)
	timestamp, err := query.ReadLastAppliedTimestamp(handle)
	require.NoError(t, err)
	require.NoError(t, handle.Close())
	_, err = h.admission.Admit(ctx, metadataReplayRequest("retained", strings.Repeat("y", 1024)))
	var conflict *domain.ErrIdempotencyKeyConflict
	require.ErrorAs(t, err, &conflict)
	result, err := h.admission.Admit(ctx, request)
	require.NoError(t, err)
	require.True(t, result.Replayed)
	handle, err = h.admission.store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	after, err := state.LoadIdempotencyKey(handle, "retained")
	require.NoError(t, err)
	require.Equal(t, before, after)
	afterAudit, err := query.ReadLastAuditEntry(handle)
	require.NoError(t, err)
	require.Equal(t, audit.GetSequence()+1, afterAudit.GetSequence())
	afterTimestamp, err := query.ReadLastAppliedTimestamp(handle)
	require.NoError(t, err)
	require.Greater(t, afterTimestamp, timestamp)
	volume, err := h.admission.attrs.Volume.Get(handle, domain.NewVolumeKey("metadata-replay", "alice", "USD/2", "").Bytes())
	require.NoError(t, err)
	require.Equal(t, commonpb.NewUint256FromUint64(100), volume.GetInput())
	require.Equal(t, commonpb.NewUint256FromUint64(0), volume.GetOutput())
}

func TestAdmitMetadataReplayAuditCapacityDoesNotDecodePayload(t *testing.T) {
	t.Parallel()
	h := newMetadataReplayHarness(t)
	ctx := attributedTestContext(context.Background())
	request := metadataReplayRequest("retained", strings.Repeat("x", 1024))
	original, err := h.admission.Admit(ctx, request)
	require.NoError(t, err)
	h.changePolicy(t, func(p *commonpb.ClusterPolicy) { p.MetadataMaxValueBytes = 512 })
	h.afterApply = func(*raftcmdpb.Proposal) {
		// The capacity gate needs only this key's sequence, not audit verification.
		// An opaque invalid value proves that it never decodes the historical body
		// (which can otherwise contain an unrelated, large signed request).
		key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(h.machine.State.NextAuditSequenceID - 1).Build()
		batch := h.admission.store.OpenWriteSession()
		require.NoError(t, batch.SetBytes(key, []byte{0xff}))
		require.NoError(t, batch.Commit())
	}
	replay, err := h.admission.Admit(ctx, request)
	require.NoError(t, err)
	require.True(t, replay.Replayed)
	require.Equal(t, original.Logs, replay.Logs)
	_, err = h.admission.Admit(ctx, metadataReplayRequest("new", strings.Repeat("x", 1024)))
	var limit *domain.ErrMetadataLimitExceeded
	require.ErrorAs(t, err, &limit)
}
