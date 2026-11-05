package admission

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// metadataReplayHarness drives real admission preloading and committed FSM/Pebble
// state. Only the Raft transport is replaced, by the generated proposer mock.
type metadataReplayHarness struct {
	admission  *Admission
	machine    *state.Machine
	apply      func(*raftcmdpb.Proposal) state.ApplyResult
	proposals  []*raftcmdpb.Proposal
	afterApply func(*raftcmdpb.Proposal)
	// When set, the test drives durable apply separately from proposal acceptance.
	pendingApplies chan func()
}

func newMetadataReplayHarness(t testing.TB) *metadataReplayHarness {
	t.Helper()
	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider()
	store, err := dal.NewStore(t.TempDir(), logger, meter.Meter("metadata-replay"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}, []byte("0123456789abcdef0123456789abcdef")))
	require.NoError(t, state.SaveClusterPolicy(batch, &commonpb.ClusterPolicy{
		Revision: 1, QueryCheckpointLimit: 10,
		MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
		MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes,
		MetadataMaxValueBytes:       domain.DefaultMetadataMaxValueBytes,
		MetadataMaxEntityBytes:      domain.DefaultMetadataMaxEntityBytes,
		MetadataMaxCommandBytes:     domain.DefaultMetadataMaxCommandBytes,
	}))
	require.NoError(t, batch.Commit())
	attrs := attributes.New()
	c, err := cache.New(1000, meter.Meter("metadata-replay"))
	require.NoError(t, err)
	registry := state.NewStateRegistry(c, attrs)
	ks := keystore.NewKeyStore()
	shared := state.NewSharedState()
	machine, err := state.NewMachine(logger, registry, state.NewCacheSnapshotter(logger, registry, nil), store, dal.NewSentinelFactory(store, false), meter, ks, shared, signal.NewNotifications(), nil, "test-cluster", 0, func(*raftpb.Entry, *dal.WriteSession) error { return nil })
	require.NoError(t, err)
	require.NoError(t, state.NewRecovery(machine, store).RecoverState())
	tracker := node.NewIndexTracker(1)
	var index uint64
	applyBytes := func(data []byte) state.ApplyResult {
		index++
		result, err := machine.ApplyEntries(context.Background(), store, &raftpb.Entry{Index: new(index), Term: proto.Uint64(1), Type: new(raftpb.EntryNormal), Data: data})
		require.NoError(t, err)
		require.Len(t, result.Results, 1)

		return result.Results[0]
	}
	h := &metadataReplayHarness{machine: machine}
	proposer := NewMockProposer(gomock.NewController(t))
	proposer.EXPECT().InitialIndex().Return(uint64(0)).AnyTimes()
	proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, p *node.Proposal) (*futures.Future[state.ApplyResult], error) {
		tracker.Increment(1)
		decoded := &raftcmdpb.Proposal{}
		require.NoError(t, proto.Unmarshal(p.Data(), decoded))
		h.proposals = append(h.proposals, decoded)
		future := futures.New[state.ApplyResult]()
		apply := func() {
			result := applyBytes(p.Data())
			if h.afterApply != nil {
				h.afterApply(decoded)
			}
			future.Resolve(result, result.Error)
		}
		if h.pendingApplies != nil {
			h.pendingApplies <- apply
		} else {
			apply()
		}
		p.Resolve(nil, nil)

		return future, nil
	}).AnyTimes()
	gate := health.NewMockWriteGate(gomock.NewController(t))
	gate.EXPECT().CheckWritesAllowed().Return(nil).AnyTimes()
	admission := NewAdmission(store, logger, proposer, plan.NewBuilder(tracker, c, attrs, store, nil, logger, 0), meter, gate, ks, shared, attrs, numscript.NewNumscriptCache(0), func(context.Context) error { return nil })
	h.admission = admission
	h.apply = func(p *raftcmdpb.Proposal) state.ApplyResult {
		p.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterPolicy)
		data, err := proto.Marshal(p)
		require.NoError(t, err)
		result := applyBytes(data)
		tracker.Advance(index + 1)

		return result
	}
	_, err = admission.Admit(attributedTestContext(context.Background()), &servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_Unsigned{Unsigned: &servicepb.ApplyBatch{Requests: []*servicepb.Request{{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: "metadata-replay"}}}}}}})
	require.NoError(t, err)
	h.proposals = nil

	return h
}

func metadataReplayRequest(key, value string) *servicepb.ApplyRequest {
	transaction := &servicepb.CreateTransactionPayload{
		Postings: []*commonpb.Posting{{Source: "world", Destination: "alice", Asset: "USD/2", Amount: commonpb.NewUint256FromUint64(100)}},
		Metadata: map[string]*commonpb.MetadataValue{"key": metadataStringValue(value)},
	}
	action := &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: transaction}}
	request := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "metadata-replay", Action: action}}}

	return &servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_Unsigned{Unsigned: &servicepb.ApplyBatch{IdempotencyKey: key, Requests: []*servicepb.Request{request}}}}
}

func TestAdmitMetadataPolicyTighteningReplaysCommittedOutcome(t *testing.T) {
	t.Parallel()
	h := newMetadataReplayHarness(t)
	ctx := attributedTestContext(context.Background())
	request := metadataReplayRequest("original", strings.Repeat("v", 1024))
	original, err := h.admission.Admit(ctx, request)
	require.NoError(t, err)
	require.False(t, original.Replayed)
	require.Len(t, original.Logs, 1)
	require.NotNil(t, original.Logs[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction())
	h.changePolicy(t, func(p *commonpb.ClusterPolicy) { p.MetadataMaxValueBytes = 512 })
	beforeAudit, beforeSequence := h.machine.State.NextAuditSequenceID, h.machine.State.NextSequenceID
	replay, err := h.admission.Admit(ctx, request)
	require.NoError(t, err)
	require.True(t, replay.Replayed)
	require.Equal(t, original.Logs, replay.Logs)
	require.Equal(t, beforeAudit, h.machine.State.NextAuditSequenceID)
	require.Equal(t, beforeSequence, h.machine.State.NextSequenceID)
	_, err = h.admission.Admit(ctx, metadataReplayRequest("new", strings.Repeat("v", 1024)))
	var limit *domain.ErrMetadataLimitExceeded
	require.ErrorAs(t, err, &limit)
	_, err = h.admission.Admit(ctx, metadataReplayRequest("original", strings.Repeat("w", 1024)))
	var conflict *domain.ErrIdempotencyKeyConflict
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, beforeAudit, h.machine.State.NextAuditSequenceID)
	require.Equal(t, beforeSequence, h.machine.State.NextSequenceID)
}

func (h *metadataReplayHarness) changePolicy(t testing.TB, change func(*commonpb.ClusterPolicy)) {
	t.Helper()
	policy := proto.Clone(h.machine.State.ClusterPolicy).(*commonpb.ClusterPolicy)
	policy.Revision++
	change(policy)
	result := h.apply(&raftcmdpb.Proposal{Id: 999, Date: &commonpb.Timestamp{Data: h.machine.State.LastAppliedTimestamp + 1}, Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyOrder{Policy: policy}}}}}}})
	require.NoError(t, result.Error)
}

func TestAdmitMetadataPolicyTighteningReplaysRetainedFailure(t *testing.T) {
	t.Parallel()
	h := newMetadataReplayHarness(t)
	request := metadataReplayRequest("failed", strings.Repeat("v", 1024))
	posting := request.GetUnsigned().GetRequests()[0].GetApply().GetAction().GetCreateTransaction().GetPostings()[0]
	posting.Source, posting.Destination = "empty", "alice"
	ctx := attributedTestContext(context.Background())
	_, originalError := h.admission.Admit(ctx, request)
	require.Error(t, originalError)
	var limit *domain.ErrMetadataLimitExceeded
	require.NotErrorAs(t, originalError, &limit)
	_, retainedError := h.admission.Admit(ctx, request)
	require.Error(t, retainedError)
	h.changePolicy(t, func(p *commonpb.ClusterPolicy) { p.MetadataMaxValueBytes = 512 })
	beforeAudit, beforeSequence := h.machine.State.NextAuditSequenceID, h.machine.State.NextSequenceID
	_, replayError := h.admission.Admit(ctx, request)
	require.EqualError(t, replayError, retainedError.Error())
	require.Equal(t, beforeAudit, h.machine.State.NextAuditSequenceID)
	require.Equal(t, beforeSequence, h.machine.State.NextSequenceID)
}

func TestAdmitMetadataPolicyTighteningDoesNotReplayExpiredOutcome(t *testing.T) {
	t.Parallel()
	h := newMetadataReplayHarness(t)
	h.changePolicy(t, func(p *commonpb.ClusterPolicy) { p.IdempotencyTtlMicros = 1 })
	ctx := attributedTestContext(context.Background())
	request := metadataReplayRequest("expired", strings.Repeat("v", 1024))
	_, err := h.admission.Admit(ctx, request)
	require.NoError(t, err)
	h.changePolicy(t, func(p *commonpb.ClusterPolicy) { p.MetadataMaxValueBytes = 512; p.IdempotencyTtlMicros = 0 })
	beforeAudit, beforeSequence := h.machine.State.NextAuditSequenceID, h.machine.State.NextSequenceID
	_, err = h.admission.Admit(ctx, request)
	var limit *domain.ErrMetadataLimitExceeded
	require.ErrorAs(t, err, &limit)
	require.Equal(t, beforeAudit, h.machine.State.NextAuditSequenceID)
	require.Equal(t, beforeSequence, h.machine.State.NextSequenceID)
}

func TestAdmitMetadataReplayProposesOnlyBoundedBarrier(t *testing.T) {
	t.Parallel()
	for _, size := range []int{1024, 16 << 10} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()
			h := newMetadataReplayHarness(t)
			ctx := attributedTestContext(context.Background())
			request := metadataReplayRequest("bounded", strings.Repeat("v", size))
			original, err := h.admission.Admit(ctx, request)
			require.NoError(t, err)
			originalBytes := proto.Size(h.proposals[0])
			h.changePolicy(t, func(p *commonpb.ClusterPolicy) { p.MetadataMaxValueBytes = 512 })
			replay, err := h.admission.Admit(ctx, request)
			require.NoError(t, err)
			require.Equal(t, original.Logs, replay.Logs)
			require.Len(t, h.proposals, 2)
			barrier := h.proposals[1]
			require.Empty(t, barrier.GetOrders())
			require.Nil(t, barrier.GetIdempotency())
			require.Empty(t, barrier.GetExecutionPlan().GetAttributes())
			require.Empty(t, barrier.GetExecutionPlan().GetIdempotencyKeys())
			require.Less(t, proto.Size(barrier), 256)
			t.Logf("original=%d bytes, replay barrier=%d bytes, historical logs=%d", originalBytes, proto.Size(barrier), len(replay.Logs))
		})
	}
}

func TestAdmitMetadataPolicyTighteningDimensions(t *testing.T) {
	t.Parallel()
	for _, dimension := range []string{domain.MetadataLimitDimensionEntries, domain.MetadataLimitDimensionKey, domain.MetadataLimitDimensionEntity, domain.MetadataLimitDimensionCommand} {
		t.Run(dimension, func(t *testing.T) {
			t.Parallel()
			h := newMetadataReplayHarness(t)
			request := metadataReplayRequest("dimensions", "value")
			transaction := request.GetUnsigned().GetRequests()[0].GetApply().GetAction().GetCreateTransaction()
			switch dimension {
			case domain.MetadataLimitDimensionEntries:
				transaction.Metadata["other"] = metadataStringValue("value")
			case domain.MetadataLimitDimensionEntity:
				transaction.Metadata = map[string]*commonpb.MetadataValue{"a": metadataStringValue(strings.Repeat("v", 400)), "b": metadataStringValue(strings.Repeat("v", 400)), "c": metadataStringValue(strings.Repeat("v", 400))}
			case domain.MetadataLimitDimensionCommand:
				transaction.Metadata["key"] = metadataStringValue(strings.Repeat("v", 600))
				transaction.AccountMetadata = map[string]*commonpb.MetadataMap{"alice": {Values: map[string]*commonpb.MetadataValue{"key": metadataStringValue(strings.Repeat("v", 600))}}}
			}
			ctx := attributedTestContext(context.Background())
			original, err := h.admission.Admit(ctx, request)
			require.NoError(t, err)
			h.changePolicy(t, func(p *commonpb.ClusterPolicy) {
				switch dimension {
				case domain.MetadataLimitDimensionEntries:
					p.MetadataMaxEntriesPerEntity = 1
				case domain.MetadataLimitDimensionKey:
					p.MetadataMaxKeyBytes = 2
				case domain.MetadataLimitDimensionEntity:
					p.MetadataMaxEntityBytes = 1024
					p.MetadataMaxValueBytes = 512
				case domain.MetadataLimitDimensionCommand:
					p.MetadataMaxCommandBytes = 1024
					p.MetadataMaxEntityBytes = 1024
					p.MetadataMaxValueBytes = 1024
				}
			})
			beforeAudit, beforeSequence := h.machine.State.NextAuditSequenceID, h.machine.State.NextSequenceID
			replay, err := h.admission.Admit(ctx, request)
			require.NoError(t, err)
			require.True(t, replay.Replayed)
			require.Equal(t, original.Logs, replay.Logs)
			request.GetUnsigned().IdempotencyKey = "new"
			_, err = h.admission.Admit(ctx, request)
			var limit *domain.ErrMetadataLimitExceeded
			require.ErrorAs(t, err, &limit)
			require.Equal(t, dimension, limit.Dimension)
			require.Equal(t, beforeAudit, h.machine.State.NextAuditSequenceID)
			require.Equal(t, beforeSequence, h.machine.State.NextSequenceID)
		})
	}
}

// BenchmarkAdmitMetadataPolicyReplay compares sequential local admission over
// real FSM/Pebble state. The synchronous transport mock excludes network, Raft
// replication, durable WAL latency and concurrent contention.
func BenchmarkAdmitMetadataPolicyReplay(b *testing.B) {
	for _, size := range []int{1024, 16 << 10} {
		for _, tightened := range []bool{false, true} {
			b.Run(fmt.Sprintf("%d/tightened=%t", size, tightened), func(b *testing.B) {
				h := newMetadataReplayHarness(b)
				request := metadataReplayRequest("benchmark", strings.Repeat("v", size))
				ctx := attributedTestContext(context.Background())
				_, err := h.admission.Admit(ctx, request)
				require.NoError(b, err)
				if tightened {
					h.changePolicy(b, func(p *commonpb.ClusterPolicy) { p.MetadataMaxValueBytes = 512 })
				}
				h.afterApply = func(*raftcmdpb.Proposal) { h.proposals = nil }
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					result, err := h.admission.Admit(ctx, request)
					require.NoError(b, err)
					require.True(b, result.Replayed)
				}
			})
		}
	}
}

// BenchmarkAdmitMetadataReplayLatestSignedAuditPayload isolates the latest audit
// header size from the current retry's 1 KiB metadata and historical response.
// Signature payloads are synthetic opaque bytes; this measures lookup cost,
// not signed-batch validity. The local synchronous Raft substitute has the same
// transport, WAL, and concurrency limitations as BenchmarkAdmitMetadataPolicyReplay.
func BenchmarkAdmitMetadataReplayLatestSignedAuditPayload(b *testing.B) {
	for _, size := range []int{64, 4 << 20} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			h := newMetadataReplayHarness(b)
			request := metadataReplayRequest("signed-audit-benchmark", strings.Repeat("v", 1024))
			ctx := attributedTestContext(context.Background())
			_, err := h.admission.Admit(ctx, request)
			require.NoError(b, err)
			h.changePolicy(b, func(p *commonpb.ClusterPolicy) { p.MetadataMaxValueBytes = 512 })
			reader, err := h.admission.store.NewReadHandle()
			require.NoError(b, err)
			latest, err := query.ReadLastAuditEntry(reader)
			require.NoError(b, err)
			require.NotNil(b, latest)
			require.NoError(b, reader.Close())
			latest.Signature = &signaturepb.SignedApplyBatch{Payload: []byte(strings.Repeat("s", size))}
			batch := h.admission.store.OpenWriteSession()
			key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(latest.GetSequence()).Build()
			require.NoError(b, batch.SetProto(key, latest))
			require.NoError(b, batch.Commit())
			h.afterApply = func(*raftcmdpb.Proposal) { h.proposals = nil }
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := h.admission.Admit(ctx, request)
				require.NoError(b, err)
				require.True(b, result.Replayed)
			}
		})
	}
}
