package plan_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type stalePreloadNotifier struct{}

func (stalePreloadNotifier) NotifyLogsCommitted(uint64) {}
func (stalePreloadNotifier) NotifyConfigChanged()       {}

// TestDelayedCommitReferenceSourceAndApply runs the production planner and FSM
// preload against real Pebble. Admission sees old durable reference data while
// a newer entry is published in cache; one rotation then discards the captured
// generation before the plan applies. A later local write must win over that
// captured seed, and both changed cache rows must agree with memory.
func TestDelayedCommitReferenceSourceAndApply(t *testing.T) {
	t.Parallel()
	logger := logging.Testing()
	meterProvider := noop.NewMeterProvider()
	meter := meterProvider.Meter("test")
	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	attrs := attributes.New()
	c, err := cache.New(10, meter)
	require.NoError(t, err)
	registry := state.NewStateRegistry(c, attrs)
	snapshotter := state.NewCacheSnapshotter(logger, registry, nil)
	machine, err := state.NewMachine(
		logger, registry, snapshotter, store, dal.NewSentinelFactory(store, false),
		meterProvider, keystore.NewKeyStore(), state.NewSharedState(),
		stalePreloadNotifier{}, nil, "test-cluster", 0,
		func(*raftpb.Entry, *dal.WriteSession) error { return nil },
	)
	require.NoError(t, err)

	key := domain.TransactionReferenceKey{LedgerName: "refrace", Reference: "fill"}
	id, tag := attributes.MakeKey(key.Bytes())
	stale := &commonpb.TransactionReferenceValue{TransactionId: 1}
	batch := store.OpenWriteSession()
	_, err = attrs.References.Set(batch, key.Bytes(), stale)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// The first prepared write is visible in old Gen1 but not in Pebble.
	c.References.Gen1().Put(id, attributes.Entry[*commonpb.TransactionReferenceValue]{
		Tag: tag, Data: &commonpb.TransactionReferenceValue{TransactionId: 2},
	})
	needs := plan.NewCoverage()
	needs.Add(dal.SubAttrReference, key.Bytes())
	builder := plan.NewBuilder(node.NewIndexTracker(11), c, attrs, store, nil, logger, 0)
	built, err := builder.Build(needs, []plan.WriteOperation{{Coverage: needs}})
	require.NoError(t, err)
	defer built.ReleaseLoaders()
	require.Len(t, built.ExecutionPlan.GetAttributes(), 1)
	var captured commonpb.TransactionReferenceValue
	require.NoError(t, captured.UnmarshalVT(built.ExecutionPlan.GetAttributes()[0].GetValue().GetRawValue()))
	require.Equal(t, uint64(2), captured.GetTransactionId())

	// A second prepared write lands in Gen0 after source capture. Its cache
	// row is staged but not committed while the cache rotates.
	newer := &commonpb.TransactionReferenceValue{TransactionId: 3}
	c.References.Gen0().Put(id, attributes.Entry[*commonpb.TransactionReferenceValue]{Tag: tag, Data: newer})
	raw, err := newer.MarshalVT()
	require.NoError(t, err)
	lean := make([]byte, 9+len(raw))
	binary.LittleEndian.PutUint64(lean[:8], tag)
	copy(lean[9:], raw)
	cacheKey := append([]byte{dal.ZoneCache, 0, dal.SubAttrReference}, id[:]...)
	earlier := store.OpenWriteSession()
	require.NoError(t, earlier.SetBytes(cacheKey, lean))
	_, err = attrs.References.Set(earlier, key.Bytes(), newer)
	require.NoError(t, err)
	rotated, _ := c.CheckRotationNeeded(11)
	require.True(t, rotated)

	apply := store.OpenWriteSession()
	require.NoError(t, machine.Preload(built.ExecutionPlan, apply, 1))
	require.NoError(t, earlier.Commit())
	require.NoError(t, apply.Commit())
	got, ok := c.References.Gen0().Get(id)
	require.True(t, ok)
	require.Equal(t, uint64(3), got.Data.GetTransactionId())
	latestKey := append([]byte{dal.ZoneCache, 1, dal.SubAttrReference}, id[:]...)
	persisted, closer, err := store.Get(latestKey)
	require.NoError(t, err)
	defer func() { require.NoError(t, closer.Close()) }()
	var durable commonpb.TransactionReferenceValue
	require.NoError(t, durable.UnmarshalVT(persisted[9:]))
	require.Equal(t, uint64(3), durable.GetTransactionId())

	// Immediate-commit control: a new admission sees the committed Gen0
	// value and emits only coverage, so there is no stale seed to apply.
	committed, err := attrs.References.Get(store, key.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(3), committed.GetTransactionId())
	control := plan.NewBuilder(node.NewIndexTracker(12), c, attrs, store, nil, logger, 0)
	controlBuild, err := control.Build(needs, []plan.WriteOperation{{Coverage: needs}})
	require.NoError(t, err)
	defer controlBuild.ReleaseLoaders()
	require.Len(t, controlBuild.ExecutionPlan.GetAttributes(), 1)
	require.Nil(t, controlBuild.ExecutionPlan.GetAttributes()[0].GetValue())
}

// TestDelayedCommitReferenceSeedRestores verifies the other side of precedence:
// without a newer Gen0 write, the captured Gen1 value must survive rotation,
// proposal apply, and a cache rebuild from Pebble. The primary-store update is
// deliberately held in a prepared batch until after admission has built its
// plan, reproducing the stale durable snapshot seen by the leader.
func TestDelayedCommitReferenceSeedRestores(t *testing.T) {
	t.Parallel()
	logger := logging.Testing()
	meterProvider := noop.NewMeterProvider()
	meter := meterProvider.Meter("test")
	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	attrs := attributes.New()
	c, err := cache.New(10, meter)
	require.NoError(t, err)
	registry := state.NewStateRegistry(c, attrs)
	snapshotter := state.NewCacheSnapshotter(logger, registry, nil)
	machine, err := state.NewMachine(
		logger, registry, snapshotter, store, dal.NewSentinelFactory(store, false),
		meterProvider, keystore.NewKeyStore(), state.NewSharedState(),
		stalePreloadNotifier{}, nil, "test-cluster", 0,
		func(*raftpb.Entry, *dal.WriteSession) error { return nil },
	)
	require.NoError(t, err)

	key := domain.TransactionReferenceKey{LedgerName: "refrace", Reference: "restored"}
	id, tag := attributes.MakeKey(key.Bytes())
	initial := store.OpenWriteSession()
	_, err = attrs.References.Set(initial, key.Bytes(), &commonpb.TransactionReferenceValue{TransactionId: 1})
	require.NoError(t, err)
	require.NoError(t, initial.Commit())

	newer := &commonpb.TransactionReferenceValue{TransactionId: 2}
	pending := store.OpenWriteSession()
	defer func() { require.NoError(t, pending.Cancel()) }()
	_, err = attrs.References.Set(pending, key.Bytes(), newer)
	require.NoError(t, err)
	c.References.Gen1().Put(id, attributes.Entry[*commonpb.TransactionReferenceValue]{Tag: tag, Data: newer})
	needs := plan.NewCoverage()
	needs.Add(dal.SubAttrReference, key.Bytes())
	builder := plan.NewBuilder(node.NewIndexTracker(11), c, attrs, store, nil, logger, 0)
	built, err := builder.Build(needs, []plan.WriteOperation{{Coverage: needs}})
	require.NoError(t, err)
	defer built.ReleaseLoaders()
	require.Len(t, built.ExecutionPlan.GetAttributes(), 1)
	var captured commonpb.TransactionReferenceValue
	require.NoError(t, captured.UnmarshalVT(built.ExecutionPlan.GetAttributes()[0].GetValue().GetRawValue()))
	require.Equal(t, uint64(2), captured.GetTransactionId(), "admission must use the pending cache state")

	rotated, _ := c.CheckRotationNeeded(11)
	require.True(t, rotated)
	apply := store.OpenWriteSession()
	require.NoError(t, apply.SetProto(
		[]byte{dal.ZoneCache, dal.SubCacheMeta},
		&raftcmdpb.CacheSnapshotMeta{CurrentGeneration: c.CurrentGeneration()},
	))
	require.NoError(t, machine.Preload(built.ExecutionPlan, apply, 1))
	require.NoError(t, pending.Commit())
	require.NoError(t, apply.Commit())
	committed, err := attrs.References.Get(store, key.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(2), committed.GetTransactionId())

	for _, generation := range []int{0, 1} {
		entry, ok := c.References.Gen0().Get(id)
		if generation == 1 {
			entry, ok = c.References.Gen1().Get(id)
		}
		require.True(t, ok)
		require.Equal(t, uint64(2), entry.Data.GetTransactionId())
	}
	require.NoError(t, snapshotter.RestoreFromStore(store))
	require.Equal(t, uint64(1), c.CurrentGeneration())
	for _, generation := range []int{0, 1} {
		entry, ok := c.References.Gen0().Get(id)
		if generation == 1 {
			entry, ok = c.References.Gen1().Get(id)
		}
		require.True(t, ok, "seed must be durable in both cache generations")
		require.False(t, entry.Deleted)
		require.Equal(t, tag, entry.Tag)
		require.Equal(t, uint64(2), entry.Data.GetTransactionId())
	}
}
