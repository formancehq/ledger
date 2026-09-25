package mirror

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/application/accountlifecycle"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestReleaseMirrorLifecycleWaitsForFSMTerminalResultAfterCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	serializer := accountlifecycle.NewSerializer()
	account := domain.AccountKey{LedgerName: "mirror", Account: "hold:1"}
	release, err := serializer.Acquire(ctx, map[domain.AccountKey]struct{}{account: {}}, false)
	require.NoError(t, err)
	future := futures.New[state.ApplyResult]()
	releaseMirrorLifecycleWhenTerminal(future, release)
	cancel()

	_, acquired := serializer.TryAcquire(account)
	require.False(t, acquired, "replacement lifecycle operation acquired the stripe before FSM resolution")

	future.Resolve(state.ApplyResult{}, nil)
	require.Eventually(t, func() bool {
		release, acquired := serializer.TryAcquire(account)
		if acquired {
			release()
		}

		return acquired
	}, time.Second, time.Millisecond, "replacement lifecycle operation remained blocked after FSM resolution")
}

func TestExpandAccountLifecycleCoverageIncludesPersistedMirrorVolumes(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	attrs := attributes.New()
	ledgerName := "mirror"
	account := domain.AccountKey{LedgerName: ledgerName, Account: "hold:1"}
	volume := domain.VolumeKey{AccountKey: account, Asset: "USD"}
	metadata := domain.MetadataKey{AccountKey: account, Key: "note"}

	batch := store.OpenWriteSession()
	_, err := attrs.Ledger.Set(batch, domain.LedgerKey{Name: ledgerName}.Bytes(), &commonpb.LedgerInfo{
		Name: ledgerName,
		AccountTypes: map[string]*commonpb.AccountType{
			"hold": {Name: "hold", Pattern: "hold:{id}", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL},
		},
	})
	require.NoError(t, err)
	_, err = attrs.Volume.Set(batch, volume.Bytes(), &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(10), Output: commonpb.NewUint256FromUint64(0),
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	worker := newWorkerForTest(t, ledgerName, nil, store, builder)
	aggregate := plan.NewCoverage()
	orderCoverage := plan.NewCoverage()
	orderCoverage.Add(dal.SubAttrMetadata, metadata.Bytes())
	aggregate.Merge(orderCoverage)

	require.NoError(t, worker.expandAccountLifecycleCoverage(aggregate, []*plan.Coverage{orderCoverage}))
	require.True(t, orderCoverage.Has(dal.SubAttrVolume, volume.Bytes()))
	require.True(t, aggregate.Has(dal.SubAttrVolume, volume.Bytes()))
}
