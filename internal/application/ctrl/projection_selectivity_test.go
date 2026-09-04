package ctrl

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func testUint64Bytes(value uint64) []byte {
	ret := make([]byte, 8)
	binary.BigEndian.PutUint64(ret, value)

	return ret
}

func testTxIDFilter(id uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{
		BuiltinUint: &commonpb.BuiltinUintCondition{
			Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID,
			Cond:  &commonpb.UintCondition{Min: &id, Max: &id},
		},
	}}
}

func TestListEntitiesAppliesReverseMainStoreOnlyFilter(t *testing.T) {
	t.Parallel()

	store := newReceiptTestStore(t)
	attrs := attributes.New()
	seedCreatedTransaction(t, store, attrs, "ledger", 1, 1, commonpb.NewTransaction().WithID(1))
	seedCreatedTransaction(t, store, attrs, "ledger", 2, 2, commonpb.NewTransaction().WithID(2))

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	rs, err := readstore.New(t.TempDir(), logging.NopZap(), readstore.DefaultConfig())
	require.NoError(t, err)
	defer func() { _ = rs.Close() }()

	result, err := listEntities(context.Background(), rs, entityListParams[uint64]{
		target:       commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		ledgerName:   "ledger",
		pageSize:     10,
		filter:       testTxIDFilter(2),
		reverse:      true,
		pebbleReader: handle,
		releaseHold:  func() {},
		afterToBytes: testUint64Bytes,
	})
	require.NoError(t, err)
	require.Equal(t, [][]byte{testUint64Bytes(2)}, result.entityIDs,
		"the reverse fast path must compile its main-store-only filter")

	_, err = listEntities(context.Background(), rs, entityListParams[uint64]{
		target:       commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		ledgerName:   "ledger",
		pageSize:     10,
		filter:       &commonpb.QueryFilter{},
		reverse:      true,
		pebbleReader: handle,
		releaseHold:  func() {},
		afterToBytes: testUint64Bytes,
	})
	require.Error(t, err, "the reverse fast path must not silently accept a malformed filter")
}

func TestAggregateVolumesMainStoreOnlyFilterDoesNotWaitForReadProjection(t *testing.T) {
	t.Parallel()

	store := newReceiptTestStore(t)
	attrs := attributes.New()
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, "ledger", &commonpb.LedgerInfo{Name: "ledger"}))
	_, err := attrs.Volume.Set(batch, domain.NewVolumeKey("ledger", "alice", "USD", "").Bytes(), &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(10),
	})
	require.NoError(t, err)
	require.NoError(t, state.SetAppliedIndex(batch, 10))
	require.NoError(t, batch.Commit())

	rs, err := readstore.New(t.TempDir(), logging.NopZap(), readstore.DefaultConfig())
	require.NoError(t, err)
	defer func() { _ = rs.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	meter := noop.NewMeterProvider().Meter("test")
	ctrl := NewDefaultController(nil, store, logging.NopZap(), attrs, rs, nil, nil, meter)
	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
		Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: "alice"},
	}}}

	result, err := ctrl.AggregateVolumes(ctx, "ledger", filter, query.AggregateOptions{})
	require.NoError(t, err,
		"a main-store-only aggregation must remain independent of a lagging projection")
	require.Len(t, result.GetVolumes(), 1)
	require.Equal(t, uint64(10), result.GetVolumes()[0].GetInput().GetV0())
}
