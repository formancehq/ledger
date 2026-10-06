package ctrl

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// A filter at the maximum accepted depth must stay pageable: the resume
// position is not part of the client's filter tree and must not consume its
// depth budget.
func TestListLogs_CursorDoesNotConsumeFilterDepth(t *testing.T) {
	t.Parallel()

	const (
		ledger  = "depth"
		logsLen = uint64(3)
	)

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")
	store := newCtrlTestStore(t)
	attrs := attributes.New()

	logs := make([]*commonpb.Log, 0, logsLen)
	for seq := uint64(1); seq <= logsLen; seq++ {
		logs = append(logs, &commonpb.Log{
			Sequence: seq,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
				LedgerName: ledger,
				Log:        &commonpb.LedgerLog{Id: seq},
			}}},
		})
	}

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{Name: ledger}))
	require.NoError(t, state.AppendLogs(batch, logs))
	require.NoError(t, state.SetAppliedIndex(batch, logsLen))
	require.NoError(t, batch.Commit())

	rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	indexBatch := rs.NewBatch()
	wb := readstore.NewWriteBatch()
	wb.Init(indexBatch)
	kb := dal.NewKeyBuilder()
	for seq := uint64(1); seq <= logsLen; seq++ {
		require.NoError(t, wb.WriteLedgerLogIndex(kb, ledger, seq, seq))
	}
	require.NoError(t, rs.WriteProgress(indexBatch, logsLen))
	require.NoError(t, rs.WriteRaftProgress(indexBatch, logsLen))
	require.NoError(t, indexBatch.Commit())
	rs.NotifyProgress()

	// MaxFilterDepth-1 single-element $and wrappers around a leaf is the
	// deepest tree validation accepts.
	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogId{
		LogId: &commonpb.LogIdCondition{Cond: &commonpb.UintCondition{Min: new(uint64)}},
	}}
	for range domain.MaxFilterDepth - 1 {
		filter = &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{
			And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{filter}},
		}}
	}
	require.Nil(t, domain.ValidateFilterForTarget(filter, commonpb.QueryTarget_QUERY_TARGET_LOGS))

	ctrl := NewDefaultController(nil, store, logger, attrs, rs, nil, meter)

	listIDs := func(afterSequence uint64) []uint64 {
		t.Helper()

		c, err := ctrl.ListLogs(t.Context(), ledger, afterSequence, 2, filter)
		require.NoError(t, err)

		got, err := cursor.Collect(c)
		require.NoError(t, err)

		ids := make([]uint64, 0, len(got))
		for _, l := range got {
			ids = append(ids, l.GetPayload().GetApply().GetLog().GetId())
		}

		return ids
	}

	require.Equal(t, []uint64{1, 2}, listIDs(0))
	require.Equal(t, []uint64{3}, listIDs(2))
}
