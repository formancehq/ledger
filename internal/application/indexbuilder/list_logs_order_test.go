package indexbuilder

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// TestListLogsPagesInBothOrders folds four ledger logs through the real index
// builder and pages them through the controller in each order: the cursor is
// exclusive in both, and reverse walks the ledger-local ids downward.
func TestListLogsPagesInBothOrders(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.batchSize = DefaultBatchSize
	b.notifications = signal.NewNotifications()

	const ledger = "ordered-logs"

	logs := []*commonpb.Log{{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger},
		}},
	}}
	for id := uint64(1); id <= 4; id++ {
		logs = append(logs, &commonpb.Log{
			Sequence: id + 1,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Id:   id,
						Date: &commonpb.Timestamp{Data: id},
						Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_AddedAccountType{
							AddedAccountType: &commonpb.AddedAccountTypeLog{},
						}},
					},
				},
			}},
		})
	}

	batch := b.pebbleStore.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{Name: ledger}))
	require.NoError(t, state.AppendLogs(batch, logs))
	require.NoError(t, state.SetAppliedIndex(batch, 5))
	require.NoError(t, batch.Commit())

	_, err := b.processLogs(t.Context(), 0, time.Time{})
	require.NoError(t, err)

	c := ctrl.NewDefaultController(nil, b.pebbleStore, b.logger, b.attrs, b.readStore, nil, noop.NewMeterProvider().Meter("ordered-logs"))

	ids := func(after uint64, pageSize uint32, reverse bool) []uint64 {
		t.Helper()

		cur, err := c.ListLogs(query.WithReadBarrierHorizon(t.Context(), 5), ledger, after, pageSize, nil, reverse)
		require.NoError(t, err)

		page, err := cursor.Collect(cur)
		require.NoError(t, err)

		out := make([]uint64, 0, len(page))
		for _, l := range page {
			out = append(out, l.GetPayload().GetApply().GetLog().GetId())
		}

		return out
	}

	require.Equal(t, []uint64{1, 2, 3}, ids(0, 3, false))
	require.Equal(t, []uint64{3, 4}, ids(2, 3, false))
	require.Equal(t, []uint64{4, 3, 2}, ids(0, 3, true))
	require.Equal(t, []uint64{2, 1}, ids(3, 3, true))
	require.Empty(t, ids(1, 3, true))
}
