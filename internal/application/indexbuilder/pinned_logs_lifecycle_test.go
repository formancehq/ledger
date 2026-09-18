package indexbuilder

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// Observe the projection progress wait, after ListLogs has opened its main
// snapshot and checked ledger existence. This follows checkpointWaitObservedContext
// in the gRPC checkpoint tests and needs no production synchronization hook.
type pinnedLogsWaitContext struct {
	context.Context

	once    sync.Once
	waiting chan struct{}
}

func (c *pinnedLogsWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })

	return c.Context.Done()
}

func TestListLogsRejectsLedgerDeletedDuringAlignment(t *testing.T) {
	t.Parallel()
	b := newTestBuilderWithStore(t)
	b.batchSize = DefaultBatchSize
	b.notifications = signal.NewNotifications()
	const ledger = "pinned-logs"
	logs := []*commonpb.Log{{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger},
		}},
	}}
	for id := uint64(1); id <= 3; id++ {
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
	require.NoError(t, state.SetAppliedIndex(batch, 4))
	require.NoError(t, batch.Commit())
	foldCursor, err := b.processLogs(t.Context(), 0, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(4), foldCursor)
	c := ctrl.NewDefaultController(nil, b.pebbleStore, b.logger, b.attrs, b.readStore, nil, noop.NewMeterProvider().Meter("pinned-logs"))
	baseline, err := c.ListLogs(query.WithReadBarrierHorizon(t.Context(), 4), ledger, 0, 3, nil)
	require.NoError(t, err)
	baselineLogs, err := cursor.Collect(baseline)
	require.NoError(t, err)
	require.Len(t, baselineLogs, 3, "prove the real index fold has populated all three expected logs")

	// A fully acquired cursor must keep its pre-deletion main snapshot alive
	// even after the projection is wiped by the later deletion.
	pinnedCursor, err := c.ListLogs(query.WithReadBarrierHorizon(t.Context(), 4), ledger, 0, 3, nil)
	require.NoError(t, err)
	defer func() {
		if pinnedCursor != nil {
			_ = pinnedCursor.Close()
		}
	}()

	// A committed Raft entry without a native log is a valid production state.
	// It makes alignment wait, providing a deterministic pause after opening
	// the pre-deletion main snapshot without modifying production code.
	batch = b.pebbleStore.OpenWriteSession()
	require.NoError(t, state.SetAppliedIndex(batch, 5))
	require.NoError(t, batch.Commit())
	deadlineCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	observed := &pinnedLogsWaitContext{Context: query.WithReadBarrierHorizon(deadlineCtx, 5), waiting: make(chan struct{})}
	type queryResult struct {
		logs []*commonpb.Log
		err  error
	}
	result := make(chan queryResult, 1)
	finished := make(chan struct{})
	defer func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Error("ListLogs worker did not stop before store cleanup")
		}
	}()
	go func() {
		defer close(finished)
		cur, queryErr := c.ListLogs(observed, ledger, 0, 3, nil)
		if queryErr != nil {
			result <- queryResult{err: queryErr}

			return
		}
		values, queryErr := cursor.Collect(cur)
		result <- queryResult{logs: values, err: queryErr}
	}()
	select {
	case <-observed.waiting:
	case early := <-result:
		t.Fatalf("query completed before the alignment wait: %+v", early)
	case <-deadlineCtx.Done():
		t.Fatal("query never reached the alignment wait")
	}

	// Commit the real deletion representation, then run the actual indexbuilder
	// deletion branch. The query's main snapshot must still see the old ledger.
	batch = b.pebbleStore.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{Name: ledger, DeletedAt: &commonpb.Timestamp{Data: 6}}))
	require.NoError(t, state.DeleteLedgerData(batch, ledger))
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{{
		Sequence: 5,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{
			DeleteLedger: &commonpb.DeletedLedgerLog{Name: ledger},
		}},
	}}))
	require.NoError(t, state.SetAppliedIndex(batch, 6))
	require.NoError(t, batch.Commit())
	foldCursor, err = b.processLogs(t.Context(), foldCursor, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(5), foldCursor)
	b.readStore.NotifyProgress()

	var got queryResult
	select {
	case got = <-result:
	case <-deadlineCtx.Done():
		t.Fatal("query did not finish after the deletion fold published progress")
	}
	pinnedLogs, err := cursor.Collect(pinnedCursor)
	pinnedCursor = nil
	require.NoError(t, err)
	pinnedIDs := make([]uint64, 0, len(pinnedLogs))
	for _, log := range pinnedLogs {
		pinnedIDs = append(pinnedIDs, log.GetPayload().GetApply().GetLog().GetId())
	}
	require.Equal(t, []uint64{1, 2, 3}, pinnedIDs, "an already acquired cursor must retain its pinned logs")
	freshCursor, freshErr := c.ListLogs(query.WithReadBarrierHorizon(t.Context(), 6), ledger, 0, 3, nil)
	require.Nil(t, freshCursor)
	var freshNotFound *commonpb.NotFoundError
	require.ErrorAs(t, freshErr, &freshNotFound, "fresh reads reject before alignment")

	// The current ledger is absent while the fully acquired cursor above still
	// served its pinned logs. Only the read straddling alignment must reject.
	live, err := b.pebbleStore.NewReadHandle()
	require.NoError(t, err)
	_, liveErr := query.GetLedgerByName(t.Context(), live, ledger)
	require.NoError(t, live.Close())
	require.ErrorIs(t, liveErr, domain.ErrNotFound)
	var rejected *domain.ErrLedgerNotFound
	require.ErrorAs(t, got.err, &rejected,
		"post-alignment deletion must reject explicitly, never return a successful empty page")
	require.Equal(t, ledger, rejected.Name)
	require.Nil(t, got.logs)
}
