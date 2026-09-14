package ctrl

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestListLogsRejectsProjectionThatDeletedPinnedLedger(t *testing.T) {
	t.Parallel()

	const (
		ledger   = "deleted-between-snapshots"
		ledgerID = uint32(7)
		mainSeq  = uint64(2)
	)

	logger := logging.FromContext(logging.TestingContext())
	store := newCtrlTestStore(t)
	attrs := attributes.New()

	mainBatch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(mainBatch, ledger, &commonpb.LedgerInfo{Name: ledger, Id: ledgerID}))
	require.NoError(t, state.AppendLogs(mainBatch, []*commonpb.Log{
		{Sequence: 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger, Id: ledgerID}}}},
		{Sequence: mainSeq, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{LedgerName: ledger, Log: &commonpb.LedgerLog{Id: 1}}}}},
	}))
	require.NoError(t, state.SetAppliedIndex(mainBatch, mainSeq))
	require.NoError(t, mainBatch.Commit())

	rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	for _, lifecycle := range []readstore.LedgerLifecycle{
		{Active: false},
		{ID: ledgerID + 1, Active: true},
	} {
		name := "deleted"
		if lifecycle.Active {
			name = "same-name recreation"
		}
		t.Run(name, func(t *testing.T) {
			projectionBatch := rs.NewBatch()
			wb := readstore.NewWriteBatch()
			wb.Init(projectionBatch)
			require.NoError(t, wb.WriteLedgerLogIndex(dal.NewKeyBuilder(), ledger, 1, mainSeq))
			require.NoError(t, readstore.DeleteLedgerIndexes(projectionBatch, ledger))
			require.NoError(t, wb.WriteLedgerLifecycle(dal.NewKeyBuilder(), ledger, lifecycle.ID, lifecycle.Active))
			require.NoError(t, rs.WriteProgress(projectionBatch, mainSeq+1))
			require.NoError(t, rs.WriteRaftProgress(projectionBatch, mainSeq+1))
			require.NoError(t, projectionBatch.Commit())
			rs.NotifyProgress()

			ctrl := NewDefaultController(nil, store, logger, attrs, rs, nil, noop.NewMeterProvider().Meter("test"))
			c, err := ctrl.ListLogs(t.Context(), ledger, 0, 100, nil)
			if c != nil {
				require.NoError(t, c.Close())
			}
			var notFound *domain.ErrLedgerNotFound
			require.True(t, errors.As(err, &notFound), "got %v", err)
		})
	}
}
