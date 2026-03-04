package replication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/pointer"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/storage/common"
)

type testProgressTracker struct {
	ledgerName string
	lastLogID  *uint64
	ch         chan uint64
}

func (t *testProgressTracker) LedgerName() string {
	return t.ledgerName
}

func (t *testProgressTracker) LastLogID() *uint64 {
	return t.lastLogID
}

func (t *testProgressTracker) UpdateLastLogID(_ context.Context, id uint64) error {
	t.lastLogID = &id
	t.ch <- id
	return nil
}

func runPipeline(t *testing.T, ctx context.Context, tracker *testProgressTracker, store LogFetcher, driver drivers.Driver) *PipelineHandler {
	t.Helper()

	handler := NewPipelineHandler(
		tracker,
		store,
		driver,
		logging.Testing(),
	)

	go handler.Run(ctx)
	t.Cleanup(func() {
		require.NoError(t, handler.Shutdown(ctx))
	})

	return handler
}

func TestPipeline(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher := NewMockLogFetcher(ctrl)
	driver := drivers.NewMockDriver(ctrl)
	log := ledger.NewLog(
		ledger.CreatedTransaction{
			Transaction: ledger.NewTransaction(),
		},
	)
	log.ID = pointer.For(uint64(1))

	deliver := make(chan struct{})
	delivered := make(chan struct{})

	logFetcher.EXPECT().
		ListLogs(gomock.Any(), common.InitialPaginatedQuery[any]{
			PageSize: 100,
			Column:   "id",
			Options:  common.ResourceQuery[any]{},
			Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
		}).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, paginatedQuery common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-deliver:
				select {
				case <-delivered:
				default:
					close(delivered)
					return &bunpaginate.Cursor[ledger.Log]{
						Data: []ledger.Log{log},
					}, nil
				}
			}
			return &bunpaginate.Cursor[ledger.Log]{}, nil
		})

	driver.EXPECT().
		Accept(gomock.Any(), drivers.NewLogWithLedger("testing", log)).
		Return([]error{nil}, nil)

	tracker := &testProgressTracker{
		ledgerName: "testing",
		ch:         make(chan uint64, 1),
	}

	runPipeline(t, ctx, tracker, logFetcher, driver)

	close(deliver)

	ShouldReceive(t, uint64(1), tracker.ch)

	require.Eventually(t, ctrl.Satisfied, time.Second, 10*time.Millisecond)
}
