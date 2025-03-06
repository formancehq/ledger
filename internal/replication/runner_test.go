package runner

import (
	"context"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func startRunner(t *testing.T, ctx context.Context, storageDriver Storage, connectorFactory drivers.Factory) *Runner {
	t.Helper()

	runner := NewRunner(
		storageDriver,
		connectorFactory,
		logging.Testing(),
	)
	go runner.Run(ctx)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		require.NoError(t, runner.Stop(ctx))
	})

	return runner
}

func TestRunner(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	storage := NewMockStorage(ctrl)
	logFetcher := NewMockLogFetcher(ctrl)
	connectorFactory := drivers.NewMockFactory(ctrl)
	connector := drivers.NewMockDriver(ctrl)

	pipelineConfiguration := ledger.NewPipelineConfiguration("module1", "connector")
	pipeline := ledger.NewPipeline(pipelineConfiguration)

	connectorFactory.EXPECT().
		Create(gomock.Any(), pipelineConfiguration.ConnectorID).
		Return(connector, nil, nil)
	connector.EXPECT().Start(gomock.Any()).Return(nil)

	log := ledger.NewLog(ledger.CreatedTransaction{
		Transaction: ledger.NewTransaction(),
	})
	log.ID = pointer.For(uint64(1))
	deliver := make(chan struct{})
	delivered := make(chan struct{})

	logFetcher.EXPECT().
		ListLogs(gomock.Any(), common.InitialPaginatedQuery[any]{
			PageSize: 100,
			Column:   "id",
			Options: common.ResourceQuery[any]{
				Builder: query.Gt("id", -1),
			},
			Order: pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
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

	storage.EXPECT().
		ListEnabledPipelines(gomock.Any()).
		AnyTimes().
		Return([]ledger.Pipeline{pipeline}, nil)

	storage.EXPECT().
		GetPipeline(gomock.Any(), pipeline.ID).
		Return(&pipeline, nil)

	storage.EXPECT().
		OpenLedger(gomock.Any(), pipelineConfiguration.Ledger).
		Return(logFetcher, &ledger.Ledger{}, nil)

	storage.EXPECT().
		StorePipelineState(gomock.Any(), pipeline.ID, 1).
		Return(nil)

	runner := startRunner(t, ctx, storage, connectorFactory)
	_, err := runner.StartPipeline(ctx, pipeline.ID)
	require.NoError(t, err)

	connector.EXPECT().
		Accept(gomock.Any(), drivers.NewLogWithLedger(pipelineConfiguration.Ledger, log)).
		Return([]error{nil}, nil)

	require.Eventually(t, func() bool {
		return runner.GetConnector("connector") != nil
	}, 5*time.Second, 10*time.Millisecond)

	select {
	case <-runner.GetConnector("connector").Ready():
	case <-time.After(time.Second):
		require.Fail(t, "connector should be ready")
	}

	close(deliver)

	require.Eventually(t, ctrl.Satisfied, 2*time.Second, 10*time.Millisecond)

	// notes(gfyrag): add this expectation AFTER the previous Eventually.
	// If configured before the Eventually, it will never finish as the stop call is made in a t.Cleanup defined earlier
	connector.EXPECT().Stop(gomock.Any()).Return(nil)
}
