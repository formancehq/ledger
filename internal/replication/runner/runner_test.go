package runner

import (
	"context"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/pagination"
	"testing"
	"time"

	ingester "github.com/formancehq/ledger/internal/replication"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func startRunner(t *testing.T, ctx context.Context, storageDriver StorageDriver, systemStore SystemStore, connectorFactory drivers.Factory) *Runner {
	t.Helper()

	runner := NewRunner(
		storageDriver,
		systemStore,
		connectorFactory,
		logging.Testing(),
	)
	go func() {
		require.NoError(t, runner.Run(ctx))
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		require.NoError(t, runner.Stop(ctx))
	})
	<-runner.Ready()

	return runner
}

func TestRunner(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	storageDriver := NewMockStorageDriver(ctrl)
	systemStore := NewMockSystemStore(ctrl)
	logFetcher := NewMockLogFetcher(ctrl)
	connectorFactory := drivers.NewMockFactory(ctrl)
	connector := drivers.NewMockDriver(ctrl)

	pipelineConfiguration := ledger.NewPipelineConfiguration("module1", "connector")
	pipeline := ledger.NewPipeline(pipelineConfiguration, ledger.NewInitState())

	connectorFactory.EXPECT().
		Create(gomock.Any(), pipelineConfiguration.ConnectorID).
		Return(connector, nil, nil)
	connector.EXPECT().Start(gomock.Any()).Return(nil)

	log := ledger.NewLog(ledger.CreatedTransaction{
		Transaction: ledger.NewTransaction(),
	})
	deliver := make(chan struct{})
	delivered := make(chan struct{})

	logFetcher.EXPECT().
		ListLogs(gomock.Any(), pagination.ColumnPaginatedQuery[any]{
			PageSize: 100,
			Column:   "id",
			Options: pagination.ResourceQuery[any]{
				Builder: query.Gte("id", 0),
			},
			Order: pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
		}).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, paginatedQuery pagination.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
			select {
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

	storageDriver.EXPECT().
		OpenLedger(gomock.Any(), pipelineConfiguration.Ledger).
		Return(logFetcher, &ledger.Ledger{}, nil)

	systemStore.EXPECT().
		StorePipelineState(gomock.Any(), pipeline.ID, ledger.NewInitState()).
		Return(nil)

	systemStore.EXPECT().
		StorePipelineState(gomock.Any(), pipeline.ID, ledger.NewReadyState()).
		Return(nil)

	runner := startRunner(t, ctx, storageDriver, systemStore, connectorFactory)
	_, err := runner.StartPipeline(ctx, pipeline)
	require.NoError(t, err)

	connector.EXPECT().
		Accept(gomock.Any(), ingester.NewLogWithLedger(pipelineConfiguration.Ledger, log)).
		Return([]error{nil}, nil)

	require.Eventually(t, func() bool {
		return runner.GetConnector("connector") != nil
	}, time.Second, 10*time.Millisecond)

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
