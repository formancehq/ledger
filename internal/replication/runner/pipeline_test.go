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

	"github.com/formancehq/ledger/internal/replication"

	"github.com/formancehq/ledger/internal/replication/drivers"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func runPipeline(t *testing.T, ctx context.Context, pipeline ledger.Pipeline, store LogFetcher, connector drivers.Driver) (*PipelineHandler, <-chan ledger.PipelineState) {
	t.Helper()

	handler := NewPipelineHandler(
		pipeline,
		store,
		connector,
		logging.Testing(),
		WithStateRetryInterval(50*time.Millisecond),
	)
	stateListener, cancelStateListener := handler.GetActiveState().Listen()
	t.Cleanup(cancelStateListener)

	go handler.Run(ctx)
	t.Cleanup(func() {
		require.NoError(t, handler.Shutdown(ctx))
	})

	return handler, stateListener
}

func TestPipelineReady(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher := NewMockLogFetcher(ctrl)
	connector := drivers.NewMockDriver(ctrl)
	log := ledger.NewLog(
		ledger.CreatedTransaction{
			Transaction: ledger.NewTransaction(),
		},
	)

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

	connector.EXPECT().
		Accept(gomock.Any(), replication.NewLogWithLedger("testing", log)).
		Return([]error{nil}, nil)

	pipelineConfiguration := ledger.NewPipelineConfiguration("testing", "testing")
	pipeline := ledger.NewPipeline(pipelineConfiguration, ledger.NewReadyState())

	_, stateListener := runPipeline(t, ctx, pipeline, logFetcher, connector)

	ShouldReceive(t, ledger.NewReadyState(), stateListener)

	close(deliver)

	require.Eventually(t, ctrl.Satisfied, time.Second, 10*time.Millisecond)
}

func TestPipelinePause(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	logFetcher := NewMockLogFetcher(ctrl)
	connector := drivers.NewMockDriver(ctrl)

	pipelineConfiguration := ledger.NewPipelineConfiguration("testing", "testing")
	state := ledger.NewPauseState(ledger.NewReadyState())
	pipeline := ledger.NewPipeline(pipelineConfiguration, state)

	_, stateListener := runPipeline(t, ctx, pipeline, logFetcher, connector)

	ShouldReceive(t, state, stateListener)
}
