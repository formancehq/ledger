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

	"github.com/formancehq/ledger/internal/replication/drivers"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func runPipeline(t *testing.T, ctx context.Context, pipeline ledger.Pipeline, store LogFetcher, connector drivers.Driver) (*PipelineHandler, <-chan int) {
	t.Helper()

	handler := NewPipelineHandler(
		pipeline,
		store,
		connector,
		logging.Testing(),
	)

	lastLogIDChannel := make(chan int)

	go handler.Run(ctx, lastLogIDChannel)
	t.Cleanup(func() {
		require.NoError(t, handler.Shutdown(ctx))
	})

	return handler, lastLogIDChannel
}

func TestPipeline(t *testing.T) {
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

	connector.EXPECT().
		Accept(gomock.Any(), drivers.NewLogWithLedger("testing", log)).
		Return([]error{nil}, nil)

	pipelineConfiguration := ledger.NewPipelineConfiguration("testing", "testing")
	pipeline := ledger.NewPipeline(pipelineConfiguration)

	_, lastLogIDChannel := runPipeline(t, ctx, pipeline, logFetcher, connector)

	close(deliver)

	ShouldReceive(t, 1, lastLogIDChannel)

	require.Eventually(t, ctrl.Satisfied, time.Second, 10*time.Millisecond)
}
