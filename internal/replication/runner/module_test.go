package runner

import (
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/internal/replication/signal"
	"net/http"
	"testing"
	"time"

	"github.com/formancehq/ledger/internal/replication/drivers"

	"go.uber.org/mock/gomock"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestModule(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)

	driversStore := drivers.NewMockStore(ctrl)

	systemStore := NewMockSystemStore(ctrl)
	systemStore.EXPECT().ListEnabledPipelines(gomock.Any()).Return(nil, nil)

	storageDriver := NewMockStorageDriver(ctrl)
	leadership := NewMockLeadership(ctrl)

	signal := signal.NewSignal(pointer.For(true))
	leadership.EXPECT().GetLeadership().Return(signal)

	var (
		runner        *Runner
		starter       *Starter
		driverFactory drivers.Factory
	)
	app := fxtest.New(t,
		fx.Supply(drivers.NewServiceConfig("", testing.Verbose())),
		fx.Supply(http.DefaultClient),
		fx.Supply(fx.Annotate(logging.Testing(), fx.As(new(logging.Logger)))),
		fx.Supply(fx.Annotate(watermill.NopLogger{}, fx.As(new(watermill.LoggerAdapter)))),
		fx.Supply(fx.Annotate(systemStore, fx.As(new(SystemStore)))),
		fx.Supply(fx.Annotate(storageDriver, fx.As(new(StorageDriver)))),
		fx.Supply(fx.Annotate(driversStore, fx.As(new(drivers.Store)))),
		fx.Provide(fx.Annotate(gochannel.NewGoChannel, fx.As(new(message.Subscriber)))),
		fx.Replace(fx.Annotate(leadership, fx.As(new(Leadership)))),
		NewFXModule(),
		fx.Populate(&starter, &runner, &driverFactory),
	)
	require.NoError(t, app.Start(ctx))
	require.Eventually(t, runner.IsReady, time.Second, 20*time.Millisecond)
	require.IsType(t, &drivers.DriverFactoryWithBatching{}, driverFactory)
	require.NoError(t, app.Stop(ctx))
}
