package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// TestWALProviderDependsOnNodeIdentity pins the Fx construction order: the
// node-config provider must persist INSTANCE_ID before provideWAL creates any
// WAL lifecycle artifact. Without the typed dependency, Fx is free to build
// the WAL first and a legitimate first boot trips EnsureInstanceID's
// missing-marker fail-closed check.
func TestWALProviderDependsOnNodeIdentity(t *testing.T) {
	t.Parallel()

	walDir := t.TempDir()
	var providedWAL *wal.DefaultWAL

	app := fx.New(
		fx.NopLogger,
		fx.Supply(Config{RaftConfig: node.NodeConfig{WalDir: walDir}}),
		fx.Provide(
			func() logging.Logger { return logging.Testing() },
			func() metric.MeterProvider { return noop.NewMeterProvider() },
			provideNodeConfig,
			provideWAL,
		),
		fx.Populate(&providedWAL),
	)
	require.NoError(t, app.Err())
	require.NotNil(t, providedWAL)

	t.Cleanup(func() {
		require.NoError(t, providedWAL.Close())
		require.NoError(t, app.Stop(context.Background()))
	})

	instanceID, err := wal.ReadInstanceID(walDir)
	require.NoError(t, err)
	require.Len(t, instanceID, wal.InstanceIDLen)
}
