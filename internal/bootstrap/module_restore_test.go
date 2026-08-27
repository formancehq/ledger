package bootstrap

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/pkg/network"
)

// restoreGraphOptions assembles the restore-mode fx graph the way
// cmd/server/server.go does: the restore app module plus the conditional
// cold-storage module, over the same supplied values.
func restoreGraphOptions(t *testing.T, coldStorageGated bool) fx.Option {
	t.Helper()

	cfg := Config{
		ClusterID: "restore-graph-test",
		DataDir:   t.TempDir(),
		Restore:   true,
		ColdStorageConfig: coldstorage.Config{
			Driver:   "s3",
			S3Bucket: "archives",
			S3Region: "us-east-1",
		},
	}

	restore := cfg.Restore
	if !coldStorageGated {
		restore = false
	}

	return fx.Options(
		fx.Supply(cfg),
		// Production supplies the zero value: the servers bind their own ports.
		fx.Supply(network.Bindings{}),
		fx.Provide(func() logging.Logger { return logging.Testing() }),
		RestoreModule(),
		ColdStorageModule(cfg.ColdStorageConfig.Driver, restore),
	)
}

// TestRestoreModeGraph_WithColdStorageEnabled pins the disaster-recovery
// regression: a server with cold storage enabled must be able to build its fx
// graph in restore mode. The Archiver consumes the runtime graph (*dal.Store,
// *state.Machine, ctrl.Admission, *node.Node) that RestoreModule deliberately
// does not provide, so the cold-storage module must stay out of restore mode.
func TestRestoreModeGraph_WithColdStorageEnabled(t *testing.T) {
	t.Parallel()

	require.NoError(t, fx.ValidateApp(restoreGraphOptions(t, true)))
}

// TestRestoreModeGraph_UngatedColdStorageIsUnbuildable documents why the gate
// exists: without it, the restore-mode graph cannot be built at all — the shape
// a cold-storage-enabled node hits at boot when asked to restore.
func TestRestoreModeGraph_UngatedColdStorageIsUnbuildable(t *testing.T) {
	t.Parallel()

	err := fx.ValidateApp(restoreGraphOptions(t, false))
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing type")
}

// TestRestoreModeReleasesEveryInjectedListener pins the ownership rule for a
// listener no module consumes. Restore mode serves HTTP and the service gRPC
// port but never the Raft one, so if the lifecycle left that socket open, the
// next node on those ports could not rebind it — which is exactly what the
// normal → restore → normal phases in tests/e2e/cluster do, and those specs are
// behind the s3 build tag where neither the default suite nor CI runs them.
func TestRestoreModeReleasesEveryInjectedListener(t *testing.T) {
	t.Parallel()

	httpListener := mustListenLoopback(t, 0)
	serviceListener := mustListenLoopback(t, 0)
	raftListener := mustListenLoopback(t, 0)

	bindings := network.Bindings{
		HTTP:    httpListener,
		Service: serviceListener,
		Raft:    raftListener,
	}

	cfg := Config{
		ClusterID:     "restore-listener-test",
		DataDir:       t.TempDir(),
		Restore:       true,
		RestoreListen: "127.0.0.1",
		HTTPPort:      httpListener.Addr().(*net.TCPAddr).Port,
		GRPCPort:      serviceListener.Addr().(*net.TCPAddr).Port,
		TLSConfig:     TLSConfig{Mode: TLSModeDisabled},
	}

	app := fx.New(
		fx.NopLogger,
		fx.Supply(cfg),
		fx.Supply(bindings),
		fx.Provide(func() logging.Logger { return logging.Testing() }),
		RestoreModule(),
	)

	startCtx, cancelStart := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStart()

	require.NoError(t, app.Start(startCtx))

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStop()

	require.NoError(t, app.Stop(stopCtx))

	for _, listener := range []net.Listener{httpListener, serviceListener, raftListener} {
		address := listener.Addr().String()

		rebound, err := net.Listen("tcp4", address)
		require.NoErrorf(t, err, "restore mode must release %s on stop", address)
		require.NoError(t, rebound.Close())
	}
}
