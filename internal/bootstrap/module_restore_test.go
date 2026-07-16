package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
)

// restoreGraphOptions assembles the restore-mode fx graph the way
// cmd/server/server.go does: the restore app module plus the conditional
// cold-storage module.
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
