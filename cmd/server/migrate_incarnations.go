package server

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/incarnationmigration"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// NewMigrateIncarnationsCommand runs the local, resumable historical backfill
// without starting Raft or any service listeners. The node must be stopped so
// Pebble's exclusive lock protects the migration from concurrent FSM writes.
func NewMigrateIncarnationsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate-incarnations",
		Short: "Backfill account incarnation history in a stopped node's data store",
		Args:  cobra.NoArgs,
		RunE:  runMigrateIncarnations,
	}
	cmd.Flags().String("data-dir", "./data", "Data directory of the stopped Ledger node")
	cmd.Flags().Duration("pause-after-batch", 0, "Pause after each 256-audit-entry commit to limit migration load")

	return cmd
}

func runMigrateIncarnations(cmd *cobra.Command, _ []string) error {
	dataDir, err := cmd.Flags().GetString("data-dir")
	if err != nil {
		return err
	}
	files, err := os.ReadDir(filepath.Join(dataDir, "live"))
	switch {
	case err == nil:
		if len(files) == 0 {
			return errors.New("opening existing Ledger data store: live directory is empty")
		}
	case os.IsNotExist(err):
		_, found, scanErr := dal.ScanLatestCheckpointID(dataDir)
		if scanErr != nil {
			return fmt.Errorf("scanning existing Ledger checkpoints: %w", scanErr)
		}
		if !found {
			return fmt.Errorf("opening existing Ledger data store: no live store or checkpoint in %s", dataDir)
		}
	default:
		return fmt.Errorf("opening existing Ledger data store: %w", err)
	}
	logger := logging.NewDefaultLogger(os.Stderr, false, false, false)
	store, err := dal.NewStore(dataDir, logger, noop.NewMeterProvider().Meter("ledger.migrate-incarnations"), dal.DefaultConfig())
	if err != nil {
		return fmt.Errorf("opening Ledger data store: %w", err)
	}
	defer func() { _ = store.Close() }()
	policy, err := query.ReadClusterPolicy(store)
	if err != nil {
		return fmt.Errorf("reading cluster policy: %w", err)
	}
	if !capabilities.IsActive(policy, capabilities.AccountIncarnationWritesV1) {
		return errors.New("AccountIncarnationWritesV1 must be active before migrating account incarnations")
	}
	pause, err := cmd.Flags().GetDuration("pause-after-batch")
	if err != nil {
		return err
	}
	if pause < 0 {
		return errors.New("pause-after-batch must be non-negative")
	}
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx = logging.ContextWithLogger(ctx, logger)
	if err := incarnationmigration.RunWithPause(ctx, store, pause); err != nil {
		return fmt.Errorf("migrating account incarnations: %w", err)
	}

	return nil
}
