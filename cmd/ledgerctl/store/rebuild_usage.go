package store

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/application/usagebuilder"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
)

// NewRebuildUsageCommand creates the store rebuild-usage command.
func NewRebuildUsageCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rebuild-usage",
		Short: "Rebuild the usage store from the audit chain (offline)",
		Long: `Drop the usage store directory and replay every audit entry from
sequence 0, rebuilding all per-template usage counters and per-ledger event
counters from scratch. This is a purely offline operation — the server must
be stopped.

Use this after restoring from a backup, when the usage store becomes
corrupted, or when audit-chain history has been extended and you want the
counters to reflect the full history that is still available in Pebble.

Note that audit entries archived to cold storage are not reconstructed —
the rebuild only replays what is currently reachable in the primary Pebble
store.`,
		RunE:              runRebuildUsage,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("data-dir", "", "Pebble data directory (required)")
	cmd.Flags().String("usage-dir", "", "Usage store output directory (default: <data-dir>/usage/)")
	cmd.Flags().Int("usage-batch-size", 0, "Number of audit entries per Pebble batch commit (0 = default 200)")

	_ = cmd.MarkFlagRequired("data-dir")

	return cmd
}

func runRebuildUsage(cmd *cobra.Command, _ []string) error {
	var (
		dataDir, _   = cmd.Flags().GetString("data-dir")
		usageDir, _  = cmd.Flags().GetString("usage-dir")
		batchSize, _ = cmd.Flags().GetInt("usage-batch-size")
	)

	if usageDir == "" {
		usageDir = filepath.Join(dataDir, "usage")
	}

	logger := logging.NopZap()

	// Drop the existing usage store so the builder starts at cursor=0 and
	// no stale counter survives the rebuild.
	spinner, _ := pterm.DefaultSpinner.Start("Dropping existing usage store...")

	if err := os.RemoveAll(usageDir); err != nil {
		spinner.Fail("Failed to drop usage store directory")

		return cmdutil.Displayed(fmt.Errorf("removing %s: %w", usageDir, err))
	}

	spinner.Success("Usage store dropped at " + usageDir)

	// Open primary Pebble read-only — same as rebuild-indexes.
	spinner, _ = pterm.DefaultSpinner.Start("Opening Pebble store (read-only)...")

	pebbleStore, err := dal.OpenReadOnly(dataDir, logger)
	if err != nil {
		spinner.Fail("Failed to open Pebble store")

		return cmdutil.Displayed(fmt.Errorf("opening Pebble store: %w", err))
	}

	defer func() { _ = pebbleStore.Close() }()

	spinner.Success("Pebble store opened")

	// Create the fresh usage store.
	spinner, _ = pterm.DefaultSpinner.Start("Creating usage store...")

	us, err := usagestore.New(usageDir, logger, usagestore.DefaultConfig())
	if err != nil {
		spinner.Fail("Failed to create usage store")

		return cmdutil.Displayed(fmt.Errorf("creating usage store: %w", err))
	}

	defer func() { _ = us.Close() }()

	spinner.Success("Usage store created at " + us.Path())

	// Rebuild — notifications is nil in offline mode (no FSM running).
	spinner, _ = pterm.DefaultSpinner.Start("Rebuilding usage projections from the audit chain...")

	builder := usagebuilder.NewBuilder(pebbleStore, us, nil, logger, noop.Meter{}, batchSize)

	lastSeq, err := builder.RebuildAll()
	if err != nil {
		spinner.Fail("Rebuild failed")

		return cmdutil.Displayed(fmt.Errorf("rebuilding usage projections: %w", err))
	}

	spinner.Success(fmt.Sprintf("Rebuild complete (last audit sequence: %d)", lastSeq))

	return nil
}
