package store

import (
	"fmt"
	"path/filepath"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/application/indexbuilder"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// NewRebuildIndexesCommand creates the store rebuild-indexes command.
func NewRebuildIndexesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rebuild-indexes",
		Short: "Rebuild the Pebble read indexes from system logs (offline)",
		Long: `Replay all system logs from Pebble and rebuild the read index
from scratch. This is a purely offline operation — no server needed.

Use this after restoring from a backup or when the read index becomes
corrupted or out of date.`,
		RunE:              runRebuildIndexes,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("data-dir", "", "Pebble data directory (required)")
	cmd.Flags().String("read-index-dir", "", "Read index output directory (default: <data-dir>/read-indexes/)")
	cmd.Flags().Int("read-index-batch-size", 0, "Number of log entries per Pebble batch commit (0 = default 1000)")

	_ = cmd.MarkFlagRequired("data-dir")

	return cmd
}

func runRebuildIndexes(cmd *cobra.Command, _ []string) error {
	var (
		dataDir, _      = cmd.Flags().GetString("data-dir")
		readIndexDir, _ = cmd.Flags().GetString("read-index-dir")
		batchSize, _    = cmd.Flags().GetInt("read-index-batch-size")
	)

	if readIndexDir == "" {
		readIndexDir = filepath.Join(dataDir, "read-indexes")
	}

	logger := logging.NopZap()

	// Open Pebble read-only. The server keeps the live DB under <data-dir>/live
	// (see dal.NewStore); the read index lives at <data-dir>/read-indexes. Open
	// the live subdir, not the data root.
	spinner, _ := pterm.DefaultSpinner.Start("Opening Pebble store (read-only)...")

	pebbleStore, err := dal.OpenReadOnly(filepath.Join(dataDir, "live"), logger)
	if err != nil {
		spinner.Fail("Failed to open Pebble store")

		return cmdutil.Displayed(fmt.Errorf("opening Pebble store: %w", err))
	}

	defer func() { _ = pebbleStore.Close() }()

	spinner.Success("Pebble store opened")

	// Open or create Pebble read index.
	spinner, _ = pterm.DefaultSpinner.Start("Opening read index store...")

	rs, err := readstore.New(readIndexDir, logger, readstore.DefaultConfig())
	if err != nil {
		spinner.Fail("Failed to open read index store")

		return cmdutil.Displayed(fmt.Errorf("opening read index store: %w", err))
	}

	defer func() { _ = rs.Close() }()

	spinner.Success("Read index store opened at " + rs.Path())

	// Rebuild.
	spinner, _ = pterm.DefaultSpinner.Start("Rebuilding indexes from system logs...")

	builder := indexbuilder.NewBuilder(pebbleStore, rs, attributes.New(), logger, noop.Meter{}, batchSize)

	lastSeq, err := builder.RebuildAll()
	if err != nil {
		spinner.Fail("Rebuild failed")

		return cmdutil.Displayed(fmt.Errorf("rebuilding indexes: %w", err))
	}

	spinner.Success(fmt.Sprintf("Rebuild complete (last log sequence: %d)", lastSeq))

	return nil
}
