package store

import (
	"fmt"
	"path/filepath"

	"github.com/formancehq/ledger-v3-poc/internal/application/indexbuilder"
	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/otlplogs"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/metric/noop"
)

// NewRebuildIndexesCommand creates the store rebuild-indexes command.
func NewRebuildIndexesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rebuild-indexes",
		Short: "Rebuild the bbolt read indexes from Pebble logs (offline)",
		Long: `Replay all system logs from Pebble and rebuild the bbolt read index
from scratch. This is a purely offline operation — no server needed.

Use this after restoring from a backup or when the read index becomes
corrupted or out of date.`,
		RunE: runRebuildIndexes,
	}

	cmd.Flags().String("data-dir", "", "Pebble data directory (required)")
	cmd.Flags().String("read-index-dir", "", "Read index output directory (default: <data-dir>/read-indexes/)")
	cmd.Flags().Bool("read-index-no-freelist-sync", false, "Skip bbolt freelist serialization (faster rebuild)")
	cmd.Flags().Int("read-index-batch-size", 0, "Number of log entries per bbolt write transaction (0 = default 1000)")

	_ = cmd.MarkFlagRequired("data-dir")

	return cmd
}

func runRebuildIndexes(cmd *cobra.Command, _ []string) error {
	var (
		dataDir, _        = cmd.Flags().GetString("data-dir")
		readIndexDir, _   = cmd.Flags().GetString("read-index-dir")
		noFreelistSync, _ = cmd.Flags().GetBool("read-index-no-freelist-sync")
		batchSize, _      = cmd.Flags().GetInt("read-index-batch-size")
	)

	if readIndexDir == "" {
		readIndexDir = filepath.Join(dataDir, "read-indexes")
	}

	logger := otlplogs.NopLogger()

	// Open Pebble read-only.
	spinner, _ := pterm.DefaultSpinner.Start("Opening Pebble store (read-only)...")

	pebbleStore, err := dal.OpenReadOnly(dataDir, logger)
	if err != nil {
		spinner.Fail("Failed to open Pebble store")
		return fmt.Errorf("opening Pebble store: %w", err)
	}
	defer func() { _ = pebbleStore.Close() }()

	spinner.Success("Pebble store opened")

	// Open or create bbolt read index.
	spinner, _ = pterm.DefaultSpinner.Start("Opening read index store...")

	rs, err := readstore.New(readIndexDir, noFreelistSync, 0, logger)
	if err != nil {
		spinner.Fail("Failed to open read index store")
		return fmt.Errorf("opening read index store: %w", err)
	}
	defer func() { _ = rs.Close() }()

	spinner.Success(fmt.Sprintf("Read index store opened at %s", rs.Path()))

	// Rebuild.
	spinner, _ = pterm.DefaultSpinner.Start("Rebuilding indexes from system logs...")

	builder := indexbuilder.NewBuilder(pebbleStore, rs, logger, noop.Meter{}, batchSize)

	lastSeq, err := builder.RebuildAll()
	if err != nil {
		spinner.Fail("Rebuild failed")
		return fmt.Errorf("rebuilding indexes: %w", err)
	}

	spinner.Success(fmt.Sprintf("Rebuild complete (last log sequence: %d)", lastSeq))

	// Sync freelist to disk so the next Open() is fast.
	if noFreelistSync {
		spinner, _ = pterm.DefaultSpinner.Start("Syncing freelist to disk...")
		if err := rs.SyncFreelist(); err != nil {
			spinner.Fail("Failed to sync freelist")
			return fmt.Errorf("syncing freelist: %w", err)
		}
		spinner.Success("Freelist synced")
	}

	return nil
}

