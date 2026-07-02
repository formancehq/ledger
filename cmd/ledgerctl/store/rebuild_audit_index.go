package store

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/application/auditindexer"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// NewRebuildAuditIndexCommand creates the store rebuild-audit-index command.
func NewRebuildAuditIndexCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rebuild-audit-index",
		Short: "Rebuild the audit secondary index from the Audit zone (offline)",
		Long: `Drop the audit secondary index and replay every audit entry from
the Audit zone to rebuild it from scratch. Purely offline — no server needed.

Use this after restoring from a backup or when the audit secondary index
becomes corrupted or out of date.`,
		RunE:              runRebuildAuditIndex,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("data-dir", "", "Pebble data directory (required)")
	cmd.Flags().String("read-index-dir", "", "Read index directory (default: <data-dir>/read-indexes/)")
	cmd.Flags().Int("audit-index-batch-size", 0, "Audit entries per Pebble batch commit (0 = default 1000)")

	_ = cmd.MarkFlagRequired("data-dir")

	return cmd
}

func runRebuildAuditIndex(cmd *cobra.Command, _ []string) error {
	var (
		dataDir, _      = cmd.Flags().GetString("data-dir")
		readIndexDir, _ = cmd.Flags().GetString("read-index-dir")
		batchSize, _    = cmd.Flags().GetInt("audit-index-batch-size")
	)

	if readIndexDir == "" {
		readIndexDir = filepath.Join(dataDir, "read-indexes")
	}

	logger := logging.NopZap()

	spinner, _ := pterm.DefaultSpinner.Start("Opening Pebble store (read-only)...")

	// The server keeps the live Pebble DB under <data-dir>/live (see dal.NewStore),
	// while the read index lives at <data-dir>/read-indexes. Open the live subdir,
	// not the data root.
	pebbleStore, err := dal.OpenReadOnly(filepath.Join(dataDir, "live"), logger)
	if err != nil {
		spinner.Fail("Failed to open Pebble store")

		return cmdutil.Displayed(fmt.Errorf("opening Pebble store: %w", err))
	}

	defer func() { _ = pebbleStore.Close() }()

	spinner.Success("Pebble store opened")

	spinner, _ = pterm.DefaultSpinner.Start("Opening read index store...")

	rs, err := readstore.New(readIndexDir, logger, readstore.DefaultConfig())
	if err != nil {
		spinner.Fail("Failed to open read index store")

		return cmdutil.Displayed(fmt.Errorf("opening read index store: %w", err))
	}

	defer func() { _ = rs.Close() }()

	spinner.Success("Read index store opened at " + rs.Path())

	spinner, _ = pterm.DefaultSpinner.Start("Rebuilding audit index...")

	idx := auditindexer.New(auditindexer.Config{BatchSize: batchSize}, pebbleStore, rs, logger, noop.Meter{})
	if err := idx.Rebuild(context.Background()); err != nil {
		spinner.Fail("Rebuild failed")

		return cmdutil.Displayed(fmt.Errorf("rebuilding audit index: %w", err))
	}

	spinner.Success("Audit index rebuild complete")

	return nil
}
