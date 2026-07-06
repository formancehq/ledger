package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/application/usagebuilder"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
)

// ensureDisjointDirs rejects any --usage-dir value that would overlap the
// primary Pebble store — the rebuild command RemoveAlls the usage dir before
// re-opening the data dir, so a colliding path silently wipes production
// data.
//
// The four rejected shapes on cleaned absolute paths:
//   - usageDir == dataDir (obvious: wipes the whole data root)
//   - usageDir is a parent of dataDir (wipes the whole data root)
//   - usageDir == <dataDir>/live (wipes Pebble's actual live directory)
//   - usageDir is a parent or child of <dataDir>/live (wipes Pebble too)
//
// The documented default is `<dataDir>/usage`, which is a sibling of
// `<dataDir>/live` and therefore safe.
func ensureDisjointDirs(dataDir, usageDir string) error {
	absData, err := filepath.Abs(dataDir)
	if err != nil {
		return fmt.Errorf("resolving --data-dir: %w", err)
	}
	absUsage, err := filepath.Abs(usageDir)
	if err != nil {
		return fmt.Errorf("resolving --usage-dir: %w", err)
	}
	absLive := filepath.Join(absData, "live")

	if absData == absUsage {
		return fmt.Errorf("--usage-dir (%s) must not equal --data-dir — running this command would delete the primary Pebble store", absUsage)
	}
	if strings.HasPrefix(absData+string(filepath.Separator), absUsage+string(filepath.Separator)) {
		return fmt.Errorf("--usage-dir (%s) must not be a parent of --data-dir (%s) — running this command would delete the primary Pebble store", absUsage, absData)
	}
	if absUsage == absLive {
		return fmt.Errorf("--usage-dir (%s) must not equal the primary Pebble live directory — running this command would delete it", absUsage)
	}
	if strings.HasPrefix(absLive+string(filepath.Separator), absUsage+string(filepath.Separator)) {
		return fmt.Errorf("--usage-dir (%s) must not be a parent of the primary Pebble live directory (%s) — running this command would delete it", absUsage, absLive)
	}
	if strings.HasPrefix(absUsage+string(filepath.Separator), absLive+string(filepath.Separator)) {
		return fmt.Errorf("--usage-dir (%s) must not live inside the primary Pebble directory (%s) — running this command would delete Pebble state", absUsage, absLive)
	}

	return nil
}

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

	// Guard against an operator passing --usage-dir equal to or inside
	// --data-dir: the RemoveAll below would then wipe (part of) the live
	// Pebble store before we ever open it read-only. Same category of
	// footgun as running `rm -rf` on a mount point.
	if err := ensureDisjointDirs(dataDir, usageDir); err != nil {
		return cmdutil.Displayed(err)
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

	// Open primary Pebble read-only — same as rebuild-indexes /
	// rebuild-audit-index (the live SSTs are at <dataDir>/live, not
	// dataDir itself).
	spinner, _ = pterm.DefaultSpinner.Start("Opening Pebble store (read-only)...")

	pebbleStore, err := dal.OpenReadOnly(filepath.Join(dataDir, "live"), logger)
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
