package store

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	grpcadp "github.com/formancehq/ledger-v3-poc/internal/adapter/grpc"
	"github.com/formancehq/ledger-v3-poc/internal/application/check"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/otlplogs"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/tarutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewBootstrapCommand creates the store bootstrap command.
func NewBootstrapCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Build a data directory from a backup tar file (offline)",
		Long: `Extract a backup tar archive into a fresh Pebble data directory,
optionally validate integrity, and finalize with checkpoint + RESTORED marker.

This is a purely offline operation — no server needed.`,
		RunE: runBootstrap,
	}

	cmd.Flags().StringP("input", "i", "", "Path to the backup tar file (required)")
	cmd.Flags().String("data-dir", "", "Target data directory (required, must be fresh)")
	cmd.Flags().Bool("validate", false, "Run integrity checks after extraction")
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")

	_ = cmd.MarkFlagRequired("input")
	_ = cmd.MarkFlagRequired("data-dir")

	return cmd
}

func runBootstrap(cmd *cobra.Command, _ []string) error {
	var (
		inputPath, _ = cmd.Flags().GetString("input")
		dataDir, _   = cmd.Flags().GetString("data-dir")
		validate, _  = cmd.Flags().GetBool("validate")
		yes, _       = cmd.Flags().GetBool("yes")
	)

	// Ensure data directory is fresh (no CURRENT_CHECKPOINT).
	cpPath := filepath.Join(dataDir, "CURRENT_CHECKPOINT")
	if _, err := os.Stat(cpPath); err == nil {
		return fmt.Errorf("data directory %s already contains CURRENT_CHECKPOINT; refusing to overwrite", dataDir)
	}

	// Create staging directory.
	stagingDir := filepath.Join(dataDir, "restore-staging")
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		return fmt.Errorf("creating staging directory: %w", err)
	}

	// Extract tar into staging.
	spinner, _ := pterm.DefaultSpinner.Start("Extracting backup archive...")

	f, err := os.Open(inputPath)
	if err != nil {
		spinner.Fail("Failed to open input file")
		return cmdutil.Displayed(fmt.Errorf("opening input file: %w", err))
	}

	if err := tarutil.ExtractTar(f, stagingDir); err != nil {
		_ = f.Close()
		spinner.Fail("Failed to extract backup")
		return cmdutil.Displayed(fmt.Errorf("extracting tar: %w", err))
	}
	_ = f.Close()

	spinner.Success("Backup extracted")

	// Open staging as read-only to read metadata.
	logger := otlplogs.NopLogger()

	store, err := dal.OpenReadOnly(stagingDir, logger)
	if err != nil {
		return fmt.Errorf("opening staging store: %w", err)
	}

	lastAppliedIndex, lastAppliedTimestamp, ledgerNames, err := readBootstrapPreviewData(store)
	if err != nil {
		_ = store.Close()
		return fmt.Errorf("reading preview data: %w", err)
	}

	_ = store.Close()

	// Print preview.
	printBootstrapPreview(lastAppliedIndex, lastAppliedTimestamp, ledgerNames)
	pterm.Println()

	// Optionally validate integrity.
	if validate {
		if err := runBootstrapValidation(cmd.Context(), stagingDir, logger); err != nil {
			return err
		}
		pterm.Println()
	}

	// Confirm unless --yes.
	if !yes {
		pterm.Warning.Println("This will finalize the data directory for use by the server.")
		pterm.Print("Continue? [y/N] ")

		reader := bufio.NewReader(os.Stdin)
		answer, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("reading confirmation: %w", err)
		}

		if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(answer)), "y") {
			pterm.Info.Println("Bootstrap cancelled")
			_ = os.RemoveAll(stagingDir)
			return nil
		}
	}

	// Create checkpoint 0 via hard-link.
	checkpointsDir := filepath.Join(dataDir, "checkpoints")
	if err := os.MkdirAll(checkpointsDir, 0755); err != nil {
		return fmt.Errorf("creating checkpoints directory: %w", err)
	}

	checkpointPath := filepath.Join(checkpointsDir, "0")
	if err := dal.HardLink(stagingDir, checkpointPath); err != nil {
		return fmt.Errorf("hard linking staging to checkpoint: %w", err)
	}

	// Write CURRENT_CHECKPOINT.
	if err := os.WriteFile(cpPath, []byte("0"), 0644); err != nil {
		return fmt.Errorf("writing CURRENT_CHECKPOINT: %w", err)
	}

	// Write RESTORED marker.
	marker := grpcadp.RestoredMarker{
		LastAppliedIndex:     lastAppliedIndex,
		LastAppliedTimestamp: lastAppliedTimestamp,
	}
	markerData, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("marshaling restored marker: %w", err)
	}

	markerPath := filepath.Join(dataDir, "RESTORED")
	if err := os.WriteFile(markerPath, markerData, 0644); err != nil {
		return fmt.Errorf("writing restored marker: %w", err)
	}

	// Remove staging directory.
	if err := os.RemoveAll(stagingDir); err != nil {
		pterm.Warning.Printfln("Failed to remove staging directory: %v", err)
	}

	pterm.Success.Printfln("Bootstrap complete (index=%d, ledgers=%d). Start the server with --bootstrap to use this data.",
		lastAppliedIndex, len(ledgerNames))

	return nil
}

// readBootstrapPreviewData reads metadata and ledger names from a store.
func readBootstrapPreviewData(store *dal.Store) (lastAppliedIndex, lastAppliedTimestamp uint64, ledgerNames []string, err error) {
	lastAppliedIndex, err = query.ReadLastAppliedIndex(store)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("getting last applied index: %w", err)
	}

	lastAppliedTimestamp, err = query.ReadLastAppliedTimestamp(store)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("getting last applied timestamp: %w", err)
	}

	cursor, err := query.ReadLedgers(context.Background(), store)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("listing ledgers: %w", err)
	}
	defer func() { _ = cursor.Close() }()

	for {
		ledger, cursorErr := cursor.Next()
		if cursorErr == io.EOF {
			break
		}
		if cursorErr != nil {
			return 0, 0, nil, fmt.Errorf("iterating ledgers: %w", cursorErr)
		}
		ledgerNames = append(ledgerNames, ledger.Name)
	}

	return lastAppliedIndex, lastAppliedTimestamp, ledgerNames, nil
}

func printBootstrapPreview(lastAppliedIndex, lastAppliedTimestamp uint64, ledgerNames []string) {
	var timestampStr string
	if lastAppliedTimestamp > 0 {
		t := time.UnixMicro(int64(lastAppliedTimestamp))
		timestampStr = t.Format(time.RFC3339)
	} else {
		timestampStr = "N/A"
	}

	pterm.DefaultSection.Println("Bootstrap Preview")

	tableData := [][]string{
		{"Last Applied Index", fmt.Sprintf("%d", lastAppliedIndex)},
		{"Last Applied Time", timestampStr},
		{"Ledger Count", fmt.Sprintf("%d", len(ledgerNames))},
		{"Ledgers", strings.Join(ledgerNames, ", ")},
	}

	_ = pterm.DefaultTable.
		WithHasHeader(false).
		WithBoxed(true).
		WithData(tableData).
		Render()
}

// runBootstrapValidation runs the integrity checker on a staging directory.
func runBootstrapValidation(ctx context.Context, stagingDir string, logger logging.Logger) error {
	store, err := dal.OpenReadOnly(stagingDir, logger)
	if err != nil {
		return fmt.Errorf("opening staging store for validation: %w", err)
	}
	defer func() { _ = store.Close() }()

	attrs := attributes.New()
	checker := check.NewChecker(store, attrs)

	var (
		validationSpinner, _ = pterm.DefaultSpinner.Start("Validating backup integrity...")
		errorCount           int
	)

	err = checker.Check(ctx, func(event *servicepb.CheckStoreEvent) {
		switch t := event.Type.(type) {
		case *servicepb.CheckStoreEvent_Progress:
			if t.Progress.TotalLogs > 0 {
				pct := float64(t.Progress.LogsChecked) / float64(t.Progress.TotalLogs) * 100
				validationSpinner.UpdateText(fmt.Sprintf("Validating backup integrity... %d/%d logs (%.0f%%)",
					t.Progress.LogsChecked, t.Progress.TotalLogs, pct))
			}
		case *servicepb.CheckStoreEvent_Error:
			errorCount++
			pterm.Printf("  %s %s\n", pterm.Red("ERROR"), t.Error.Message)
		}
	})
	if err != nil {
		validationSpinner.Fail("Failed to validate backup")
		return fmt.Errorf("running integrity check: %w", err)
	}

	_ = validationSpinner.Stop()

	pterm.Println()
	if errorCount == 0 {
		pterm.Success.Println("Backup is valid - no integrity errors found")
	} else {
		pterm.Error.Printfln("%d integrity error(s) found", errorCount)
	}

	return nil
}

