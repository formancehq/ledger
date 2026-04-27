package store

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/application/check"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/backup"
	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/otlplogs"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// NewBootstrapCommand creates the store bootstrap command.
func NewBootstrapCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Build a data directory from an S3 backup (offline)",
		Long: `Download backup files from S3 into a fresh Pebble data directory,
optionally validate integrity, and finalize with checkpoint + RESTORED marker.

This is a purely offline operation — no server needed.`,
		RunE: runBootstrap,
	}

	cmd.Flags().String("s3-bucket", "", "S3 bucket containing the backup (required)")
	cmd.Flags().String("s3-region", "", "AWS region for S3 bucket")
	cmd.Flags().String("s3-endpoint", "", "Custom S3 endpoint (for MinIO)")
	cmd.Flags().String("bucket-id", "", "Namespace prefix for backup files (default: uses cluster-id from config)")
	cmd.Flags().String("data-dir", "", "Target data directory (required, must be fresh)")
	cmd.Flags().Bool("validate", false, "Run integrity checks after download")
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")

	_ = cmd.MarkFlagRequired("s3-bucket")
	_ = cmd.MarkFlagRequired("data-dir")

	return cmd
}

func runBootstrap(cmd *cobra.Command, _ []string) error {
	var (
		s3Bucket, _   = cmd.Flags().GetString("s3-bucket")
		s3Region, _   = cmd.Flags().GetString("s3-region")
		s3Endpoint, _ = cmd.Flags().GetString("s3-endpoint")
		bucketID, _   = cmd.Flags().GetString("bucket-id")
		dataDir, _    = cmd.Flags().GetString("data-dir")
		validate, _   = cmd.Flags().GetBool("validate")
		yes, _        = cmd.Flags().GetBool("yes")
	)

	// Ensure data directory is fresh (no CURRENT_CHECKPOINT).
	cpPath := filepath.Join(dataDir, "CURRENT_CHECKPOINT")
	if _, err := os.Stat(cpPath); err == nil {
		return fmt.Errorf("data directory %s already contains CURRENT_CHECKPOINT; refusing to overwrite", dataDir)
	}

	// Create S3 storage
	storage, err := backup.NewStorage("s3", "", s3Bucket, s3Region, s3Endpoint)
	if err != nil {
		return cmdutil.Displayed(fmt.Errorf("creating S3 storage: %w", err))
	}

	if bucketID == "" {
		bucketID = "default"
	}

	manifestKey := bucketID + "/backups/manifest.json"
	fileKeyPrefix := bucketID + "/backups/data/"

	// Read manifest
	spinner, _ := pterm.DefaultSpinner.Start("Reading backup manifest from S3...")

	manifestReader, err := storage.GetFile(cmd.Context(), manifestKey)
	if err != nil {
		spinner.Fail("Failed to read manifest")

		return cmdutil.Displayed(fmt.Errorf("reading backup manifest: %w", err))
	}

	manifestData, err := io.ReadAll(manifestReader)
	_ = manifestReader.Close()

	if err != nil {
		spinner.Fail("Failed to read manifest")

		return cmdutil.Displayed(fmt.Errorf("reading manifest data: %w", err))
	}

	var manifest backup.Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		spinner.Fail("Failed to parse manifest")

		return cmdutil.Displayed(fmt.Errorf("parsing manifest: %w", err))
	}

	spinner.Success(fmt.Sprintf("Manifest loaded: %d files", len(manifest.Files)))

	// Create staging directory
	stagingDir := filepath.Join(dataDir, "restore-staging")
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return fmt.Errorf("creating staging directory: %w", err)
	}

	// Download files
	dlSpinner, _ := pterm.DefaultSpinner.Start("Downloading backup files from S3...")

	var totalBytes uint64

	for filename := range manifest.Files {
		key := fileKeyPrefix + filename
		destPath := filepath.Join(stagingDir, filepath.FromSlash(filename))

		if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
			dlSpinner.Fail("Failed to create directory")

			return cmdutil.Displayed(fmt.Errorf("creating directory for %s: %w", filename, err))
		}

		reader, err := storage.GetFile(cmd.Context(), key)
		if err != nil {
			dlSpinner.Fail("Failed to download file")

			return cmdutil.Displayed(fmt.Errorf("downloading %s: %w", filename, err))
		}

		outFile, err := os.Create(destPath)
		if err != nil {
			_ = reader.Close()
			dlSpinner.Fail("Failed to create file")

			return cmdutil.Displayed(fmt.Errorf("creating file %s: %w", filename, err))
		}

		n, err := io.Copy(outFile, reader)
		_ = reader.Close()
		_ = outFile.Close()

		if err != nil {
			dlSpinner.Fail("Failed to write file")

			return cmdutil.Displayed(fmt.Errorf("writing file %s: %w", filename, err))
		}

		totalBytes += uint64(n)
	}

	dlSpinner.Success(fmt.Sprintf("Downloaded %d files (%s)", len(manifest.Files), cmdutil.FormatBytes(totalBytes)))

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

	printBootstrapPreview(lastAppliedIndex, lastAppliedTimestamp, ledgerNames)
	pterm.Println()

	if validate {
		err := runBootstrapValidation(cmd.Context(), stagingDir, logger)
		if err != nil {
			return err
		}

		pterm.Println()
	}

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

	// Compact attributes
	compactSpinner, _ := pterm.DefaultSpinner.Start("Compacting attributes for restore compatibility...")

	compactStore, err := dal.OpenDirect(stagingDir, logger)
	if err != nil {
		compactSpinner.Fail("Failed to open staging for compaction")

		return cmdutil.Displayed(fmt.Errorf("opening staging for compaction: %w", err))
	}

	if err := attributes.CompactAllForBackup(compactStore); err != nil {
		_ = compactStore.Close()
		compactSpinner.Fail("Failed to compact attributes")

		return cmdutil.Displayed(fmt.Errorf("compacting backup attributes: %w", err))
	}

	if err := compactStore.Close(); err != nil {
		compactSpinner.Fail("Failed to close compacted store")

		return cmdutil.Displayed(fmt.Errorf("closing compacted staging: %w", err))
	}

	compactSpinner.Success("Attributes compacted")

	// Re-read metadata after compaction
	store, err = dal.OpenReadOnly(stagingDir, logger)
	if err != nil {
		return fmt.Errorf("re-opening staging store: %w", err)
	}

	lastAppliedIndex, lastAppliedTimestamp, _, err = readBootstrapPreviewData(store)
	if err != nil {
		_ = store.Close()

		return fmt.Errorf("re-reading preview data after compaction: %w", err)
	}

	_ = store.Close()

	// Create checkpoint 0 via hard-link
	checkpointsDir := filepath.Join(dataDir, "checkpoints")
	if err := os.MkdirAll(checkpointsDir, 0o755); err != nil {
		return fmt.Errorf("creating checkpoints directory: %w", err)
	}

	checkpointPath := filepath.Join(checkpointsDir, "0")
	if err := dal.HardLink(stagingDir, checkpointPath); err != nil {
		return fmt.Errorf("hard linking staging to checkpoint: %w", err)
	}

	// Write CURRENT_CHECKPOINT (atomic)
	if err := dal.WriteCurrentCheckpointAtomic(dataDir, 0); err != nil {
		return fmt.Errorf("writing CURRENT_CHECKPOINT: %w", err)
	}

	// Write RESTORED marker
	marker := node.RestoredMarker{
		LastAppliedIndex:     lastAppliedIndex,
		LastAppliedTimestamp: lastAppliedTimestamp,
	}

	markerData, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("marshaling restored marker: %w", err)
	}

	markerPath := filepath.Join(dataDir, "RESTORED")
	if err := os.WriteFile(markerPath, markerData, 0o644); err != nil {
		return fmt.Errorf("writing restored marker: %w", err)
	}

	// Remove staging directory
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
		if errors.Is(cursorErr, io.EOF) {
			break
		}

		if cursorErr != nil {
			return 0, 0, nil, fmt.Errorf("iterating ledgers: %w", cursorErr)
		}

		ledgerNames = append(ledgerNames, ledger.GetName())
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
		{"Last Applied Index", strconv.FormatUint(lastAppliedIndex, 10)},
		{"Last Applied Time", timestampStr},
		{"Ledger Count", strconv.Itoa(len(ledgerNames))},
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
	checker := check.NewChecker(store, attrs, logger)

	pterm.Info.Println("Validating backup integrity...")

	var errorCount int

	err = checker.Check(ctx, func(event *servicepb.CheckStoreEvent) {
		switch t := event.GetType().(type) {
		case *servicepb.CheckStoreEvent_Progress:
			if t.Progress.GetTotalLogs() > 0 {
				pct := float64(t.Progress.GetLogsChecked()) / float64(t.Progress.GetTotalLogs()) * 100
				pterm.Printf("\r  Validating backup integrity... %d/%d logs (%.0f%%)",
					t.Progress.GetLogsChecked(), t.Progress.GetTotalLogs(), pct)
			}
		case *servicepb.CheckStoreEvent_Error:
			errorCount++

			pterm.Printf("\n  %s %s\n", pterm.Red("ERROR"), t.Error.GetMessage())
		}
	})

	pterm.Println()

	if err != nil {
		pterm.Error.Println("Failed to validate backup")

		return fmt.Errorf("running integrity check: %w", err)
	}

	pterm.Println()

	if errorCount == 0 {
		pterm.Success.Println("Backup is valid - no integrity errors found")
	} else {
		pterm.Error.Printfln("%d integrity error(s) found", errorCount)
	}

	return nil
}
