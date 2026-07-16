package store

import (
	"bufio"
	"context"
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

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/application/check"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// NewBootstrapCommand creates the store bootstrap command.
func NewBootstrapCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Build a data directory from a backup (offline)",
		Long: `Download backup files from S3 or Azure Blob Storage into a fresh Pebble data
directory, optionally validate integrity, and finalize with checkpoint + RESTORED marker.

This is a purely offline operation — no server needed.`,
		RunE:              runBootstrap,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmdutil.AddBackupStorageFlags(cmd)
	cmd.Flags().String("bucket-id", "", "Namespace prefix for backup files (default: uses cluster-id from config)")
	cmd.Flags().String("data-dir", "", "Target data directory (required, must be fresh)")
	cmd.Flags().Bool("validate", false, "Run integrity checks after download")
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")

	_ = cmd.MarkFlagRequired("data-dir")

	return cmd
}

func runBootstrap(cmd *cobra.Command, _ []string) error {
	var (
		bucketID, _ = cmd.Flags().GetString("bucket-id")
		dataDir, _  = cmd.Flags().GetString("data-dir")
		validate, _ = cmd.Flags().GetBool("validate")
		yes, _      = cmd.Flags().GetBool("yes")
	)

	// Ensure the data directory is fresh: no checkpoints, no live/ database
	// (normal startup prefers it over the restored checkpoint, silently
	// booting the stale store under the marker's boundary), no leftover
	// RESTORED marker.
	if err := dal.ValidateFreshRestoreTarget(dataDir); err != nil {
		return err
	}

	if marker, err := node.ReadRestoredMarker(dataDir); err != nil {
		return fmt.Errorf("checking for RESTORED marker: %w", err)
	} else if marker != nil {
		return fmt.Errorf("data directory %s already contains a RESTORED marker; refusing to overwrite", dataDir)
	}

	storageCfg, err := cmdutil.BackupStorageConfigFromFlags(cmd)
	if err != nil {
		return cmdutil.Displayed(err)
	}

	storage, err := backup.NewStorage(storageCfg)
	if err != nil {
		return cmdutil.Displayed(fmt.Errorf("creating backup storage: %w", err))
	}

	if bucketID == "" {
		bucketID = "default"
	}

	manifestKey := bucketID + "/backups/manifest.json"

	// Read manifest
	spinner, _ := pterm.DefaultSpinner.Start("Reading backup manifest...")

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

	manifestPtr, err := backup.DecodeManifest(manifestData)
	if err != nil {
		spinner.Fail("Failed to parse manifest")

		return cmdutil.Displayed(fmt.Errorf("parsing manifest: %w", err))
	}

	manifest := *manifestPtr

	if manifest.Checkpoint == nil && len(manifest.Exports) == 0 {
		spinner.Fail("Manifest contains no checkpoint and no exports")

		return cmdutil.Displayed(errors.New("manifest contains no data to restore"))
	}

	var checkpointFiles int
	if manifest.Checkpoint != nil {
		checkpointFiles = len(manifest.Checkpoint.Files)
	}

	spinner.Success(fmt.Sprintf("Manifest loaded: %d checkpoint files, %d export segments",
		checkpointFiles, len(manifest.Exports)))

	// Create staging directory
	stagingDir := filepath.Join(dataDir, "restore-staging")
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return fmt.Errorf("creating staging directory: %w", err)
	}

	// Download checkpoint files (if any)
	if manifest.Checkpoint != nil && len(manifest.Checkpoint.Files) > 0 {
		dlSpinner, _ := pterm.DefaultSpinner.Start("Downloading checkpoint files...")

		var totalBytes uint64

		for filename, cf := range manifest.Checkpoint.Files {
			// Resolve by the content-addressed key recorded in the manifest,
			// never by reconstructing prefix+filename: the manifest is the
			// authoritative pointer to the exact object bytes it committed.
			key := cf.Key
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

		dlSpinner.Success(fmt.Sprintf("Downloaded %d checkpoint files (%s)", len(manifest.Checkpoint.Files), cmdutil.FormatBytes(totalBytes)))
	}

	logger := logging.NopZap()

	// Apply export segments and rebuild derived state (if any).
	if len(manifest.Exports) > 0 {
		exportSpinner, _ := pterm.DefaultSpinner.Start("Applying export segments and rebuilding derived state...")

		exportStore, err := dal.OpenDirect(stagingDir, logger)
		if err != nil {
			exportSpinner.Fail("Failed to open staging for exports")

			return cmdutil.Displayed(err)
		}

		if err := backup.ApplyExportsAndRebuild(cmd.Context(), logger, storage, exportStore, &manifest); err != nil {
			_ = exportStore.Close()
			exportSpinner.Fail("Failed to apply export segments")

			return cmdutil.Displayed(err)
		}

		if err := exportStore.Close(); err != nil {
			exportSpinner.Fail("Failed to close staging after exports")

			return cmdutil.Displayed(err)
		}

		exportSpinner.Success(fmt.Sprintf("Applied %d export segments and rebuilt derived state", len(manifest.Exports)))
	}

	// Open staging as read-only to read metadata.

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

	// Prepare attributes for backup (Global-zone resets; no compaction).
	prepareSpinner, _ := pterm.DefaultSpinner.Start("Preparing attributes for restore compatibility...")

	prepareStore, err := dal.OpenDirect(stagingDir, logger)
	if err != nil {
		prepareSpinner.Fail("Failed to open staging for backup preparation")

		return cmdutil.Displayed(fmt.Errorf("opening staging for backup preparation: %w", err))
	}

	if err := attributes.PrepareForBackup(prepareStore); err != nil {
		_ = prepareStore.Close()
		prepareSpinner.Fail("Failed to prepare attributes for backup")

		return cmdutil.Displayed(fmt.Errorf("preparing backup attributes: %w", err))
	}

	if err := prepareStore.Close(); err != nil {
		prepareSpinner.Fail("Failed to close prepared store")

		return cmdutil.Displayed(fmt.Errorf("closing prepared staging: %w", err))
	}

	prepareSpinner.Success("Attributes prepared for backup")

	// Re-read metadata after backup preparation
	store, err = dal.OpenReadOnly(stagingDir, logger)
	if err != nil {
		return fmt.Errorf("re-opening staging store: %w", err)
	}

	lastAppliedIndex, lastAppliedTimestamp, _, err = readBootstrapPreviewData(store)
	if err != nil {
		_ = store.Close()

		return fmt.Errorf("re-reading preview data after backup preparation: %w", err)
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

	// The marker is the restore's commit point — written only after the
	// checkpoint is in place, atomically.
	if err := node.WriteRestoredMarker(dataDir, node.RestoredMarker{
		LastAppliedIndex:     lastAppliedIndex,
		LastAppliedTimestamp: lastAppliedTimestamp,
	}); err != nil {
		// Roll the placement back: a checkpoint without its marker would
		// make the freshness guard refuse a re-run of this command.
		if rmErr := os.RemoveAll(checkpointPath); rmErr != nil {
			pterm.Warning.Printfln("Failed to remove checkpoint after marker write failure; delete %s manually before retrying: %v", checkpointPath, rmErr)
		}

		return err
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

	readHandle, handleErr := store.NewDirectReadHandle()
	if handleErr != nil {
		return 0, 0, nil, fmt.Errorf("creating read handle: %w", handleErr)
	}
	defer func() { _ = readHandle.Close() }()

	cursor, err := query.ReadLedgers(context.Background(), readHandle)
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

	persisted, err := query.ReadPersistedConfig(store)
	if err != nil {
		return fmt.Errorf("loading persisted config from staging store: %w", err)
	}

	if persisted == nil {
		return errors.New("staging store has no persisted config; cannot validate audit chain (incomplete or malformed backup?)")
	}

	attrs := attributes.New()
	// No cold reader on this path: it validates a local staging store from a
	// backup, so the idempotency pass keeps the post-archive boundary as its
	// verification floor. nil TTL: no trusted runtime config for a foreign
	// backup, so the pass falls back to the backup's persisted TTL.
	checker := check.NewChecker(store, attrs, persisted.GetClusterId(), nil, nil, logger)

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

	if err := cmdutil.IntegrityResult("backup validation", errorCount); err != nil {
		return err
	}

	pterm.Success.Println("Backup is valid - no integrity errors found")

	return nil
}
