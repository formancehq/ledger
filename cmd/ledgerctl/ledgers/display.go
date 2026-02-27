package ledgers

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// ledgerModeString returns a user-friendly string for a LedgerMode.
func ledgerModeString(mode commonpb.LedgerMode) string {
	switch mode {
	case commonpb.LedgerMode_LEDGER_MODE_MIRROR:
		return "MIRROR"
	default:
		return "NORMAL"
	}
}

// syncStateString returns a user-friendly string for a MirrorSyncState.
func syncStateString(state commonpb.MirrorSyncState) string {
	switch state {
	case commonpb.MirrorSyncState_MIRROR_SYNC_STATE_FOLLOWING:
		return "FOLLOWING"
	default:
		return "SYNCING"
	}
}

// renderMirrorSource displays mirror source configuration.
func renderMirrorSource(src *commonpb.MirrorSourceConfig) {
	pterm.Println()
	pterm.Println("Mirror Source:")
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	pterm.Printf("  Ledger:  %s\n", src.LedgerName)

	switch s := src.Type.(type) {
	case *commonpb.MirrorSourceConfig_Http:
		pterm.Printf("  Type:    HTTP\n")
		pterm.Printf("  URL:     %s\n", s.Http.BaseUrl)
		if s.Http.AuthToken != "" {
			pterm.Printf("  Auth:    ****\n")
		}
	case *commonpb.MirrorSourceConfig_Postgres:
		pterm.Printf("  Type:    PostgreSQL\n")
		pterm.Printf("  DSN:     %s\n", cmdutil.ObfuscateDSN(s.Postgres.Dsn))
	}

	if src.BatchSize > 0 {
		pterm.Printf("  Batch:   %d\n", src.BatchSize)
	}
}

// renderMirrorSyncProgress displays mirror sync progress information.
func renderMirrorSyncProgress(progress *commonpb.MirrorSyncProgress) {
	pterm.Println()
	pterm.Println("Sync Progress:")
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	pterm.Printf("  State:     %s\n", syncStateString(progress.State))
	pterm.Printf("  Cursor:    %d\n", progress.Cursor)

	if progress.SourceLogCount > 0 {
		pterm.Printf("  Total:     %d\n", progress.SourceLogCount)
		pterm.Printf("  Remaining: %d\n", progress.RemainingLogs)

		if progress.SourceLogCount > 0 {
			pct := float64(progress.Cursor) / float64(progress.SourceLogCount) * 100
			if pct > 100 {
				pct = 100
			}
			pterm.Printf("  Progress:  %.1f%%\n", pct)
		}
	}

	if progress.Error != nil {
		pterm.Printf("  Error:     %s\n", pterm.Red(progress.Error.Message))
		if progress.Error.OccurredAt != nil {
			pterm.Printf("  Error At:  %s\n", progress.Error.OccurredAt.AsTime().Format("2006-01-02T15:04:05Z07:00"))
		}
	}
}

// parseMirrorFlags parses mirror-related flags and returns the mode and source config.
// If any --mirror-* flag is explicitly set, mode is inferred as "mirror".
func parseMirrorFlags(cmd *cobra.Command, ledgerName string) (commonpb.LedgerMode, *commonpb.MirrorSourceConfig, error) {
	modeStr, _ := cmd.Flags().GetString("mode")

	// Auto-infer mirror mode when mirror flags are explicitly provided
	hasMirrorFlags := cmd.Flags().Changed("mirror-source-type") ||
		cmd.Flags().Changed("mirror-ledger-name") ||
		cmd.Flags().Changed("mirror-base-url") ||
		cmd.Flags().Changed("mirror-auth-token") ||
		cmd.Flags().Changed("mirror-dsn") ||
		cmd.Flags().Changed("mirror-batch-size")

	if hasMirrorFlags && !cmd.Flags().Changed("mode") {
		modeStr = "mirror"
	}

	switch modeStr {
	case "normal", "":
		if hasMirrorFlags {
			return 0, nil, fmt.Errorf("mirror flags provided but --mode is set to 'normal'; use --mode=mirror")
		}
		return commonpb.LedgerMode_LEDGER_MODE_NORMAL, nil, nil
	case "mirror":
		// Continue to build mirror source config
	default:
		return 0, nil, fmt.Errorf("invalid mode %q: must be 'normal' or 'mirror'", modeStr)
	}

	sourceType, _ := cmd.Flags().GetString("mirror-source-type")
	sourceLedgerName, _ := cmd.Flags().GetString("mirror-ledger-name")
	if sourceLedgerName == "" {
		sourceLedgerName = ledgerName
	}
	batchSize, _ := cmd.Flags().GetUint32("mirror-batch-size")

	cfg := &commonpb.MirrorSourceConfig{
		LedgerName: sourceLedgerName,
		BatchSize:  batchSize,
	}

	switch sourceType {
	case "http", "":
		baseURL, _ := cmd.Flags().GetString("mirror-base-url")
		if baseURL == "" {
			return 0, nil, fmt.Errorf("--mirror-base-url is required for http mirror source")
		}
		authToken, _ := cmd.Flags().GetString("mirror-auth-token")
		cfg.Type = &commonpb.MirrorSourceConfig_Http{
			Http: &commonpb.HttpMirrorSourceConfig{
				BaseUrl:   baseURL,
				AuthToken: authToken,
			},
		}
	case "postgres":
		dsn, _ := cmd.Flags().GetString("mirror-dsn")
		if dsn == "" {
			return 0, nil, fmt.Errorf("--mirror-dsn is required for postgres mirror source")
		}
		cfg.Type = &commonpb.MirrorSourceConfig_Postgres{
			Postgres: &commonpb.PostgresMirrorSourceConfig{
				Dsn: dsn,
			},
		}
	default:
		return 0, nil, fmt.Errorf("invalid mirror source type %q: must be 'http' or 'postgres'", sourceType)
	}

	return commonpb.LedgerMode_LEDGER_MODE_MIRROR, cfg, nil
}
