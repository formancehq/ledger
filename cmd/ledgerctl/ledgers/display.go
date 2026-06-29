package ledgers

import (
	"errors"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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

	pterm.Printf("  Ledger:  %s\n", src.GetLedgerName())

	switch s := src.GetType().(type) {
	case *commonpb.MirrorSourceConfig_Http:
		pterm.Printf("  Type:    HTTP\n")
		pterm.Printf("  URL:     %s\n", s.Http.GetBaseUrl())

		if cc := s.Http.GetOauth2ClientCredentials(); cc != nil {
			pterm.Printf("  OAuth2:  client_id=%s endpoint=%s\n", cc.GetClientId(), cc.GetTokenEndpoint())
		}
	case *commonpb.MirrorSourceConfig_Postgres:
		pterm.Printf("  Type:    PostgreSQL\n")
		pterm.Printf("  DSN:     %s\n", cmdutil.ObfuscateDSN(s.Postgres.GetDsn()))

		if iam := s.Postgres.GetAwsIamAuth(); iam != nil {
			pterm.Printf("  IAM:     AWS RDS IAM auth (region=%s)\n", iam.GetRegion())

			if role := iam.GetAssumeRoleArn(); role != "" {
				pterm.Printf("  Role:    %s\n", role)
			}
		}
	}

	if src.GetBatchSize() > 0 {
		pterm.Printf("  Batch:   %d\n", src.GetBatchSize())
	}
}

// renderMirrorSyncProgress displays mirror sync progress information.
func renderMirrorSyncProgress(progress *commonpb.MirrorSyncProgress) {
	pterm.Println()
	pterm.Println("Sync Progress:")
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	pterm.Printf("  State:     %s\n", syncStateString(progress.GetState()))
	pterm.Printf("  Cursor:    %d\n", progress.GetCursor())

	if progress.GetSourceLogCount() > 0 {
		pterm.Printf("  Total:     %d\n", progress.GetSourceLogCount())
		pterm.Printf("  Remaining: %d\n", progress.GetRemainingLogs())

		if progress.GetSourceLogCount() > 0 {
			pct := float64(progress.GetCursor()) / float64(progress.GetSourceLogCount()) * 100
			if pct > 100 {
				pct = 100
			}

			pterm.Printf("  Progress:  %.1f%%\n", pct)
		}
	}

	if progress.GetError() != nil {
		pterm.Printf("  Error:     %s\n", pterm.Red(progress.GetError().GetMessage()))

		if progress.GetError().GetOccurredAt() != nil {
			pterm.Printf("  Error At:  %s\n", progress.GetError().GetOccurredAt().AsTime().Format("2006-01-02T15:04:05Z07:00"))
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
		cmd.Flags().Changed("mirror-oauth2-client-id") ||
		cmd.Flags().Changed("mirror-oauth2-client-secret") ||
		cmd.Flags().Changed("mirror-oauth2-token-endpoint") ||
		cmd.Flags().Changed("mirror-oauth2-scopes") ||
		cmd.Flags().Changed("mirror-dsn") ||
		cmd.Flags().Changed("mirror-aws-iam-region") ||
		cmd.Flags().Changed("mirror-aws-iam-assume-role-arn") ||
		cmd.Flags().Changed("mirror-batch-size")

	if hasMirrorFlags && !cmd.Flags().Changed("mode") {
		modeStr = "mirror"
	}

	switch modeStr {
	case "normal", "":
		if hasMirrorFlags {
			return 0, nil, errors.New("mirror flags provided but --mode is set to 'normal'; use --mode=mirror")
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
			return 0, nil, errors.New("--mirror-base-url is required for http mirror source")
		}

		httpCfg := &commonpb.HttpMirrorSourceConfig{
			BaseUrl: baseURL,
		}
		oauth2ClientID, _ := cmd.Flags().GetString("mirror-oauth2-client-id")

		oauth2TokenEndpoint, _ := cmd.Flags().GetString("mirror-oauth2-token-endpoint")
		if oauth2ClientID != "" || oauth2TokenEndpoint != "" {
			oauth2ClientSecret, _ := cmd.Flags().GetString("mirror-oauth2-client-secret")
			oauth2Scopes, _ := cmd.Flags().GetStringArray("mirror-oauth2-scopes")
			httpCfg.Oauth2ClientCredentials = &commonpb.OAuth2ClientCredentials{
				ClientId:      oauth2ClientID,
				ClientSecret:  oauth2ClientSecret,
				TokenEndpoint: oauth2TokenEndpoint,
				Scopes:        oauth2Scopes,
			}
		}

		cfg.Type = &commonpb.MirrorSourceConfig_Http{
			Http: httpCfg,
		}
	case "postgres":
		dsn, _ := cmd.Flags().GetString("mirror-dsn")
		if dsn == "" {
			return 0, nil, errors.New("--mirror-dsn is required for postgres mirror source")
		}

		pgCfg := &commonpb.PostgresMirrorSourceConfig{
			Dsn: dsn,
		}

		if cmd.Flags().Changed("mirror-aws-iam-region") {
			iamRegion, _ := cmd.Flags().GetString("mirror-aws-iam-region")
			if iamRegion == "" {
				return 0, nil, errors.New("--mirror-aws-iam-region must be a non-empty region when set (got empty value)")
			}
			pgCfg.AwsIamAuth = &commonpb.PostgresAwsIamAuth{
				Region: iamRegion,
			}
		}

		if assumeRoleArn, _ := cmd.Flags().GetString("mirror-aws-iam-assume-role-arn"); assumeRoleArn != "" {
			if pgCfg.GetAwsIamAuth() == nil {
				return 0, nil, errors.New("--mirror-aws-iam-assume-role-arn requires --mirror-aws-iam-region to be set")
			}
			pgCfg.AwsIamAuth.AssumeRoleArn = assumeRoleArn
		}

		cfg.Type = &commonpb.MirrorSourceConfig_Postgres{
			Postgres: pgCfg,
		}
	default:
		return 0, nil, fmt.Errorf("invalid mirror source type %q: must be 'http' or 'postgres'", sourceType)
	}

	return commonpb.LedgerMode_LEDGER_MODE_MIRROR, cfg, nil
}
