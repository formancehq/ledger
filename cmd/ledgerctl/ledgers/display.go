package ledgers

import (
	stdjson "encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v3"
	"google.golang.org/protobuf/encoding/protojson"

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
	case *commonpb.MirrorSourceConfig_LedgerV2Http:
		pterm.Printf("  Type:    ledgerV2Http\n")
		pterm.Printf("  URL:     %s\n", s.LedgerV2Http.GetBaseUrl())

		if cc := s.LedgerV2Http.GetOauth2ClientCredentials(); cc != nil {
			pterm.Printf("  OAuth2:  client_id=%s endpoint=%s\n", cc.GetClientId(), cc.GetTokenEndpoint())
		}
	case *commonpb.MirrorSourceConfig_LedgerV2Database:
		pterm.Printf("  Type:    ledgerV2Database\n")
		pterm.Printf("  DSN:     %s\n", cmdutil.ObfuscateDSN(s.LedgerV2Database.GetDsn()))

		if iam := s.LedgerV2Database.GetAwsIamAuth(); iam != nil {
			pterm.Printf("  IAM:     AWS RDS IAM auth (region=%s)\n", iam.GetRegion())

			if role := iam.GetAssumeRoleArn(); role != "" {
				pterm.Printf("  Role:    %s\n", role)
			}
		}
	}

	if src.GetBatchSize() > 0 {
		pterm.Printf("  Batch:   %d\n", src.GetBatchSize())
	}

	if rules := src.GetRewriteRules(); len(rules) > 0 {
		pterm.Printf("  Rewrites:\n")

		for i, rule := range rules {
			pterm.Printf("    [%d] %s stop=%t\n", i, describeRewriteRule(rule), rule.GetStop())
		}
	}
}

// describeRewriteRule renders a rule as `<scope> match=<expr> actions=<n>` for
// the CLI display. It never inspects action payloads — that would double the
// output; the scope and action count are enough to eyeball a config.
func describeRewriteRule(rule *commonpb.MirrorRewriteRule) string {
	switch scope := rule.GetScope().(type) {
	case *commonpb.MirrorRewriteRule_CreatedTransaction:
		return fmt.Sprintf("scope=created_transaction match=%q actions=%d", matchOrTrue(scope.CreatedTransaction.GetMatch()), len(scope.CreatedTransaction.GetActions()))
	case *commonpb.MirrorRewriteRule_RevertedTransaction:
		return fmt.Sprintf("scope=reverted_transaction match=%q actions=%d", matchOrTrue(scope.RevertedTransaction.GetMatch()), len(scope.RevertedTransaction.GetActions()))
	case *commonpb.MirrorRewriteRule_SavedMetadata:
		return fmt.Sprintf("scope=saved_metadata match=%q actions=%d", matchOrTrue(scope.SavedMetadata.GetMatch()), len(scope.SavedMetadata.GetActions()))
	case *commonpb.MirrorRewriteRule_DeletedMetadata:
		return fmt.Sprintf("scope=deleted_metadata match=%q actions=%d", matchOrTrue(scope.DeletedMetadata.GetMatch()), len(scope.DeletedMetadata.GetActions()))
	case *commonpb.MirrorRewriteRule_AnyVariant:
		return fmt.Sprintf("scope=any_variant match=%q actions=%d", matchOrTrue(scope.AnyVariant.GetMatch()), len(scope.AnyVariant.GetActions()))
	default:
		return "scope=<unset>"
	}
}

func matchOrTrue(m string) string {
	if m == "" {
		return "true"
	}

	return m
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
		cmd.Flags().Changed("mirror-batch-size") ||
		cmd.Flags().Changed("mirror-rewrite-file") ||
		cmd.Flags().Changed("mirror-rewrite-rule")

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

	rewriteRules, err := parseRewriteRules(cmd)
	if err != nil {
		return 0, nil, err
	}

	cfg := &commonpb.MirrorSourceConfig{
		LedgerName:   sourceLedgerName,
		BatchSize:    batchSize,
		RewriteRules: rewriteRules,
	}

	switch sourceType {
	case "ledgerV2Http", "":
		baseURL, _ := cmd.Flags().GetString("mirror-base-url")
		if baseURL == "" {
			return 0, nil, errors.New("--mirror-base-url is required for ledgerV2Http mirror source")
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

		cfg.Type = &commonpb.MirrorSourceConfig_LedgerV2Http{
			LedgerV2Http: httpCfg,
		}
	case "ledgerV2Database":
		dsn, _ := cmd.Flags().GetString("mirror-dsn")
		if dsn == "" {
			return 0, nil, errors.New("--mirror-dsn is required for ledgerV2Database mirror source")
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

		if cmd.Flags().Changed("mirror-aws-iam-assume-role-arn") {
			assumeRoleArn, _ := cmd.Flags().GetString("mirror-aws-iam-assume-role-arn")
			if assumeRoleArn == "" {
				return 0, nil, errors.New("--mirror-aws-iam-assume-role-arn must be a non-empty ARN when set (got empty value)")
			}
			if pgCfg.GetAwsIamAuth() == nil {
				return 0, nil, errors.New("--mirror-aws-iam-assume-role-arn requires --mirror-aws-iam-region to be set")
			}
			pgCfg.AwsIamAuth.AssumeRoleArn = assumeRoleArn
		}

		cfg.Type = &commonpb.MirrorSourceConfig_LedgerV2Database{
			LedgerV2Database: pgCfg,
		}
	default:
		return 0, nil, fmt.Errorf("invalid mirror source type %q: must be 'ledgerV2Http' or 'ledgerV2Database'", sourceType)
	}

	return commonpb.LedgerMode_LEDGER_MODE_MIRROR, cfg, nil
}

// parseRewriteRules assembles the mirror rewrite rules from
// --mirror-rewrite-file (a YAML/JSON list of MirrorRewriteRule) followed by
// any --mirror-rewrite-rule flags (one YAML/JSON object each). Each rule is
// routed through protojson so proto oneof variants (`scope`, `action`)
// dispatch correctly; the default JSON decoder cannot do that. Rules are
// validated server-side at admission — here we only decode.
func parseRewriteRules(cmd *cobra.Command) ([]*commonpb.MirrorRewriteRule, error) {
	var rules []*commonpb.MirrorRewriteRule

	if path, _ := cmd.Flags().GetString("mirror-rewrite-file"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading --mirror-rewrite-file %q: %w", path, err)
		}

		fileRules, err := decodeRewriteRuleList(data)
		if err != nil {
			return nil, fmt.Errorf("parsing --mirror-rewrite-file %q: %w", path, err)
		}

		rules = append(rules, fileRules...)
	}

	inline, _ := cmd.Flags().GetStringArray("mirror-rewrite-rule")
	for _, entry := range inline {
		rule, err := decodeRewriteRule([]byte(entry))
		if err != nil {
			return nil, fmt.Errorf("parsing --mirror-rewrite-rule %q: %w", entry, err)
		}

		rules = append(rules, rule)
	}

	return rules, nil
}

// decodeRewriteRuleList decodes a YAML/JSON list into MirrorRewriteRule
// protos. YAML is bridged to JSON first because protojson is the only
// decoder that understands the proto oneof dispatch.
func decodeRewriteRuleList(data []byte) ([]*commonpb.MirrorRewriteRule, error) {
	var raw []any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	out := make([]*commonpb.MirrorRewriteRule, 0, len(raw))

	for i, item := range raw {
		jsonBytes, err := yamlToJSON(item)
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", i, err)
		}

		rule := &commonpb.MirrorRewriteRule{}
		if err := protojson.Unmarshal(jsonBytes, rule); err != nil {
			return nil, fmt.Errorf("rule %d: %w", i, err)
		}

		out = append(out, rule)
	}

	return out, nil
}

// decodeRewriteRule decodes a single YAML/JSON rule the same way.
func decodeRewriteRule(data []byte) (*commonpb.MirrorRewriteRule, error) {
	var raw any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	jsonBytes, err := yamlToJSON(raw)
	if err != nil {
		return nil, err
	}

	rule := &commonpb.MirrorRewriteRule{}
	if err := protojson.Unmarshal(jsonBytes, rule); err != nil {
		return nil, err
	}

	return rule, nil
}

// yamlToJSON marshals a Go value produced by yaml.Unmarshal into JSON that
// protojson can then decode. yaml.v3 already produces map[string]any for
// mappings, so encoding/json handles the conversion natively.
func yamlToJSON(v any) ([]byte, error) {
	return stdjson.Marshal(v)
}
