package ledgers

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/indexes"
	domainindexes "github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewCreateCommand creates the ledgers create command.
func NewCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "create",
		Aliases: []string{"new", "add"},
		Short:   "Create a new ledger",
		Long:    "Create a new ledger via gRPC.\n\nTo create a mirror ledger, use --mode=mirror with source configuration flags.",
		Args:    cobra.NoArgs,

		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("name", "", "Name of the ledger to create")
	cmd.Flags().StringArray("schema", nil, "Metadata schema entries in target:key:type format (can be repeated, e.g. account:age:int64)")
	cmd.Flags().StringArray("index", nil, "Initial index: a builtin type (e.g. reference) or metadata:<account|transaction>:<key> (repeatable; atomic with ledger creation)")
	cmd.Flags().String("idempotency-key", "", "Idempotency key for the entire ledger creation batch")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	// Account type enforcement
	cmd.Flags().String("default-enforcement-mode", "", "Default enforcement mode for unmatched accounts: STRICT or AUDIT (default: STRICT)")
	cmdutil.RegisterEnumCompletion(cmd, "default-enforcement-mode", "STRICT", "AUDIT")

	// Mirror mode flags
	cmd.Flags().String("mode", "normal", "Ledger mode: normal or mirror")
	cmdutil.RegisterEnumCompletion(cmd, "mode", "normal", "mirror")
	cmd.Flags().String("mirror-source-type", "http", "Mirror source type: http or postgres")
	cmdutil.RegisterEnumCompletion(cmd, "mirror-source-type", "http", "postgres")
	cmd.Flags().String("mirror-ledger-name", "", "Source ledger name in the v2 system (defaults to ledger name)")
	cmd.Flags().String("mirror-base-url", "", "Base URL of the v2 API (for http source)")
	cmd.Flags().String("mirror-oauth2-client-id", "", "OAuth2 client ID for the v2 API (for http source)")
	cmd.Flags().String("mirror-oauth2-client-secret", "", "OAuth2 client secret for the v2 API (for http source)")
	cmd.Flags().String("mirror-oauth2-token-endpoint", "", "OAuth2 token endpoint URL (for http source)")
	cmd.Flags().StringArray("mirror-oauth2-scopes", nil, "OAuth2 scopes (for http source, can be repeated)")
	cmd.Flags().String("mirror-dsn", "", "PostgreSQL DSN (for postgres source)")
	cmd.Flags().String("mirror-aws-iam-region", "", "Enable AWS RDS IAM authentication using the given region (for postgres source); credentials are taken from the ambient AWS chain (IRSA, instance profile, env)")
	cmd.Flags().String("mirror-aws-iam-assume-role-arn", "", "Optional STS role ARN to assume before minting the RDS IAM token (cross-account / multi-tenant mirrors); requires --mirror-aws-iam-region")
	cmd.Flags().Uint32("mirror-batch-size", 0, "Max logs per batch (0 = default 100)")
	cmd.Flags().String("mirror-rewrite-file", "", "Path to a YAML/JSON list of MirrorRewriteRule objects applied to every mirror log entry during translation. Each rule sets exactly one scope (createdTransaction | revertedTransaction | savedMetadata | deletedMetadata | anyVariant), an optional CEL `match` predicate, and typed `actions` (rewriteAddress, setMetadata, drop, ...). See docs/technical/architecture/subsystems/events-mirror/cel-rewrite.md")
	cmd.Flags().StringArray("mirror-rewrite-rule", nil, "A single MirrorRewriteRule as a YAML/JSON object, e.g. {\"anyVariant\":{\"actions\":[{\"rewriteAddress\":{\"pattern\":\":worker:\\\\d+\",\"replacement\":\"\"}}]}} (repeatable; appended after any --mirror-rewrite-file rules)")

	return cmd
}

// PrepareCreate builds one atomic ledger creation proposal, including initial indexes, without making RPCs.
func PrepareCreate(cmd *cobra.Command) ([]*servicepb.Request, error) {
	ctx := cmdutil.CommandContextOrBackground(cmd)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	name, err := cmd.Flags().GetString("name")
	if err != nil {
		return nil, err
	}

	if name == "" {
		if !createInputIsTerminal(cmd) {
			return nil, errors.New("ledger name is required (use --name flag)")
		}

		var result string
		if err := askCreate(cmd, &survey.Input{Message: "Enter ledger name"}, &result); err != nil {
			return nil, fmt.Errorf("failed to read input: %w", err)
		}

		name = result
		if name == "" {
			return nil, errors.New("ledger name is required")
		}
	}

	schemaEntries, err := cmd.Flags().GetStringArray("schema")
	if err != nil {
		return nil, err
	}

	initialSchema, err := parseSchemaEntries(cmd, schemaEntries)
	if err != nil {
		return nil, err
	}

	indexEntries, err := cmd.Flags().GetStringArray("index")
	if err != nil {
		return nil, err
	}
	initialIndexes, err := parseInitialIndexes(indexEntries)
	if err != nil {
		return nil, err
	}

	// Parse mirror mode
	mode, mirrorSource, err := parseMirrorFlags(cmd, name)
	if err != nil {
		return nil, err
	}

	// Parse default enforcement mode
	var defaultEnforcementMode commonpb.ChartEnforcementMode
	enforcementStr, err := cmd.Flags().GetString("default-enforcement-mode")
	if err != nil {
		return nil, err
	}
	if enforcementStr != "" {
		defaultEnforcementMode, err = parseEnforcementModeProtoStrict(enforcementStr)
		if err != nil {
			return nil, err
		}
	}

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{
					Name:                   name,
					InitialSchema:          initialSchema,
					Mode:                   mode,
					MirrorSource:           mirrorSource,
					DefaultEnforcementMode: defaultEnforcementMode,
				},
			},
		},
	}

	// Keep index declarations in the creation proposal so the mirror worker
	// cannot commit history before its initial query indexes exist (EN-2070).
	for _, id := range initialIndexes {
		requests = append(requests, &servicepb.Request{Type: &servicepb.Request_CreateIndex{
			CreateIndex: &servicepb.CreateIndexRequest{Ledger: name, Id: id},
		}})
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return requests, nil
}

// RenderCreate validates and renders the native create response without starting a spinner.
func RenderCreate(cmd *cobra.Command, resp *servicepb.ApplyResponse) error {
	return renderCreate(cmd, resp, nil)
}

func renderCreate(cmd *cobra.Command, resp *servicepb.ApplyResponse, spinner *cmdutil.Spinner) error {
	if cmdutil.IsStructuredOutput(cmd) && spinner != nil {
		_ = spinner.Stop() // Structured output must not emit spinner prefixes.
		spinner = nil
	}

	fail := func(message string, err error) error {
		if spinner == nil {
			return err
		}
		spinner.Fail(message)

		return cmdutil.Displayed(err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		return fail("Response signature verification failed", fmt.Errorf("response signature verification failed: %w", err))
	}

	if len(resp.GetLogs()) == 0 {
		return fail("No response received", errors.New("no response received"))
	}

	log := resp.GetLogs()[0]

	createLedgerLog := log.GetPayload().GetCreateLedger()
	if createLedgerLog == nil {
		return fail("Unexpected response type", errors.New("unexpected response type"))
	}

	ledger := createLedgerLog.ToLedgerInfo()
	ledger.Metadata = createLedgerLog.GetMetadata()

	if handled, err := cmdutil.EncodeStructured(cmd, ledger); handled || err != nil {
		return err
	}

	if spinner != nil {
		spinner.Success("Created")
	}

	pterm.Println()

	pterm.Printf("Ledger: %s\n", pterm.Cyan(ledger.GetName()))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	pterm.Printf("Name:       %s\n", ledger.GetName())

	createdAt := "-"
	if ledger.GetCreatedAt() != nil {
		createdAt = ledger.GetCreatedAt().AsTime().Format(time.RFC3339)
	}

	pterm.Printf("Created At: %s\n", createdAt)
	pterm.Printf("Mode:       %s\n", ledgerModeString(ledger.GetMode()))

	if ledger.GetMirrorSource() != nil {
		renderMirrorSource(ledger.GetMirrorSource())
	}

	if ledger.GetMetadataSchema() != nil {
		renderLedgerSchema(ledger.GetMetadataSchema())
	}

	// Render any CreateIndex logs that followed the CreateLedger log in the
	// atomic batch (see EN-2070). Each outer Log wraps an ApplyLedgerLog; the
	// inner LedgerLog carries the CreateIndex payload.
	var indexLines []string
	for _, lg := range resp.GetLogs()[1:] {
		applyLog := lg.GetPayload().GetApply()
		if applyLog == nil {
			continue
		}
		createdIdx := applyLog.GetLog().GetData().GetCreateIndex()
		if createdIdx == nil {
			continue
		}
		typeName, target, key := describeIndex(createdIdx.GetId())
		if target != "-" && key != "-" {
			indexLines = append(indexLines, fmt.Sprintf("  %s (%s.%s)", typeName, target, key))
		} else {
			indexLines = append(indexLines, "  "+typeName)
		}
	}
	if len(indexLines) > 0 {
		pterm.Printf("Indexes:\n")
		for _, line := range indexLines {
			pterm.Println(line)
		}
	}

	return nil
}

func parseSchemaEntries(cmd *cobra.Command, entries []string) ([]*commonpb.SetMetadataFieldTypeCommand, error) {
	var schema []*commonpb.SetMetadataFieldTypeCommand

	for _, entry := range entries {
		target, key, mdType, err := cmdutil.ParseSchemaEntry(entry)
		if err != nil {
			return nil, err
		}

		schema = append(schema, &commonpb.SetMetadataFieldTypeCommand{
			TargetType: target,
			Key:        key,
			Type:       mdType,
		})
	}

	// If no schema entries from flags, offer wizard mode (only in interactive terminals)
	if len(schema) == 0 && !cmd.Flags().Changed("schema") && createInputIsTerminal(cmd) {
		wizardSchema, err := schemaWizard(cmd)
		if err != nil {
			return nil, err
		}

		schema = wizardSchema
	}

	return schema, nil
}

func schemaWizard(cmd *cobra.Command) ([]*commonpb.SetMetadataFieldTypeCommand, error) {
	var addSchema bool
	if err := askCreate(cmd, &survey.Confirm{Message: "Add metadata schema?", Default: false}, &addSchema); err != nil {
		return nil, fmt.Errorf("failed to read input: %w", err)
	}

	if !addSchema {
		return nil, nil
	}

	var schema []*commonpb.SetMetadataFieldTypeCommand

	for {
		var targetStr string
		if err := askCreate(cmd, &survey.Select{Message: "Select target type", Options: cmdutil.TargetTypeOptions()}, &targetStr); err != nil {
			return nil, fmt.Errorf("failed to read input: %w", err)
		}

		target, err := cmdutil.ParseTargetType(targetStr)
		if err != nil {
			return nil, err
		}

		var key string
		if err := askCreate(cmd, &survey.Input{Message: "Enter metadata key name"}, &key); err != nil {
			return nil, fmt.Errorf("failed to read input: %w", err)
		}

		if key == "" {
			pterm.Warning.WithWriter(cmd.ErrOrStderr()).Println("Key cannot be empty, skipping.")

			continue
		}

		var typeStr string
		if err := askCreate(cmd, &survey.Select{Message: "Select metadata type", Options: cmdutil.MetadataTypeOptions()}, &typeStr); err != nil {
			return nil, fmt.Errorf("failed to read input: %w", err)
		}

		mdType, err := cmdutil.ParseMetadataType(typeStr)
		if err != nil {
			return nil, err
		}

		schema = append(schema, &commonpb.SetMetadataFieldTypeCommand{
			TargetType: target,
			Key:        key,
			Type:       mdType,
		})

		var another bool
		if err := askCreate(cmd, &survey.Confirm{Message: "Add another field?", Default: false}, &another); err != nil {
			return nil, fmt.Errorf("failed to read input: %w", err)
		}

		if !another {
			break
		}
	}

	return schema, nil
}

func parseInitialIndexes(entries []string) ([]*commonpb.IndexID, error) {
	var ids []*commonpb.IndexID
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		id, err := indexes.ParseDefinition(entry)
		if err != nil {
			return nil, err
		}
		canonical := domainindexes.Canonical(id)
		if _, duplicate := seen[canonical]; duplicate {
			return nil, fmt.Errorf("duplicate initial index %q", entry)
		}
		seen[canonical] = struct{}{}
		ids = append(ids, id)
	}

	return ids, nil
}

func askCreate(cmd *cobra.Command, prompt survey.Prompt, response any) error {
	ctx := cmdutil.CommandContextOrBackground(cmd)
	if err := ctx.Err(); err != nil {
		return err
	}

	input, ok := cmd.InOrStdin().(*os.File)
	if !ok {
		return errors.New("interactive input requires a terminal")
	}

	return cmdutil.AskOneContext(ctx, prompt, response, input, createPromptOutput{Writer: cmd.ErrOrStderr()}, cmd.ErrOrStderr())
}

func createInputIsTerminal(cmd *cobra.Command) bool {
	input, ok := cmd.InOrStdin().(*os.File)

	return ok && term.IsTerminal(int(input.Fd()))
}

// createPromptOutput supplies Survey with stderr's terminal descriptor while
// sending prompt bytes through the command's scoped error writer.
type createPromptOutput struct{ io.Writer }

func (createPromptOutput) Fd() uintptr { return os.Stderr.Fd() }
