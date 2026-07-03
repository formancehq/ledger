package ledgers

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewCreateCommand creates the ledgers create command.
func NewCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "create",
		Aliases:           []string{"new", "add"},
		Short:             "Create a new ledger",
		Long:              "Create a new ledger via gRPC.\n\nTo create a mirror ledger, use --mode=mirror with source configuration flags.",
		Args:              cobra.NoArgs,
		RunE:              runCreate,
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("name", "", "Name of the ledger to create")
	cmd.Flags().StringArray("schema", nil, "Metadata schema entries in target:key:type format (can be repeated, e.g. account:age:int64)")
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

	return cmd
}

func runCreate(cmd *cobra.Command, _ []string) error {
	name, _ := cmd.Flags().GetString("name")

	if name == "" {
		if !term.IsTerminal(int(os.Stdin.Fd())) {
			return errors.New("ledger name is required (use --name flag)")
		}

		result, err := pterm.DefaultInteractiveTextInput.
			WithDefaultText("Enter ledger name").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		name = result
		if name == "" {
			pterm.Error.Println("Ledger name is required")

			return cmdutil.Displayed(errors.New("ledger name is required"))
		}
	}

	schemaEntries, _ := cmd.Flags().GetStringArray("schema")

	initialSchema, err := parseSchemaEntries(cmd, schemaEntries)
	if err != nil {
		return err
	}

	// Parse mirror mode
	mode, mirrorSource, err := parseMirrorFlags(cmd, name)
	if err != nil {
		return err
	}

	// Parse default enforcement mode
	var defaultEnforcementMode commonpb.ChartEnforcementMode
	if enforcementStr, _ := cmd.Flags().GetString("default-enforcement-mode"); enforcementStr != "" {
		defaultEnforcementMode, err = parseEnforcementModeProtoStrict(enforcementStr)
		if err != nil {
			return err
		}
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	modeStr := "normal"
	if mode == commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		modeStr = "mirror"
	}

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Creating %s ledger %s...", modeStr, name))

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

	applyReq, err := cmdutil.BuildApplyRequest(cmd, requests...)
	if err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	resp, err := client.Apply(ctx, applyReq)
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to create ledger", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		spinner.Fail("Response signature verification failed")

		return cmdutil.Displayed(fmt.Errorf("response signature verification failed: %w", err))
	}

	if len(resp.GetLogs()) == 0 {
		spinner.Fail("No response received")

		return cmdutil.Displayed(errors.New("no response received"))
	}

	log := resp.GetLogs()[0]

	createLedgerLog := log.GetPayload().GetCreateLedger()
	if createLedgerLog == nil {
		spinner.Fail("Unexpected response type")

		return cmdutil.Displayed(errors.New("unexpected response type"))
	}

	ledger := createLedgerLog.ToLedgerInfo()

	spinner.Success("Created")

	if handled, err := cmdutil.EncodeStructured(cmd, ledger); handled || err != nil {
		return err
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
	if len(schema) == 0 && !cmd.Flags().Changed("schema") && term.IsTerminal(int(os.Stdin.Fd())) {
		wizardSchema, err := schemaWizard()
		if err != nil {
			return nil, err
		}

		schema = wizardSchema
	}

	return schema, nil
}

func schemaWizard() ([]*commonpb.SetMetadataFieldTypeCommand, error) {
	addSchema, err := pterm.DefaultInteractiveConfirm.
		WithDefaultText("Add metadata schema?").
		WithDefaultValue(false).
		Show()
	if err != nil {
		return nil, fmt.Errorf("failed to read input: %w", err)
	}

	if !addSchema {
		return nil, nil
	}

	var schema []*commonpb.SetMetadataFieldTypeCommand

	for {
		targetStr, err := pterm.DefaultInteractiveSelect.
			WithOptions(cmdutil.TargetTypeOptions()).
			WithDefaultText("Select target type").
			Show()
		if err != nil {
			return nil, fmt.Errorf("failed to read input: %w", err)
		}

		target, err := cmdutil.ParseTargetType(targetStr)
		if err != nil {
			return nil, err
		}

		key, err := pterm.DefaultInteractiveTextInput.
			WithDefaultText("Enter metadata key name").
			Show()
		if err != nil {
			return nil, fmt.Errorf("failed to read input: %w", err)
		}

		if key == "" {
			pterm.Warning.Println("Key cannot be empty, skipping.")

			continue
		}

		typeStr, err := pterm.DefaultInteractiveSelect.
			WithOptions(cmdutil.MetadataTypeOptions()).
			WithDefaultText("Select metadata type").
			Show()
		if err != nil {
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

		another, err := pterm.DefaultInteractiveConfirm.
			WithDefaultText("Add another field?").
			WithDefaultValue(false).
			Show()
		if err != nil {
			return nil, fmt.Errorf("failed to read input: %w", err)
		}

		if !another {
			break
		}
	}

	return schema, nil
}
