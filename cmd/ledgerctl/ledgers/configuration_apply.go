package ledgers

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewConfigurationApplyCommand creates the configuration apply subcommand.
func NewConfigurationApplyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "apply <name>",
		Short: "Apply a configuration file to a ledger",
		Long: `Compare a local configuration file (JSON or YAML) against the ledger's
current configuration, compute a diff, and apply the necessary changes.

Examples:
  ledgerctl ledgers configuration apply myledger -f config.yaml
  ledgerctl ledgers configuration apply myledger -f config.json --dry-run
  ledgerctl ledgers configuration apply myledger -f config.yaml --yes`,
		Args:              cobra.ExactArgs(1),
		RunE:              runConfigurationApply,
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().StringP("file", "f", "", "Path to configuration file (JSON or YAML, required)")
	cmd.Flags().Bool("dry-run", false, "Show planned changes without applying")
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	_ = cmd.MarkFlagRequired("file")

	return cmd
}

func runConfigurationApply(cmd *cobra.Command, args []string) error {
	ledgerName := args[0]
	filePath, _ := cmd.Flags().GetString("file")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	yes, _ := cmd.Flags().GetBool("yes")

	// Read desired config from file
	desired, err := ReadConfigFile(filePath)
	if err != nil {
		return err
	}

	// Fetch current config from server
	current, err := fetchEditableConfig(cmd, ledgerName)
	if err != nil {
		return err
	}

	// Compute diff
	actions, err := ComputeDiff(ledgerName, current, desired)
	if err != nil {
		return fmt.Errorf("compute diff: %w", err)
	}

	if len(actions) == 0 {
		pterm.Success.Println("Configuration is already up to date.")

		return nil
	}

	// Display plan
	pterm.Println()
	pterm.DefaultSection.Printf("Changes (%d)\n", len(actions))

	table := pterm.TableData{
		{"", "SECTION", "OPERATION", "DESCRIPTION"},
	}
	for _, a := range actions {
		symbol := actionSymbol(a.Operation)
		table = append(table, []string{symbol, a.Section, a.Operation, a.Description})
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()
	pterm.Println()

	if dryRun {
		pterm.Info.Println("Dry run: no changes applied.")

		return nil
	}

	if !yes {
		confirmed, err := pterm.DefaultInteractiveConfirm.
			WithDefaultText(fmt.Sprintf("Apply %d changes to ledger %q?", len(actions), ledgerName)).
			WithDefaultValue(false).
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		if !confirmed {
			pterm.Info.Println("Apply cancelled.")

			return nil
		}
	}

	// Build Apply request
	requests := make([]*servicepb.Request, len(actions))
	for i, a := range actions {
		requests[i] = a.Request
	}

	envelopes, err := cmdutil.BuildEnvelopes(cmd, requests)
	if err != nil {
		return cmdutil.Displayed(fmt.Errorf("failed to sign requests: %w", err))
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Applying %d changes to %s...", len(actions), ledgerName))

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{Envelopes: envelopes})
	if err != nil {
		spinner.Fail("Failed to apply configuration")

		return cmdutil.FormatGRPCError("failed to apply configuration", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		spinner.Fail("Response signature verification failed")

		return cmdutil.Displayed(fmt.Errorf("response signature verification failed: %w", err))
	}

	spinner.Success(fmt.Sprintf("Applied %d changes to ledger %s", len(actions), ledgerName))

	return nil
}

func actionSymbol(op string) string {
	switch op {
	case "add":
		return pterm.Green("+")
	case "update":
		return pterm.Yellow("~")
	case "remove":
		return pterm.Red("-")
	default:
		return " "
	}
}
