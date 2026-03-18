package ledgers

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewConfigurationExportCommand creates the configuration export subcommand.
func NewConfigurationExportCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export <name>",
		Short: "Export a ledger's editable configuration",
		Long: `Export all editable configuration for a ledger as JSON or YAML.
The output excludes read-only status fields and can be edited then applied back.

Examples:
  ledgerctl ledgers configuration export myledger --yaml > config.yaml
  ledgerctl ledgers configuration export myledger --json > config.json`,
		Args: cobra.ExactArgs(1),
		RunE: runConfigurationExport,
	}

	cmd.Flags().Bool("json", false, "Output as JSON (default if neither --json nor --yaml)")
	cmd.Flags().Bool("yaml", false, "Output as YAML")
	cmd.MarkFlagsMutuallyExclusive("json", "yaml")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runConfigurationExport(cmd *cobra.Command, args []string) error {
	ledgerName := args[0]

	cfg, err := fetchEditableConfig(cmd, ledgerName)
	if err != nil {
		return err
	}

	yamlOutput, _ := cmd.Flags().GetBool("yaml")
	if yamlOutput {
		return cfg.WriteYAML(os.Stdout)
	}

	return cfg.WriteJSON(os.Stdout)
}

// fetchEditableConfig fetches the current configuration from the server and
// converts it to the editable model.
func fetchEditableConfig(cmd *cobra.Command, ledgerName string) (*EditableConfig, error) {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching configuration for %s...", ledgerName))

	ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
	if err != nil {
		spinner.Fail("Failed to get ledger")

		return nil, cmdutil.FormatGRPCError("failed to get ledger", err)
	}

	pqResp, err := client.ListPreparedQueries(ctx, &servicepb.ListPreparedQueriesRequest{Ledger: ledgerName})
	if err != nil {
		spinner.Fail("Failed to list prepared queries")

		return nil, cmdutil.FormatGRPCError("failed to list prepared queries", err)
	}

	nsStream, err := client.ListNumscripts(ctx, &servicepb.ListNumscriptsRequest{Ledger: ledgerName})
	if err != nil {
		spinner.Fail("Failed to list numscripts")

		return nil, cmdutil.FormatGRPCError("failed to list numscripts", err)
	}

	var numscripts []*commonpb.NumscriptInfo
	for {
		info, err := nsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			spinner.Fail("Failed to receive numscripts")

			return nil, cmdutil.FormatGRPCError("failed to receive numscripts", err)
		}
		numscripts = append(numscripts, info)
	}

	_ = spinner.Stop()

	return ConfigFromProto(ledger, pqResp.GetQueries(), numscripts), nil
}
