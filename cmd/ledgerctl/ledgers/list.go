package ledgers

import (
	"encoding/json"
	"os"
	"sort"
	"time"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewListCommand creates the ledgers list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "l"},
		Short:   "List all ledgers",
		Long:    "List all ledgers in the cluster via gRPC",
		RunE:    runList,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runList(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Fetching ledgers...")

	ledgers, err := cmdutil.GetAllLedgersInfo(ctx, client)
	if err != nil {
		_ = spinner.Stop()
		return cmdutil.FormatGRPCError("failed to list ledgers", err)
	}

	_ = spinner.Stop()

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(ledgers)
	}

	names := make([]string, 0, len(ledgers))
	for name := range ledgers {
		names = append(names, name)
	}
	sort.Strings(names)

	if len(names) == 0 {
		pterm.Info.Println("No ledgers found.")
		pterm.Println(pterm.Gray("Create one with: ledgerctl ledgers create --name <name>"))
		return nil
	}

	tableData := pterm.TableData{
		{"NAME", "CREATED AT"},
	}

	for _, name := range names {
		ledger := ledgers[name]
		createdAt := "-"
		if ledger.CreatedAt != nil {
			createdAt = ledger.CreatedAt.AsTime().Format(time.RFC3339)
		}
		tableData = append(tableData, []string{
			ledger.Name,
			createdAt,
		})
	}

	pterm.Println()
	return pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}
