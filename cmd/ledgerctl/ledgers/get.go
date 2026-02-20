package ledgers

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewGetCommand creates the ledgers get command.
func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get <name>",
		Aliases: []string{"g", "show", "describe"},
		Short:   "Get a ledger by name",
		Long:    "Get detailed information about a ledger by its name via gRPC",
		Args:    cobra.ExactArgs(1),
		RunE:    runGet,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runGet(cmd *cobra.Command, args []string) error {
	ledgerName := args[0]

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching ledger %s...", ledgerName))

	ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{
		Ledger: ledgerName,
	})
	if err != nil {
		spinner.Fail("Failed to get ledger")
		return cmdutil.FormatGRPCError("failed to get ledger", err)
	}

	_ = spinner.Stop()

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(ledger)
	}

	pterm.Println()

	pterm.Printf("Ledger: %s\n", ledger.Name)
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	pterm.Printf("ID:         %d\n", ledger.Id)
	pterm.Printf("Name:       %s\n", ledger.Name)
	createdAt := "-"
	if ledger.CreatedAt != nil {
		createdAt = ledger.CreatedAt.AsTime().Format(time.RFC3339)
	}
	pterm.Printf("Created At: %s\n", createdAt)

	return nil
}
