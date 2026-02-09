package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newLedgersDeleteCommand creates the ledgers delete command.
func newLedgersDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "delete [name]",
		Aliases: []string{"rm", "del", "remove"},
		Short:   "Delete a ledger",
		Long: `Delete a ledger via gRPC.

The ledger will be soft-deleted (marked as deleted but data retained).

Examples:
  ledgerctl ledgers delete my-ledger
  ledgerctl ledgers delete --name my-ledger
  ledgerctl ledgers delete  # Interactive mode`,
		Args: cobra.MaximumNArgs(1),
		RunE: runLedgersDelete,
	}

	cmd.Flags().String("name", "", "Name of the ledger to delete")
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runLedgersDelete(cmd *cobra.Command, args []string) error {
	// Get ledger name from args, flag, or interactive prompt
	name, _ := cmd.Flags().GetString("name")
	if len(args) > 0 {
		name = args[0]
	}

	if name == "" {
		client, conn, err := getClient(cmd)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()

		// Use selectLedger for interactive selection
		selectedName, err := selectLedger(cmd, client, "")
		if err != nil {
			return err
		}
		name = selectedName
	}

	// Confirmation prompt unless --yes is used
	yes, _ := cmd.Flags().GetBool("yes")
	if !yes {
		pterm.Warning.Printfln("You are about to delete ledger %s", name)
		confirmed, err := pterm.DefaultInteractiveConfirm.
			WithDefaultText(fmt.Sprintf("Delete ledger '%s'?", name)).
			WithDefaultValue(false).
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}
		if !confirmed {
			pterm.Info.Println("Deletion cancelled.")
			return nil
		}
	}

	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Deleting ledger %s...", name))

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{
			{
				Type: &servicepb.Request_DeleteLedger{
					DeleteLedger: &servicepb.DeleteLedgerRequest{
						Name: name,
					},
				},
			},
		},
	})
	if err != nil {
		spinner.Fail("Failed to delete ledger")
		return fmt.Errorf("failed to delete ledger: %w", err)
	}

	// Extract the deleted ledger info from the response
	if len(resp.Logs) == 0 {
		spinner.Fail("No response received")
		return fmt.Errorf("no response received")
	}

	log := resp.Logs[0]
	deleteLedgerLog := log.Payload.GetDeleteLedger()
	if deleteLedgerLog == nil {
		spinner.Fail("Unexpected response type")
		return fmt.Errorf("unexpected response type")
	}

	ledger := deleteLedgerLog.Info

	spinner.Success("Deleted")

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(ledger)
	}

	pterm.Println()

	// Display deleted ledger details
	pterm.Printf("Ledger: %s (deleted)\n", ledger.Name)
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	pterm.Printf("ID:         %d\n", ledger.Id)
	pterm.Printf("Name:       %s\n", pterm.Gray(ledger.Name))
	createdAt := "-"
	if ledger.CreatedAt != nil {
		createdAt = ledger.CreatedAt.AsTime().Format(time.RFC3339)
	}
	pterm.Printf("Created At: %s\n", createdAt)
	deletedAt := "-"
	if ledger.DeletedAt != nil {
		deletedAt = ledger.DeletedAt.AsTime().Format(time.RFC3339)
	}
	pterm.Printf("Deleted At: %s\n", deletedAt)

	return nil
}
