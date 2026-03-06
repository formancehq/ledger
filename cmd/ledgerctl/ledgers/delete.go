package ledgers

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewDeleteCommand creates the ledgers delete command.
func NewDeleteCommand() *cobra.Command {
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
		RunE: runDelete,
	}

	cmd.Flags().String("name", "", "Name of the ledger to delete")
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runDelete(cmd *cobra.Command, args []string) error {
	name, _ := cmd.Flags().GetString("name")
	if len(args) > 0 {
		name = args[0]
	}

	if name == "" {
		client, conn, err := cmdutil.GetClient(cmd)
		if err != nil {
			return err
		}

		defer func() { _ = conn.Close() }()

		selectedName, err := cmdutil.SelectLedger(cmd, client, "")
		if err != nil {
			return err
		}

		name = selectedName
	}

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

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Deleting ledger %s...", name))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_DeleteLedger{
				DeleteLedger: &servicepb.DeleteLedgerRequest{
					Name: name,
				},
			},
		},
	}

	if err := cmdutil.SignRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to delete ledger", err)
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

	deleteLedgerLog := log.GetPayload().GetDeleteLedger()
	if deleteLedgerLog == nil {
		spinner.Fail("Unexpected response type")

		return cmdutil.Displayed(errors.New("unexpected response type"))
	}

	ledger := deleteLedgerLog.GetInfo()

	spinner.Success("Deleted")

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")

		return encoder.Encode(ledger)
	}

	pterm.Println()

	pterm.Printf("Ledger: %s (deleted)\n", pterm.Cyan(ledger.GetName()))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	pterm.Printf("Name:       %s\n", pterm.Gray(ledger.GetName()))

	createdAt := "-"
	if ledger.GetCreatedAt() != nil {
		createdAt = ledger.GetCreatedAt().AsTime().Format(time.RFC3339)
	}

	pterm.Printf("Created At: %s\n", createdAt)

	deletedAt := "-"
	if ledger.GetDeletedAt() != nil {
		deletedAt = ledger.GetDeletedAt().AsTime().Format(time.RFC3339)
	}

	pterm.Printf("Deleted At: %s\n", deletedAt)

	return nil
}
