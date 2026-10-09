package ledgers

import (
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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

		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("name", "", "Name of the ledger to delete")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

// RenderDelete writes the deletion log after the mutation has completed.
func RenderDelete(cmd *cobra.Command, deleteLedgerLog *commonpb.DeletedLedgerLog) error {
	if handled, err := cmdutil.EncodeStructured(cmd, deleteLedgerLog); handled || err != nil {
		return err
	}

	pterm.Println()

	pterm.Printf("Ledger: %s (deleted)\n", pterm.Cyan(deleteLedgerLog.GetName()))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	pterm.Printf("Name:       %s\n", pterm.Gray(deleteLedgerLog.GetName()))

	deletedAt := "-"
	if deleteLedgerLog.GetDeletedAt() != nil {
		deletedAt = deleteLedgerLog.GetDeletedAt().AsTime().Format(time.RFC3339)
	}

	pterm.Printf("Deleted At: %s\n", deletedAt)

	return nil
}
