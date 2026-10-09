package transactions

import (
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// NewRevertCommand creates the transactions revert command.
func NewRevertCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "revert [transaction-id]",
		Aliases: []string{"undo", "reverse"},
		Short:   "Revert a transaction",
		Long: `Revert a transaction by creating a counter-transaction.

The revert operation creates a new transaction that reverses all postings
from the original transaction.

Flags:
  --force            Force the revert even if funds have already been spent
  --at-effective-date  Use the original transaction timestamp for the revert
  -y, --yes          Skip confirmation prompt

If --ledger is not provided and only one ledger exists, it will be used automatically.
If multiple ledgers exist, you will be prompted to select one.

Examples:
  ledgerctl transactions revert 42 --ledger my-ledger
  ledgerctl transactions revert 42 --force
  ledgerctl transactions revert 42 --at-effective-date
  ledgerctl transactions revert 42 -y  # Skip confirmation
  ledgerctl tx revert 42 --metadata key1=value1 --metadata key2=value2`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Bool("force", false, "Force revert even if funds have been spent")
	cmd.Flags().Bool("at-effective-date", false, "Use original transaction timestamp for the revert")
	cmd.Flags().StringArray("metadata", nil, "Metadata for the revert transaction (key=value)")
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

// RenderRevert writes an already committed revert payload. It does not submit
// a mutation or verify signatures; the executor must do both before calling it.
func RenderRevert(cmd *cobra.Command, revertedTx *commonpb.RevertedTransaction) error {
	text := pterm.DefaultBasicText.WithWriter(cmd.OutOrStdout())

	if handled, err := cmdutil.EncodeStructured(cmd, revertedTx); handled || err != nil {
		return err
	}

	text.Println()

	// Display revert info
	text.Printf("Revert Transaction #%d\n", revertedTx.GetRevertTransaction().GetId())
	text.Println(pterm.Gray("─────────────────────────────────"))
	text.Printf("Original Transaction: #%d\n", revertedTx.GetRevertedTransactionId())

	if revertedTx.GetRevertTransaction().GetTimestamp() != nil {
		text.Printf("Timestamp:            %s\n", pterm.Gray(revertedTx.GetRevertTransaction().GetTimestamp().AsTime().Format("2006-01-02T15:04:05Z07:00")))
	}

	rescale := cmdutil.RescaleTarget(cmd)

	// Display postings of the revert transaction
	if len(revertedTx.GetRevertTransaction().GetPostings()) > 0 {
		text.Println()
		text.Println("Revert Postings:")

		postingsTable := pterm.TableData{
			{"#", "SOURCE", "", "DESTINATION", "AMOUNT", "ASSET", "COLOR"},
		}

		for i, posting := range revertedTx.GetRevertTransaction().GetPostings() {
			amount, asset := posting.GetAmount().Dec(), posting.GetAsset()
			if rescale != nil {
				var err error

				amount, asset, err = cmdutil.Rescale(amount, asset, *rescale)
				if err != nil {
					return err
				}
			}

			color := posting.GetColor()
			if color == "" {
				color = "-"
			}

			postingsTable = append(postingsTable, []string{
				strconv.Itoa(i + 1),
				posting.GetSource(),
				"→",
				posting.GetDestination(),
				amount,
				asset,
				color,
			})
		}

		err := pterm.DefaultTable.WithWriter(cmd.OutOrStdout()).WithHasHeader().WithData(postingsTable).Render()
		if err != nil {
			return err
		}
	}

	// Display post-commit volumes (carried on the revert transaction)
	if pcv := revertedTx.GetRevertTransaction().GetPostCommitVolumes(); pcv != nil {
		err := renderPostCommitVolumes(cmd.OutOrStdout(), pcv, rescale)
		if err != nil {
			return err
		}
	}

	return nil
}
