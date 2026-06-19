package transactions

import (
	"fmt"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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
		RunE:              runRevert,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Bool("force", false, "Force revert even if funds have been spent")
	cmd.Flags().Bool("at-effective-date", false, "Use original transaction timestamp for the revert")
	cmd.Flags().StringArray("metadata", nil, "Metadata for the revert transaction (key=value)")
	cmd.Flags().String("receipt", "", "JWT receipt for the transaction (avoids server-side lookup)")
	cmd.Flags().Bool("expand-volumes", false, "Include post-commit volumes in response")
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runRevert(cmd *cobra.Command, args []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	// Get ledger name (from flag or interactive selection)
	ledgerFlag, _ := cmd.Flags().GetString("ledger")

	ledgerName, err := cmdutil.SelectLedger(cmd, client, ledgerFlag)
	if err != nil {
		return err
	}

	// Get transaction ID (from args or prompt)
	var txID uint64
	if len(args) > 0 {
		txID, err = strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			pterm.Error.Printfln("Invalid transaction ID: %v", err)

			return cmdutil.Displayed(fmt.Errorf("invalid transaction ID: %w", err))
		}
	} else {
		input, err := pterm.DefaultInteractiveTextInput.
			WithDefaultText("Enter transaction ID to revert").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		txID, err = strconv.ParseUint(input, 10, 64)
		if err != nil {
			pterm.Error.Printfln("Invalid transaction ID: %v", err)

			return cmdutil.Displayed(fmt.Errorf("invalid transaction ID: %w", err))
		}
	}

	// Get flags
	force, _ := cmd.Flags().GetBool("force")
	atEffectiveDate, _ := cmd.Flags().GetBool("at-effective-date")
	receiptFlag, _ := cmd.Flags().GetString("receipt")
	expandVolumes, _ := cmd.Flags().GetBool("expand-volumes")
	metadataFlags, _ := cmd.Flags().GetStringArray("metadata")

	// Parse metadata
	metadata := make(map[string]string)

	for _, m := range metadataFlags {
		key, value, err := cmdutil.ParseKeyValue(m)
		if err != nil {
			pterm.Error.Printfln("Invalid metadata format: %s", m)

			return cmdutil.Displayed(fmt.Errorf("invalid metadata format %q: %w", m, err))
		}

		metadata[key] = value
	}

	// Confirmation prompt (unless --yes flag is set)
	yes, _ := cmd.Flags().GetBool("yes")
	if !yes {
		pterm.Println()
		pterm.Warning.Printfln("You are about to revert transaction #%d", txID)
		pterm.Println(pterm.Gray("This will create a new transaction reversing all postings."))
		pterm.Println()

		confirmed, err := pterm.DefaultInteractiveConfirm.
			WithDefaultText("Are you sure you want to revert this transaction?").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read confirmation: %w", err)
		}

		if !confirmed {
			pterm.Info.Println("Revert cancelled")

			return nil
		}
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Reverting transaction #%d...", txID))

	// Build revert request
	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledgerName,
					Action: &servicepb.LedgerAction{
						Data: &servicepb.LedgerAction_RevertTransaction{
							RevertTransaction: &servicepb.RevertTransactionPayload{
								TransactionId:   txID,
								Force:           force,
								AtEffectiveDate: atEffectiveDate,
								Metadata:        commonpb.MetadataFromGoMap(metadata),
								Receipt:         receiptFlag,
								ExpandVolumes:   expandVolumes,
							},
						},
					},
				},
			},
		},
	}

	envelopes, err := cmdutil.BuildEnvelopes(cmd, requests)
	if err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{Envelopes: envelopes})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to revert transaction", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		spinner.Fail("Response signature verification failed")

		return cmdutil.Displayed(fmt.Errorf("response signature verification failed: %w", err))
	}

	spinner.Success("Reverted")

	if len(resp.GetLogs()) == 0 {
		pterm.Warning.Println("No logs returned")

		return nil
	}

	// Get the revert transaction from response
	log := resp.GetLogs()[0]

	applyLog := log.GetPayload().GetApply()
	if applyLog == nil {
		pterm.Warning.Println("Unexpected response format")

		return nil
	}

	revertedTx := applyLog.GetLog().GetData().GetRevertedTransaction()
	if revertedTx == nil {
		pterm.Warning.Println("No reverted transaction in response")

		return nil
	}

	if handled, err := cmdutil.EncodeStructured(cmd, revertedTx); handled || err != nil {
		return err
	}

	pterm.Println()

	// Display revert info
	pterm.Printf("Revert Transaction #%d\n", revertedTx.GetRevertTransaction().GetId())
	pterm.Println(pterm.Gray("─────────────────────────────────"))
	pterm.Printf("Original Transaction: #%d\n", txID)

	if revertedTx.GetRevertTransaction().GetTimestamp() != nil {
		pterm.Printf("Timestamp:            %s\n", pterm.Gray(revertedTx.GetRevertTransaction().GetTimestamp().AsTime().Format("2006-01-02T15:04:05Z07:00")))
	}

	// Display postings of the revert transaction
	if len(revertedTx.GetRevertTransaction().GetPostings()) > 0 {
		pterm.Println()
		pterm.Println("Revert Postings:")

		postingsTable := pterm.TableData{
			{"#", "SOURCE", "", "DESTINATION", "AMOUNT", "ASSET"},
		}

		for i, posting := range revertedTx.GetRevertTransaction().GetPostings() {
			postingsTable = append(postingsTable, []string{
				strconv.Itoa(i + 1),
				posting.GetSource(),
				"→",
				posting.GetDestination(),
				posting.GetAmount().Dec(),
				posting.GetAsset(),
			})
		}

		err := pterm.DefaultTable.WithHasHeader().WithData(postingsTable).Render()
		if err != nil {
			return err
		}
	}

	// Display post-commit volumes
	if revertedTx.GetPostCommitVolumes() != nil {
		err := renderPostCommitVolumes(revertedTx.GetPostCommitVolumes())
		if err != nil {
			return err
		}
	}

	return nil
}
