package ledger

import (
	"strconv"

	"github.com/formancehq/fctl/cmd/ledger/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/spf13/cobra"
)

func NewSendCommand() *cobra.Command {
	const (
		metadataFlag  = "metadata"
		referenceFlag = "reference"
	)
	return fctl.NewCommand("send [<source>] <destination> <amount> <asset>",
		fctl.WithAliases("s", "se"),
		fctl.WithShortDescription("Send from one account to another"),
		fctl.WithConfirmFlag(),
		fctl.WithArgs(cobra.RangeArgs(3, 4)),
		fctl.WithStringSliceFlag(metadataFlag, []string{""}, "Metadata to use"),
		fctl.WithStringFlag(referenceFlag, "", "Reference to add to the generated transaction"),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}

			organizationID, err := fctl.ResolveOrganizationID(cmd, cfg)
			if err != nil {
				return err
			}

			stack, err := fctl.ResolveStack(cmd, cfg, organizationID)
			if err != nil {
				return err
			}

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to create a new transaction") {
				return fctl.ErrMissingApproval
			}

			ledgerClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			var source, destination, asset, amountStr string
			if len(args) == 3 {
				source = "world"
				destination = args[0]
				amountStr = args[1]
				asset = args[2]
			} else {
				source = args[0]
				destination = args[1]
				amountStr = args[2]
				asset = args[3]
			}

			amount, err := strconv.ParseInt(amountStr, 10, 64)
			if err != nil {
				return err
			}

			metadata, err := fctl.ParseMetadata(fctl.GetStringSlice(cmd, metadataFlag))
			if err != nil {
				return err
			}

			reference := fctl.GetString(cmd, referenceFlag)
			response, _, err := ledgerClient.TransactionsApi.
				CreateTransaction(cmd.Context(), fctl.GetString(cmd, internal.LedgerFlag)).
				PostTransaction(formance.PostTransaction{
					Postings: []formance.Posting{{
						Amount:      amount,
						Asset:       asset,
						Destination: destination,
						Source:      source,
					}},
					Reference: &reference,
					Metadata:  metadata,
				}).
				Execute()
			if err != nil {
				return err
			}

			return internal.PrintTransaction(cmd.OutOrStdout(), response.Data[0])
		}),
	)
}
