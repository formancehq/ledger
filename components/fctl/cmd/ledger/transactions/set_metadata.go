package transactions

import (
	"github.com/formancehq/fctl/cmd/ledger/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewSetMetadataCommand() *cobra.Command {
	return fctl.NewCommand("set-metadata <transaction-id> [<key>=<value>...]",
		fctl.WithShortDescription("Set metadata on transaction"),
		fctl.WithAliases("sm", "set-meta"),
		fctl.WithConfirmFlag(),
		fctl.WithValidArgs("last"),
		fctl.WithArgs(cobra.MinimumNArgs(2)),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {

			metadata, err := fctl.ParseMetadata(args[1:])
			if err != nil {
				return err
			}

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

			ledgerClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			transactionID, err := internal.TransactionIDOrLastN(cmd.Context(), ledgerClient,
				fctl.GetString(cmd, internal.LedgerFlag), args[0])
			if err != nil {
				return err
			}

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to set a metadata on transaction %d", transactionID) {
				return fctl.ErrMissingApproval
			}

			_, err = ledgerClient.TransactionsApi.
				AddMetadataOnTransaction(cmd.Context(), fctl.GetString(cmd, internal.LedgerFlag), transactionID).
				RequestBody(metadata).
				Execute()
			if err != nil {
				return err
			}

			fctl.Success(cmd.OutOrStdout(), "Metadata added!")
			return nil
		}),
	)
}
