package transactions

import (
	"github.com/formancehq/fctl/cmd/ledger/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewShowCommand() *cobra.Command {
	return fctl.NewCommand("show <transaction-id>",
		fctl.WithShortDescription("Print a transaction"),
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithAliases("sh"),
		fctl.WithValidArgs("last"),
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

			ledgerClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			ledger := fctl.GetString(cmd, internal.LedgerFlag)
			txId, err := internal.TransactionIDOrLastN(cmd.Context(), ledgerClient, ledger, args[0])
			if err != nil {
				return err
			}

			rsp, _, err := ledgerClient.TransactionsApi.GetTransaction(cmd.Context(), ledger, txId).Execute()
			if err != nil {
				return errors.Wrapf(err, "retrieving transaction")
			}

			return internal.PrintTransaction(cmd.OutOrStdout(), rsp.Data)
		}),
	)
}
