package transactions

import (
	"github.com/formancehq/fctl/cmd/ledger/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewRevertCommand() *cobra.Command {
	return fctl.NewCommand("revert <transaction-id>",
		fctl.WithConfirmFlag(),
		fctl.WithShortDescription("Revert a transaction"),
		fctl.WithArgs(cobra.ExactArgs(1)),
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

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to revert transaction %s", args[0]) {
				return fctl.ErrMissingApproval
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

			rsp, _, err := ledgerClient.TransactionsApi.RevertTransaction(cmd.Context(), ledger, txId).Execute()
			if err != nil {
				return errors.Wrapf(err, "reverting transaction")
			}

			return internal.PrintTransaction(cmd.OutOrStdout(), rsp.Data)
		}),
	)
}
