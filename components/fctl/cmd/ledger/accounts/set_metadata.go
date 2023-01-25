package accounts

import (
	"github.com/formancehq/fctl/cmd/ledger/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/spf13/cobra"
)

func NewSetMetadataCommand() *cobra.Command {
	return fctl.NewCommand("set-metadata <account> [<key>=<value>...]",
		fctl.WithConfirmFlag(),
		fctl.WithShortDescription("Set metadata on account"),
		fctl.WithAliases("sm", "set-meta"),
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

			account := args[0]

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to set a metadata on account '%s'", account) {
				return fctl.ErrMissingApproval
			}

			ledgerClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			_, err = ledgerClient.AccountsApi.
				AddMetadataToAccount(cmd.Context(), fctl.GetString(cmd, internal.LedgerFlag), account).
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
