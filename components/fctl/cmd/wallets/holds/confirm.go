package holds

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewConfirmCommand() *cobra.Command {
	const (
		finalFlag  = "final"
		amountFlag = "amount"
	)
	return fctl.NewCommand("confirm <hold-id>",
		fctl.WithShortDescription("Confirm a hold"),
		fctl.WithAliases("c", "conf"),
		fctl.WithArgs(cobra.RangeArgs(1, 2)),
		fctl.WithBoolFlag(finalFlag, false, "Is final debit (close hold)"),
		fctl.WithIntFlag(amountFlag, 0, "Amount to confirm"),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return errors.Wrap(err, "retrieving config")
			}

			organizationID, err := fctl.ResolveOrganizationID(cmd, cfg)
			if err != nil {
				return err
			}

			stack, err := fctl.ResolveStack(cmd, cfg, organizationID)
			if err != nil {
				return err
			}

			stackClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return errors.Wrap(err, "creating stack client")
			}

			final := fctl.GetBool(cmd, finalFlag)
			amount := int64(fctl.GetInt(cmd, amountFlag))

			_, err = stackClient.WalletsApi.ConfirmHold(cmd.Context(), args[0]).
				ConfirmHoldRequest(formance.ConfirmHoldRequest{
					Amount: &amount,
					Final:  &final,
				}).Execute()
			if err != nil {
				return errors.Wrap(err, "listing wallets")
			}

			fctl.Success(cmd.OutOrStdout(), "Hold '%s' confirmed!", args[0])

			return nil
		}),
	)
}
