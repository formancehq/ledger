package wallets

import (
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewUpdateCommand() *cobra.Command {
	const (
		metadataFlag = "metadata"
	)
	return fctl.NewCommand("update <wallet-id>",
		fctl.WithShortDescription("Update a wallets"),
		fctl.WithAliases("up"),
		fctl.WithConfirmFlag(),
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithStringSliceFlag(metadataFlag, []string{""}, "Metadata to use"),
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

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to update a wallets") {
				return fctl.ErrMissingApproval
			}

			client, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return errors.Wrap(err, "creating stack client")
			}

			metadata, err := fctl.ParseMetadata(fctl.GetStringSlice(cmd, metadataFlag))
			if err != nil {
				return err
			}

			_, err = client.WalletsApi.UpdateWallet(cmd.Context(), args[0]).UpdateWalletRequest(formance.UpdateWalletRequest{
				Metadata: metadata,
			}).Execute()
			if err != nil {
				return errors.Wrap(err, "Updating wallets")
			}

			fctl.Success(cmd.OutOrStdout(), "Wallet updated successfully!")
			return nil
		}),
	)
}
