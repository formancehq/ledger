package holds

import (
	"github.com/formancehq/fctl/cmd/wallets/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewListCommand() *cobra.Command {
	const (
		metadataFlag = "metadata"
	)
	return fctl.NewCommand("list",
		fctl.WithShortDescription("List holds of a wallets"),
		fctl.WithAliases("ls", "l"),
		fctl.WithArgs(cobra.RangeArgs(0, 1)),
		internal.WithTargetingWalletByName(),
		internal.WithTargetingWalletByID(),
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

			client, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return errors.Wrap(err, "creating stack client")
			}

			walletID, err := internal.RetrieveWalletID(cmd, client)
			if err != nil {
				return err
			}

			metadata, err := fctl.ParseMetadata(fctl.GetStringSlice(cmd, metadataFlag))
			if err != nil {
				return err
			}

			res, _, err := client.WalletsApi.
				GetHolds(cmd.Context()).
				Metadata(metadata).
				WalletID(walletID).
				Execute()
			if err != nil {
				return errors.Wrap(err, "listing wallets")
			}

			if len(res.Cursor.Data) == 0 {
				fctl.Println("No holds found.")
				return nil
			}

			if err := pterm.DefaultTable.
				WithHasHeader(true).
				WithWriter(cmd.OutOrStdout()).
				WithData(
					fctl.Prepend(
						fctl.Map(res.Cursor.Data,
							func(src formance.Hold) []string {
								return []string{
									src.Id,
									src.WalletID,
									src.Description,
									fctl.MetadataAsShortString(src.Metadata),
								}
							}),
						[]string{"ID", "Wallet ID", "Description", "Metadata"},
					),
				).Render(); err != nil {
				return errors.Wrap(err, "rendering table")
			}

			return nil
		}),
	)
}
