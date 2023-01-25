package wallets

import (
	"strconv"

	"github.com/formancehq/fctl/cmd/wallets/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewCreditWalletCommand() *cobra.Command {
	const (
		metadataFlag = "metadata"
		balanceFlag  = "balance"
		sourceFlag   = "source"
	)
	return fctl.NewCommand("credit <amount> <asset>",
		fctl.WithShortDescription("Credit a wallets"),
		fctl.WithAliases("cr"),
		fctl.WithConfirmFlag(),
		fctl.WithArgs(cobra.ExactArgs(2)),
		fctl.WithStringSliceFlag(metadataFlag, []string{""}, "Metadata to use"),
		fctl.WithStringFlag(balanceFlag, "", "Balance to credit"),
		fctl.WithStringSliceFlag(sourceFlag, []string{}, `Use --source account=<account> | --source wallet=id:<wallet-id>[/<balance>] | --source wallet=name:<wallet-name>[/<balance>]`),
		internal.WithTargetingWalletByName(),
		internal.WithTargetingWalletByID(),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return errors.Wrap(err, "reading config")
			}

			organizationID, err := fctl.ResolveOrganizationID(cmd, cfg)
			if err != nil {
				return err
			}

			stack, err := fctl.ResolveStack(cmd, cfg, organizationID)
			if err != nil {
				return err
			}

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to credit a wallets") {
				return fctl.ErrMissingApproval
			}

			client, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return errors.Wrap(err, "creating stack client")
			}

			amountStr := args[0]
			asset := args[1]
			walletID, err := internal.RetrieveWalletIDFromName(cmd, client)
			if err != nil {
				return err
			}

			if walletID == "" {
				return errors.New("You need to specify wallet id using --id or --name flags")
			}

			amount, err := strconv.ParseInt(amountStr, 10, 32)
			if err != nil {
				return errors.Wrap(err, "parsing amount")
			}

			metadata, err := fctl.ParseMetadata(fctl.GetStringSlice(cmd, metadataFlag))
			if err != nil {
				return err
			}

			sources := make([]formance.Subject, 0)
			for _, sourceStr := range fctl.GetStringSlice(cmd, sourceFlag) {
				source, err := internal.ParseSubject(sourceStr, cmd, client)
				if err != nil {
					return err
				}
				sources = append(sources, *source)
			}

			_, err = client.WalletsApi.CreditWallet(cmd.Context(), walletID).CreditWalletRequest(formance.CreditWalletRequest{
				Amount: formance.Monetary{
					Asset:  asset,
					Amount: amount,
				},
				Metadata: metadata,
				Sources:  sources,
				Balance:  formance.PtrString(fctl.GetString(cmd, balanceFlag)),
			}).Execute()
			if err != nil {
				return err
			}

			fctl.Success(cmd.OutOrStdout(), "Wallet credited successfully!")

			return nil
		}),
	)
}
