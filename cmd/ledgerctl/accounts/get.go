package accounts

import (
	"errors"
	"fmt"
	"sort"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewGetCommand creates the accounts get command.
func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get [address]",
		Aliases: cmdutil.GetAliases,
		Short:   "Get an account by address",
		Long: `Get detailed information about an account including its volumes via gRPC.

If --ledger is not provided and only one ledger exists, it will be used automatically.
If multiple ledgers exist, you will be prompted to select one.

Examples:
  ledgerctl accounts get bank --ledger my-ledger
  ledgerctl accounts get bank  # Will prompt for ledger if needed
  ledgerctl accounts get       # Will prompt for both ledger and address`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runGet,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Uint64("checkpoint-id", 0, "Read from a query checkpoint instead of the live store")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runGet(cmd *cobra.Command, args []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ledgerFlag, _ := cmd.Flags().GetString("ledger")

	ledgerName, err := cmdutil.SelectLedger(cmd, client, ledgerFlag)
	if err != nil {
		return err
	}

	var address string
	if len(args) > 0 {
		address = args[0]
	} else {
		result, err := pterm.DefaultInteractiveTextInput.
			WithDefaultText("Enter account address").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		address = result
		if address == "" {
			pterm.Error.Println("Account address is required")

			return cmdutil.Displayed(errors.New("account address is required"))
		}
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching account %s...", pterm.Cyan(address)))

	checkpointID, _ := cmd.Flags().GetUint64("checkpoint-id")

	account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
		Ledger:       ledgerName,
		Address:      address,
		CheckpointId: checkpointID,
	})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to get account", err)
	}

	_ = spinner.Stop()

	if handled, err := cmdutil.EncodeStructured(cmd, account); handled || err != nil {
		return err
	}

	pterm.Println()

	pterm.Printf("Account: %s\n", pterm.Cyan(account.GetAddress()))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	if len(account.GetMetadata()) > 0 {
		pterm.Println("Metadata:")

		metadataTable := pterm.TableData{
			{"KEY", "VALUE"},
		}
		for key, value := range account.GetMetadata() {
			metadataTable = append(metadataTable, []string{
				key,
				commonpb.MetadataValueToString(value),
			})
		}

		err := pterm.DefaultTable.WithHasHeader().WithData(metadataTable).Render()
		if err != nil {
			return err
		}

		pterm.Println()
	}

	pterm.Println("Volumes:")

	if len(account.GetVolumes()) > 0 {
		volumesTable := pterm.TableData{
			{"ASSET", "INPUT", "OUTPUT", "BALANCE"},
		}

		// With --rescale, currencies that differ only in precision are summed
		// into a single base-currency row, re-expressed at the requested scale.
		if rescale := cmdutil.RescaleTarget(cmd); rescale != nil {
			raw := make(map[string]cmdutil.RawVolume, len(account.GetVolumes()))
			for asset, vol := range account.GetVolumes() {
				raw[asset] = cmdutil.RawVolume{Input: vol.GetInput(), Output: vol.GetOutput()}
			}

			for _, av := range cmdutil.AggregateVolumes(raw) {
				balanceColor := pterm.Green
				if av.Balance.Sign() < 0 {
					balanceColor = pterm.Red
				}

				volumesTable = append(volumesTable, []string{
					invariants.FormatAsset(av.Asset, *rescale),
					cmdutil.RescaleAmount(av.Input, av.Precision, *rescale),
					cmdutil.RescaleAmount(av.Output, av.Precision, *rescale),
					balanceColor(cmdutil.RescaleAmount(av.Balance, av.Precision, *rescale)),
				})
			}

			return pterm.DefaultTable.WithHasHeader().WithData(volumesTable).Render()
		}

		assets := make([]string, 0, len(account.GetVolumes()))
		for asset := range account.GetVolumes() {
			assets = append(assets, asset)
		}

		sort.Strings(assets)

		for _, asset := range assets {
			vol := account.GetVolumes()[asset]
			balance := vol.GetBalance()

			balanceColor := pterm.Green
			if balance != "" && balance[0] == '-' {
				balanceColor = pterm.Red
			}

			volumesTable = append(volumesTable, []string{
				asset,
				vol.GetInput(),
				vol.GetOutput(),
				balanceColor(balance),
			})
		}

		return pterm.DefaultTable.WithHasHeader().WithData(volumesTable).Render()
	}

	pterm.Println(pterm.Gray("(no volumes)"))

	return nil
}
