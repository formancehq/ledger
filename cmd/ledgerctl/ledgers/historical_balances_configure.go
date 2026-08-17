package ledgers

import (
	"errors"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func newConfigureHistoricalBalancesCommand(enabled bool) *cobra.Command {
	action := "enable"
	if !enabled {
		action = "disable"
	}
	cmd := &cobra.Command{
		Use:               action + " [ledger]",
		Short:             action + " historical balances for a ledger",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runConfigureHistoricalBalances(cmd, args, enabled)
		},
	}
	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	cmdutil.AddOutputFlags(cmd)

	return cmd
}

func runConfigureHistoricalBalances(cmd *cobra.Command, args []string, enabled bool) error {
	ledger, _ := cmd.Flags().GetString("ledger")
	if len(args) == 1 {
		ledger = args[0]
	}
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ledger, err = cmdutil.SelectLedger(cmd, client, ledger)
	if err != nil {
		return err
	}
	request := &servicepb.Request{Type: &servicepb.Request_ConfigureHistoricalBalances{
		ConfigureHistoricalBalances: &servicepb.ConfigureHistoricalBalancesRequest{Ledger: ledger, Enabled: enabled},
	}}
	apply, err := cmdutil.BuildApplyRequest(cmd, request)
	if err != nil {
		return err
	}
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()
	response, err := client.Apply(ctx, apply)
	if err != nil {
		return cmdutil.FormatGRPCError("failed to configure historical balances", err)
	}
	if err := cmdutil.VerifyResponseSignatures(cmd, response.GetLogs()); err != nil {
		return err
	}
	if len(response.GetLogs()) != 1 || response.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetConfiguredHistoricalBalances() == nil {
		return errors.New("unexpected historical-balance configuration response")
	}
	if handled, err := cmdutil.EncodeStructured(cmd, response.GetLogs()[0]); handled || err != nil {
		return err
	}
	pterm.Success.Printfln("Historical balances %s for ledger %s", historicalBalancesState(enabled), ledger)

	return nil
}

func historicalBalancesState(enabled bool) string {
	if enabled {
		return "enabled"
	}

	return "disabled"
}
