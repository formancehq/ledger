package ledgers

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func newHistoricalBalancesStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "status [ledger]",
		Short:             "Show historical-balance projection status",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runHistoricalBalancesStatus,
	}
	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	cmdutil.AddOutputFlags(cmd)

	return cmd
}

func runHistoricalBalancesStatus(cmd *cobra.Command, args []string) error {
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
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()
	status, err := client.GetHistoricalBalancesStatus(ctx, &servicepb.GetHistoricalBalancesStatusRequest{Ledger: ledger})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to get historical-balance status", err)
	}
	if handled, err := cmdutil.EncodeStructured(cmd, status); handled || err != nil {
		return err
	}
	pterm.Printf("Ledger: %s\nState: %s\nAudit watermark: %d\nLog watermark: %d\n", status.GetLedger(), status.GetState(), status.GetAuditWatermark(), status.GetLogWatermark())
	if status.GetError() != "" {
		pterm.Printf("Error: %s\n", status.GetError())
	}

	return nil
}
