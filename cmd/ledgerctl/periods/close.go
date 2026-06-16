package periods

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewCloseCommand creates the periods close command.
func NewCloseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "close",
		Short: "Close the current open period",
		Long:  "Close the current open period and open a new one. A background seal process will compute the sealing hash.",
		RunE:  runClose,
	}

	cmdutil.AddOutputFlags(cmd)

	return cmd
}

func runClose(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
			Type: &servicepb.Request_ClosePeriod{
				ClosePeriod: &servicepb.ClosePeriodRequest{},
			},
		}),
	})
	if err != nil {
		return fmt.Errorf("closing period: %w", err)
	}

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
	}

	if len(resp.GetLogs()) > 0 {
		log := resp.GetLogs()[0]
		if closePeriodLog := log.GetPayload().GetClosePeriod(); closePeriodLog != nil {
			pterm.Success.Printfln("Period %d closed successfully", closePeriodLog.GetClosedPeriod().GetId())
			pterm.Info.Printfln("New period %d opened", closePeriodLog.GetNewPeriod().GetId())
			pterm.Info.Println("Background sealing process will compute the sealing hash")
		} else {
			pterm.Success.Println("Period closed successfully")
		}
	}

	return nil
}
