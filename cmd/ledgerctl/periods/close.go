package periods

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewCloseCommand creates the periods close command.
func NewCloseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "close",
		Short: "Close the current open period",
		Long:  "Close the current open period and open a new one. A background seal process will compute the sealing hash.",
		RunE:  runClose,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")

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
		Requests: []*servicepb.Request{
			{
				Type: &servicepb.Request_ClosePeriod{
					ClosePeriod: &servicepb.ClosePeriodRequest{},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("closing period: %w", err)
	}

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(resp)
	}

	if len(resp.Logs) > 0 {
		log := resp.Logs[0]
		if closePeriodLog := log.Payload.GetClosePeriod(); closePeriodLog != nil {
			pterm.Success.Printfln("Period %d closed successfully", closePeriodLog.ClosedPeriod.Id)
			pterm.Info.Printfln("New period %d opened", closePeriodLog.NewPeriod.Id)
			pterm.Info.Println("Background sealing process will compute the sealing hash")
		} else {
			pterm.Success.Println("Period closed successfully")
		}
	}

	return nil
}
