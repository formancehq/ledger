package periods

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewListCommand creates the periods list command.
func NewListCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all periods",
		Long:  "List all accounting periods with their status",
		RunE:  runList,
	}
}

func runList(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	stream, err := client.ListPeriods(ctx, &servicepb.ListPeriodsRequest{})
	if err != nil {
		return fmt.Errorf("listing periods: %w", err)
	}

	var periods []*commonpb.Period
	for {
		period, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("receiving period: %w", err)
		}
		periods = append(periods, period)
	}

	if len(periods) == 0 {
		pterm.Info.Println("No periods found.")
		return nil
	}

	// Build table
	tableData := pterm.TableData{
		{"ID", "Status", "Start", "End", "Close Seq"},
	}

	for _, p := range periods {
		var (
			startStr    = "-"
			endStr      = "-"
			closeSeqStr = "-"
		)

		if p.Start != nil {
			startStr = time.UnixMicro(int64(p.Start.Data)).Format(time.RFC3339)
		}
		if p.End != nil {
			endStr = time.UnixMicro(int64(p.End.Data)).Format(time.RFC3339)
		}
		if p.CloseSequence > 0 {
			closeSeqStr = fmt.Sprintf("%d", p.CloseSequence)
		}

		tableData = append(tableData, []string{
			fmt.Sprintf("%d", p.Id),
			formatPeriodStatus(p.Status),
			startStr,
			endStr,
			closeSeqStr,
		})
	}

	if err := pterm.DefaultTable.WithHasHeader().WithData(tableData).Render(); err != nil {
		return fmt.Errorf("rendering table: %w", err)
	}

	return nil
}

func formatPeriodStatus(status commonpb.PeriodStatus) string {
	name := strings.TrimPrefix(commonpb.PeriodStatus_name[int32(status)], "PERIOD_")
	switch status {
	case commonpb.PeriodStatus_PERIOD_OPEN:
		return pterm.Green(name)
	case commonpb.PeriodStatus_PERIOD_CLOSING:
		return pterm.Yellow(name)
	case commonpb.PeriodStatus_PERIOD_CLOSED:
		return pterm.Cyan(name)
	case commonpb.PeriodStatus_PERIOD_ARCHIVED:
		return pterm.Gray(name)
	default:
		return name
	}
}
