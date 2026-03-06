package periods

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewListCommand creates the periods list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all periods",
		Long:  "List all accounting periods with their status",
		RunE:  runList,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")

	return cmd
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
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return fmt.Errorf("receiving period: %w", err)
		}

		periods = append(periods, period)
	}

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")

		return encoder.Encode(periods)
	}

	if len(periods) == 0 {
		pterm.Info.Println("No periods found.")

		return nil
	}

	// Build table
	tableData := pterm.TableData{
		{"ID", "STATUS", "START", "END", "CLOSE SEQ"},
	}

	for _, p := range periods {
		var (
			startStr    = "-"
			endStr      = "-"
			closeSeqStr = "-"
		)

		if p.GetStart() != nil {
			startStr = time.UnixMicro(int64(p.GetStart().GetData())).Format(time.RFC3339)
		}

		if p.GetEnd() != nil {
			endStr = time.UnixMicro(int64(p.GetEnd().GetData())).Format(time.RFC3339)
		}

		if p.GetCloseSequence() > 0 {
			closeSeqStr = strconv.FormatUint(p.GetCloseSequence(), 10)
		}

		tableData = append(tableData, []string{
			strconv.FormatUint(p.GetId(), 10),
			formatPeriodStatus(p.GetStatus()),
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
