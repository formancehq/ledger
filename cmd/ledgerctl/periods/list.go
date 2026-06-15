package periods

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewListCommand creates the periods list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: cmdutil.ListAliases,
		Short:   "List all periods",
		Long:    "List all accounting periods with their status",
		RunE:    runList,
	}

	cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{
		SupportsReverse: true,
	})
	cmdutil.AddMinLogSequenceFlag(cmd)
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

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

	pgn := cmdutil.GetPaginationFlags(cmd)
	minLogSeq, _ := cmd.Flags().GetUint64("min-log-sequence")

	periods, nextCursor, err := cmdutil.FetchSinglePageOrAll(cmd, pgn.Cursor, func(cur string) ([]*commonpb.Period, metadata.MD, error) {
		page := pgn
		page.Cursor = cur

		stream, err := client.ListPeriods(ctx, &servicepb.ListPeriodsRequest{
			Options: cmdutil.BuildListOptions(page, cmdutil.ConsistencyFlags{MinLogSequence: minLogSeq}, nil),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("listing periods: %w", err)
		}

		items, recvErr := cmdutil.CollectStream(stream)
		if recvErr != nil {
			return nil, nil, fmt.Errorf("receiving periods: %w", recvErr)
		}

		return items, stream.Trailer(), nil
	})
	if err != nil {
		return err
	}

	if handled, err := cmdutil.EncodeStructured(cmd, periods); handled || err != nil {
		cmdutil.EmitNextCursorHint(cmd, nextCursor)

		return err
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

	pterm.Println()

	cmdutil.EmitNextCursorHint(cmd, nextCursor)

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
