package queries

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewExecuteCommand creates the queries execute command.
func NewExecuteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "execute <name>",
		Aliases: []string{"exec", "run"},
		Short:   "Execute a prepared query",
		Long: `Execute a prepared query and display results.

Parameters are passed as key=value pairs via --param flags.
Value types are inferred: integers, booleans, otherwise strings.

Examples:
  ledgerctl queries execute active-users --ledger my-ledger
  ledgerctl queries execute by-tier --ledger my-ledger --param tier=gold
  ledgerctl queries execute big-txns --ledger my-ledger --param min_amount=1000
  ledgerctl queries execute active-users --ledger my-ledger --mode aggregate
  ledgerctl queries execute active-users --ledger my-ledger --all`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeQueryNames,
		RunE:              runExecute,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().StringArray("param", nil, "Query parameter as key=value (repeatable)")
	cmd.Flags().Uint32("page-size", cmdutil.DefaultPageSize, "Number of results per page")
	cmd.Flags().String("mode", "list", "Query mode: list or aggregate")
	cmd.Flags().Uint64("min-log-sequence", 0, "Minimum log sequence before reading")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	cmdutil.AddAnalyzeFlag(cmd)
	cmd.Flags().Bool("all", false, "Fetch all results at once (no pagination)")
	cmdutil.AddOutputFlags(cmd)

	return cmd
}

func runExecute(cmd *cobra.Command, args []string) error {
	queryName := args[0]

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

	paramFlags, _ := cmd.Flags().GetStringArray("param")
	pageSize, _ := cmd.Flags().GetUint32("page-size")
	modeStr, _ := cmd.Flags().GetString("mode")
	minLogSeq, _ := cmd.Flags().GetUint64("min-log-sequence")
	showProfile, _ := cmd.Flags().GetBool("analyze")
	fetchAll, _ := cmd.Flags().GetBool("all")

	params, err := parseParams(paramFlags)
	if err != nil {
		return err
	}

	mode, err := parseMode(modeStr)
	if err != nil {
		return err
	}

	if fetchAll {
		pageSize = 0
	}

	var cursor string

	pageNum := 1

	for {
		ctx, cancel := cmdutil.GetContext(cmd)
		if showProfile {
			ctx = cmdutil.ProfileContext(ctx)
		}

		var trailer metadata.MD

		resp, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
			Ledger:         ledgerName,
			QueryName:      queryName,
			Parameters:     params,
			PageSize:       pageSize,
			Cursor:         cursor,
			MinLogSequence: minLogSeq,
			Mode:           mode,
		}, ggrpc.Trailer(&trailer))

		cancel()

		if err != nil {
			return cmdutil.FormatGRPCError("failed to execute prepared query", err)
		}

		switch result := resp.GetResult().(type) {
		case *servicepb.ExecutePreparedQueryResponse_Cursor:
			renderCursorPage(cmd, result.Cursor, pageNum)

			if showProfile {
				cmdutil.RenderProfile(cmdutil.ExtractProfile(trailer))
			}

			if !result.Cursor.GetHasMore() || fetchAll {
				return nil
			}

			if cmdutil.IsStructuredOutput(cmd) {
				return nil
			}

			proceed, promptErr := pterm.DefaultInteractiveConfirm.
				WithDefaultText("Load next page?").
				WithDefaultValue(true).
				Show()
			if promptErr != nil {
				return fmt.Errorf("failed to read input: %w", promptErr)
			}

			if !proceed {
				return nil
			}

			cursor = result.Cursor.GetNext()
			pageNum++

		case *servicepb.ExecutePreparedQueryResponse_Aggregate:
			err := renderAggregate(cmd, result.Aggregate)
			if err != nil {
				return err
			}

			if showProfile {
				cmdutil.RenderProfile(cmdutil.ExtractProfile(trailer))
			}

			return nil

		default:
			pterm.Info.Println("No results.")

			return nil
		}
	}
}

func renderCursorPage(cmd *cobra.Command, cursor *commonpb.PreparedQueryCursor, pageNum int) {
	if len(cursor.GetAccountData()) > 0 {
		if handled, err := cmdutil.EncodeStructured(cmd, cursor.GetAccountData()); handled || err != nil {
			return
		}

		if pageNum > 1 {
			pterm.Println()
		}

		pterm.Printf("Results (Page %d)\n", pageNum)
		pterm.Println(pterm.Gray("─────────────────────────────────"))

		tableData := pterm.TableData{{"ADDRESS", "METADATA"}}
		for _, a := range cursor.GetAccountData() {
			metaCount := strconv.Itoa(len(a.GetMetadata()))

			tableData = append(tableData, []string{a.GetAddress(), metaCount})
		}

		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

		return
	}

	if len(cursor.GetTransactionData()) > 0 {
		if handled, err := cmdutil.EncodeStructured(cmd, cursor.GetTransactionData()); handled || err != nil {
			return
		}

		if pageNum > 1 {
			pterm.Println()
		}

		pterm.Printf("Results (Page %d)\n", pageNum)
		pterm.Println(pterm.Gray("─────────────────────────────────"))

		tableData := pterm.TableData{{"ID", "TIMESTAMP", "POSTINGS", "REFERENCE"}}
		for _, t := range cursor.GetTransactionData() {
			tableData = append(tableData, []string{
				strconv.FormatUint(t.GetId(), 10),
				t.GetTimestamp().AsTime().Format("2006-01-02 15:04:05"),
				strconv.Itoa(len(t.GetPostings())),
				t.GetReference(),
			})
		}

		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

		return
	}

	if pageNum == 1 {
		pterm.Info.Println("No results.")
	} else {
		pterm.Info.Println("No more results.")
	}
}

func renderAggregate(cmd *cobra.Command, result *commonpb.AggregateResult) error {
	if handled, err := cmdutil.EncodeStructured(cmd, result); handled || err != nil {
		return err
	}

	if len(result.GetVolumes()) > 0 {
		tableData := pterm.TableData{{"ASSET", "COLOR", "INPUT", "OUTPUT"}}
		for _, v := range result.GetVolumes() {
			tableData = append(tableData, []string{
				v.GetAsset(),
				v.GetColor(),
				v.GetInput().ToBigInt().String(),
				v.GetOutput().ToBigInt().String(),
			})
		}

		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
	}

	for _, g := range result.GetGroups() {
		pterm.Println()
		pterm.Printfln("Group: %s", g.GetPrefix())

		tableData := pterm.TableData{{"ASSET", "COLOR", "INPUT", "OUTPUT"}}
		for _, v := range g.GetVolumes() {
			tableData = append(tableData, []string{
				v.GetAsset(),
				v.GetColor(),
				v.GetInput().ToBigInt().String(),
				v.GetOutput().ToBigInt().String(),
			})
		}

		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
	}

	return nil
}

// parseParams turns --param key=value flags into ParameterValue messages.
// Values are always sent as strings; the server coerces to the parameter's
// declared type (int64, uint64, bool) at compile time via extractInt64 /
// extractUint64 / extractBool. The CLI used to infer the type from the raw
// value, but that broke any string-typed param whose contents happened to
// parse as an int (e.g. a hex hash made of digits — see #249).
func parseParams(flags []string) (map[string]*commonpb.ParameterValue, error) {
	if len(flags) == 0 {
		return nil, nil
	}

	params := make(map[string]*commonpb.ParameterValue, len(flags))

	for _, f := range flags {
		k, v, ok := strings.Cut(f, "=")
		if !ok {
			return nil, fmt.Errorf("invalid parameter %q (expected key=value)", f)
		}

		params[k] = &commonpb.ParameterValue{Value: &commonpb.ParameterValue_StringValue{StringValue: v}}
	}

	return params, nil
}

func parseMode(s string) (commonpb.QueryMode, error) {
	switch s {
	case "list", "":
		return commonpb.QueryMode_QUERY_MODE_LIST, nil
	case "aggregate", "agg":
		return commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES, nil
	default:
		return 0, fmt.Errorf("unknown mode %q (use list or aggregate)", s)
	}
}
