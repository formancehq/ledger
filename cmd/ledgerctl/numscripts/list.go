package numscripts

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewListCommand creates the numscripts list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: cmdutil.ListAliases,
		Short:   "List all numscripts in the library",
		Long: `List all numscripts in a ledger's library (latest version of each).

Examples:
  ledgerctl numscripts list --ledger myledger
  ledgerctl numscripts list --ledger myledger --page-size 5 --reverse`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runList,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{
		SupportsReverse: true,
	})
	cmdutil.AddConsistencyFlags(cmd)
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

	ledgerFlag, _ := cmd.Flags().GetString("ledger")

	ledgerName, err := cmdutil.SelectLedger(cmd, client, ledgerFlag)
	if err != nil {
		return err
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	pgn := cmdutil.GetPaginationFlags(cmd)
	cns := cmdutil.GetConsistencyFlags(cmd)

	fetchPage := func(cur string) ([]*commonpb.NumscriptInfo, metadata.MD, error) {
		page := pgn
		page.Cursor = cur

		stream, err := client.ListNumscripts(ctx, &servicepb.ListNumscriptsRequest{
			Ledger:  ledgerName,
			Options: cmdutil.BuildListOptions(page, cns, nil),
		})
		if err != nil {
			return nil, nil, cmdutil.FormatGRPCError("failed to list numscripts", err)
		}

		items, recvErr := cmdutil.CollectStream(stream)
		if recvErr != nil {
			return nil, nil, cmdutil.FormatGRPCError("failed to receive numscript", recvErr)
		}

		return items, stream.Trailer(), nil
	}

	scripts, nextCursor, err := cmdutil.FetchSinglePageOrAll(cmd, pgn.Cursor, fetchPage)
	if err != nil {
		return err
	}

	if handled, err := cmdutil.EncodeStructured(cmd, scripts); handled || err != nil {
		cmdutil.EmitNextCursorHint(cmd, nextCursor)

		return err
	}

	if len(scripts) == 0 {
		pterm.Info.Printfln("No numscripts in library for ledger %s.", ledgerName)
		pterm.Println(pterm.Gray("Hint: Save a numscript using:"))
		pterm.FgCyan.Printfln("  ledgerctl numscripts save <name> --ledger %s --file <path>", ledgerName)

		return nil
	}

	tableData := pterm.TableData{
		{"NAME", "VERSION", "CREATED AT"},
	}

	for _, info := range scripts {
		createdAt := ""
		if info.GetCreatedAt() != nil {
			createdAt = info.GetCreatedAt().AsTime().Format("2006-01-02T15:04:05Z07:00")
		}

		tableData = append(tableData, []string{
			info.GetName(),
			info.GetVersion(),
			createdAt,
		})
	}

	if err := pterm.DefaultTable.WithHasHeader().WithData(tableData).Render(); err != nil {
		return err
	}

	pterm.Println()

	cmdutil.EmitNextCursorHint(cmd, nextCursor)

	return nil
}
