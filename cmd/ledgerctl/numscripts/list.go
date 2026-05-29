package numscripts

import (
	"errors"
	"io"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewListCommand creates the numscripts list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all numscripts in the library",
		Long: `List all numscripts in a ledger's library (latest version of each).

Examples:
  ledgerctl numscripts list --ledger myledger`,
		Args: cobra.NoArgs,
		RunE: runList,
	}

	cmd.Flags().Uint64("checkpoint-id", 0, "Read from a query checkpoint instead of the live store")
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

	checkpointID, _ := cmd.Flags().GetUint64("checkpoint-id")

	stream, err := client.ListNumscripts(ctx, &servicepb.ListNumscriptsRequest{Ledger: ledgerName, CheckpointId: checkpointID})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to list numscripts", err)
	}

	tableData := pterm.TableData{
		{"NAME", "VERSION", "CREATED AT"},
	}

	count := 0

	for {
		info, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return cmdutil.FormatGRPCError("failed to receive numscript", err)
		}

		createdAt := ""
		if info.GetCreatedAt() != nil {
			createdAt = info.GetCreatedAt().AsTime().Format("2006-01-02T15:04:05Z07:00")
		}

		tableData = append(tableData, []string{
			info.GetName(),
			info.GetVersion(),
			createdAt,
		})
		count++
	}

	if count == 0 {
		pterm.Info.Printfln("No numscripts in library for ledger %s.", ledgerName)
		pterm.Println(pterm.Gray("Hint: Save a numscript using:"))
		pterm.FgCyan.Printfln("  ledgerctl numscripts save <name> --ledger %s --file <path>", ledgerName)

		return nil
	}

	return pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}
