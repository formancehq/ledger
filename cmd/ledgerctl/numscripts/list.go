package numscripts

import (
	"errors"
	"io"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewListCommand creates the numscripts list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all numscripts in the library",
		Long: `List all numscripts in the global library (latest version of each).

Examples:
  ledgerctl numscripts list`,
		Args: cobra.NoArgs,
		RunE: runList,
	}

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

	stream, err := client.ListNumscripts(ctx, &servicepb.ListNumscriptsRequest{})
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
		pterm.Info.Println("No numscripts in library.")
		pterm.Println(pterm.Gray("Hint: Save a numscript using:"))
		pterm.FgCyan.Println("  ledgerctl numscripts save <name> --file <path>")

		return nil
	}

	return pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}
