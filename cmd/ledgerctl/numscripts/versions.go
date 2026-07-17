package numscripts

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewVersionsCommand creates the numscripts versions command.
func NewVersionsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "versions <name>",
		Short: "List every stored version of a numscript",
		Long: `List the current latest version (the greatest stored semver) and every
stored version of a numscript, highest semver first.

Examples:
  ledgerctl numscripts versions transfer --ledger myledger`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runVersions,
	}

	cmdutil.AddConsistencyFlags(cmd)
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runVersions(cmd *cobra.Command, args []string) error {
	name := args[0]

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

	resp, err := client.ListNumscriptVersions(ctx, &servicepb.ListNumscriptVersionsRequest{
		Ledger: ledgerName,
		Name:   name,
		Read:   cmdutil.BuildReadOptions(cmdutil.GetConsistencyFlags(cmd)),
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to list numscript versions", err)
	}

	versions := resp.GetVersions()

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
	}

	if len(versions) == 0 {
		pterm.Info.Printfln("No versions stored for numscript %s on ledger %s.", name, ledgerName)

		return nil
	}

	pterm.Printf("Latest: %s\n\n", pterm.Cyan(resp.GetLatestVersion()))

	tableData := pterm.TableData{
		{"VERSION", "CREATED AT"},
	}

	for _, v := range versions {
		createdAt := ""
		if v.GetCreatedAt() != nil {
			createdAt = v.GetCreatedAt().AsTime().Format("2006-01-02T15:04:05Z07:00")
		}

		tableData = append(tableData, []string{
			v.GetVersion(),
			createdAt,
		})
	}

	if err := pterm.DefaultTable.WithHasHeader().WithData(tableData).Render(); err != nil {
		return err
	}

	pterm.Println()

	return nil
}
