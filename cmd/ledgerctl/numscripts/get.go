package numscripts

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewGetCommand creates the numscripts get command.
func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Get a numscript from the library",
		Long: `Get a numscript from the global library by name.

By default, returns the latest version. Use --version to get a specific version.

Examples:
  ledgerctl numscripts get transfer
  ledgerctl numscripts get transfer --version 2`,
		Args: cobra.ExactArgs(1),
		RunE: runGet,
	}

	cmd.Flags().String("version", "", "Specific version to retrieve (empty = latest)")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runGet(cmd *cobra.Command, args []string) error {
	name := args[0]
	version, _ := cmd.Flags().GetString("version")

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	info, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
		Name:    name,
		Version: version,
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to get numscript", err)
	}

	pterm.Printf("Name:       %s\n", pterm.Cyan(info.GetName()))
	pterm.Printf("Version:    %s\n", info.GetVersion())

	if info.GetCreatedAt() != nil {
		pterm.Printf("Created at: %s\n", pterm.Gray(info.GetCreatedAt().AsTime().Format("2006-01-02T15:04:05Z07:00")))
	}

	pterm.Println()
	pterm.DefaultSection.Println("Content")
	pterm.Println(info.GetContent())

	return nil
}
