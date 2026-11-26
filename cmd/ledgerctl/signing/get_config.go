package signing

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewGetConfigCommand creates the signing get-config command.
func NewGetConfigCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-config",
		Short: "Show whether request signatures are mandatory",
		Long: `Show the cluster's write-signing policy.

requireSignatures is true when unsigned write requests are rejected.

Examples:
  ledgerctl signing get-config
  ledgerctl signing get-config --json`,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runGetConfig,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runGetConfig(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	resp, err := client.GetSigningConfig(ctx, &servicepb.GetSigningConfigRequest{})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to read signing config", err)
	}

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
	}

	state := "disabled"
	if resp.GetRequireSignatures() {
		state = "enabled"
	}
	pterm.Printfln("Mandatory signatures: %s", state)

	return nil
}
