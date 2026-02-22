package signing

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewRequireCommand creates the signing require command.
func NewRequireCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "require <true|false>",
		Short: "Enable or disable mandatory request signatures",
		Long: `Enable or disable mandatory request signatures.

When enabled, all requests must be signed with a registered key.
This command must be signed by an existing key (use --signing-key).

Examples:
  # Enable mandatory signatures
  ledgerctl signing require true --signing-key /path/to/seed

  # Disable mandatory signatures
  ledgerctl signing require false --signing-key /path/to/seed`,
		Args: cobra.ExactArgs(1),
		RunE: runRequire,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runRequire(cmd *cobra.Command, args []string) error {
	var require bool
	switch args[0] {
	case "true", "1", "yes", "on", "enable":
		require = true
	case "false", "0", "no", "off", "disable":
		require = false
	default:
		return fmt.Errorf("expected 'true' or 'false', got %q", args[0])
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	action := "Enabling"
	if !require {
		action = "Disabling"
	}
	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("%s mandatory signatures...", action))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_SetSigningConfig{
				SetSigningConfig: &servicepb.SetSigningConfigRequest{
					RequireSignatures: require,
				},
			},
		},
	}

	if err := cmdutil.SignRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")
		return err
	}

	_, err = client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		spinner.Fail(fmt.Sprintf("Failed to %s mandatory signatures", strings.ToLower(action)))
		return cmdutil.FormatGRPCError("failed to update signing config", err)
	}

	if require {
		spinner.Success("Mandatory signatures enabled")
	} else {
		spinner.Success("Mandatory signatures disabled")
	}

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(map[string]any{"requireSignatures": require})
	}

	return nil
}
