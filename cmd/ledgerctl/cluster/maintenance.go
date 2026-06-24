package cluster

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewMaintenanceCommand creates the cluster maintenance command.
func NewMaintenanceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "maintenance <true|false|enable|disable>",
		Short: "Enable or disable cluster maintenance mode",
		Long: `Enable or disable cluster maintenance mode.

When enabled, all write operations (Raft commands) are blocked at the admission
layer. Only the maintenance mode command itself is allowed through (to disable
maintenance mode). Read operations continue to work normally.

Examples:
  # Enable maintenance mode
  ledgerctl cluster maintenance enable

  # Disable maintenance mode
  ledgerctl cluster maintenance disable

  # With request signing
  ledgerctl cluster maintenance enable --signing-key /path/to/seed`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runMaintenance,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runMaintenance(cmd *cobra.Command, args []string) error {
	var enabled bool

	switch args[0] {
	case "true", "1", "yes", "on", "enable":
		enabled = true
	case "false", "0", "no", "off", "disable":
		enabled = false
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
	if !enabled {
		action = "Disabling"
	}

	spinner, _ := pterm.DefaultSpinner.Start(action + " maintenance mode...")

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_SetMaintenanceMode{
				SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{
					Enabled: enabled,
				},
			},
		},
	}

	applyReq, err := cmdutil.BuildApplyRequest(cmd, requests...)
	if err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	_, err = client.Apply(ctx, applyReq)
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to update maintenance mode", err)
	}

	if enabled {
		spinner.Success("Maintenance mode enabled")
	} else {
		spinner.Success("Maintenance mode disabled")
	}

	if handled, err := cmdutil.EncodeStructured(cmd, map[string]any{"maintenanceMode": enabled}); handled || err != nil {
		return err
	}

	return nil
}
