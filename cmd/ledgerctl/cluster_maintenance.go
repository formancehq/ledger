package main

import (
	"fmt"
	"strings"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newClusterMaintenanceCommand creates the cluster maintenance command.
func newClusterMaintenanceCommand() *cobra.Command {
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
		Args: cobra.ExactArgs(1),
		RunE: runClusterMaintenance,
	}

	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runClusterMaintenance(cmd *cobra.Command, args []string) error {
	var enabled bool
	switch args[0] {
	case "true", "1", "yes", "on", "enable":
		enabled = true
	case "false", "0", "no", "off", "disable":
		enabled = false
	default:
		return fmt.Errorf("expected 'true' or 'false', got %q", args[0])
	}

	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	action := "Enabling"
	if !enabled {
		action = "Disabling"
	}
	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("%s maintenance mode...", action))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_SetMaintenanceMode{
				SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{
					Enabled: enabled,
				},
			},
		},
	}

	if err := signRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")
		return err
	}

	_, err = client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		spinner.Fail(fmt.Sprintf("Failed to %s maintenance mode", strings.ToLower(action)))
		return formatGRPCError("failed to update maintenance mode", err)
	}

	if enabled {
		spinner.Success("Maintenance mode enabled")
	} else {
		spinner.Success("Maintenance mode disabled")
	}

	return nil
}
