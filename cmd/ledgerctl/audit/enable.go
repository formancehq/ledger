package audit

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewEnableCommand creates the audit enable command.
func NewEnableCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "enable",
		Short: "Enable audit logging",
		Long: `Enable audit logging on the server.

When enabled, all Raft proposals are recorded in the audit log.
This command supports --signing-key for signed requests.

Examples:
  # Enable audit logging
  ledgerctl audit enable

  # Enable audit logging with signed request
  ledgerctl audit enable --signing-key /path/to/seed`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSetAuditConfig(cmd, true)
		},
	}

	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

// NewDisableCommand creates the audit disable command.
func NewDisableCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "disable",
		Short: "Disable audit logging",
		Long: `Disable audit logging on the server.

When disabled, proposals are no longer recorded in the audit log.
This command supports --signing-key for signed requests.

Examples:
  # Disable audit logging
  ledgerctl audit disable

  # Disable audit logging with signed request
  ledgerctl audit disable --signing-key /path/to/seed`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSetAuditConfig(cmd, false)
		},
	}

	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runSetAuditConfig(cmd *cobra.Command, enabled bool) error {
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

	spinner, _ := pterm.DefaultSpinner.Start(action + " audit logging...")

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_SetAuditConfig{
				SetAuditConfig: &servicepb.SetAuditConfigRequest{
					Enabled: enabled,
				},
			},
		},
	}

	if err := cmdutil.SignRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	_, err = client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to update audit config", err)
	}

	if enabled {
		spinner.Success("Audit logging enabled")
	} else {
		spinner.Success("Audit logging disabled")
	}

	return nil
}
