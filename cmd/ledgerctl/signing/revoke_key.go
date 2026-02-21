package signing

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewRevokeKeyCommand creates the signing revoke-key command.
func NewRevokeKeyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "revoke-key",
		Aliases: []string{"remove-key", "revoke"},
		Short:   "Revoke a registered signing key",
		Long: `Revoke a registered signing key.

This command must be signed by an existing key (use --signing-key).

Examples:
  ledgerctl signing revoke-key --key-id ops --signing-key /path/to/seed`,
		Args: cobra.NoArgs,
		RunE: runRevokeKey,
	}

	cmd.Flags().String("key-id", "", "Key ID to revoke (required)")
	cmd.Flags().Bool("cascade", false, "Also revoke all descendant keys")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runRevokeKey(cmd *cobra.Command, _ []string) error {
	keyID, _ := cmd.Flags().GetString("key-id")
	if keyID == "" {
		return fmt.Errorf("--key-id is required")
	}
	cascade, _ := cmd.Flags().GetBool("cascade")

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Revoking signing key %s...", keyID))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_RevokeSigningKey{
				RevokeSigningKey: &servicepb.RevokeSigningKeyRequest{
					KeyId:   keyID,
					Cascade: cascade,
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
		spinner.Fail("Failed to revoke signing key")
		return cmdutil.FormatGRPCError("failed to revoke signing key", err)
	}

	spinner.Success("Revoked")

	pterm.Println()
	pterm.Printf("Key ID: %s (revoked)\n", pterm.Gray(keyID))

	return nil
}
