package auth

import "github.com/spf13/cobra"

// NewCommand returns the "auth" parent command for Ed25519 authentication utilities.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Ed25519 authentication utilities",
		Long:  "Commands for managing Ed25519 key-based authentication (generate keys, create tokens)",
	}

	cmd.AddCommand(NewGenerateKeyCommand())
	cmd.AddCommand(NewGenerateTokenCommand())

	return cmd
}
