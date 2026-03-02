package auth

import "github.com/spf13/cobra"

// NewCommand returns the "auth" parent command for authentication utilities.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Authentication utilities",
		Long:  "Commands for managing authentication (generate keys, create tokens, credential storage)",
	}

	cmd.AddCommand(NewGenerateKeyCommand())
	cmd.AddCommand(NewGenerateTokenCommand())
	cmd.AddCommand(NewLoginCommand())
	cmd.AddCommand(NewLogoutCommand())
	cmd.AddCommand(NewStatusCommand())

	return cmd
}
