package signing

import "github.com/spf13/cobra"

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "signing",
		Aliases: []string{"sign", "keys"},
		Short:   "Manage request signing keys and configuration",
		Long:    "Commands for managing Ed25519 signing keys and signature requirements via gRPC",
	}
	cmd.AddCommand(NewGenerateKeyCommand())
	cmd.AddCommand(NewRegisterKeyCommand())
	cmd.AddCommand(NewRevokeKeyCommand())
	cmd.AddCommand(NewRequireCommand())
	cmd.AddCommand(NewListKeysCommand())
	return cmd
}
