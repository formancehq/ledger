package main

import (
	"github.com/spf13/cobra"
)

// newSigningCommand creates the signing parent command.
func newSigningCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "signing",
		Aliases: []string{"sign", "keys"},
		Short:   "Manage request signing keys and configuration",
		Long:    "Commands for managing Ed25519 signing keys and signature requirements via gRPC",
	}

	cmd.AddCommand(newSigningGenerateKeyCommand())
	cmd.AddCommand(newSigningRegisterKeyCommand())
	cmd.AddCommand(newSigningRevokeKeyCommand())
	cmd.AddCommand(newSigningRequireCommand())

	return cmd
}
