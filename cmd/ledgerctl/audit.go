package main

import (
	"github.com/spf13/cobra"
)

// newAuditCommand creates the audit parent command.
func newAuditCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "audit",
		Aliases: []string{"a"},
		Short:   "Audit log operations",
		Long:    "Commands for viewing the replicated audit log via gRPC",
	}

	cmd.AddCommand(newAuditListCommand())

	return cmd
}
