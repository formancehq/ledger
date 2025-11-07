//go:build ee

package ee

import (
	"github.com/formancehq/go-libs/v3/audit"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

// AddFlags adds all EE-related flags to the command
func AddFlags(cmd *cobra.Command) {
	// Add audit flags
	audit.AddFlags(cmd.Flags())
}

// Module returns the Enterprise Edition Fx module
func Module(cobraCmd *cobra.Command) fx.Option {
	return fx.Options(
		// Provide audit module
		AuditModule(cobraCmd),
	)
}
