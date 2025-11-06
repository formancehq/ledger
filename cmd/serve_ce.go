//go:build !ee

package cmd

import (
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

// getEEModules returns EE-specific FX modules (CE stub)
func getEEModules(cmd *cobra.Command) ([]fx.Option, error) {
	// CE: No EE modules
	return nil, nil
}

// injectEEMiddleware injects EE-specific middleware (CE stub)
func injectEEMiddleware() fx.Option {
	// CE: No EE middleware
	return fx.Options()
}
