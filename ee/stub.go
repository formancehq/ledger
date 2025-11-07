//go:build !ee

package ee

import (
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

// AddFlags is a no-op for Community Edition builds
func AddFlags(_ *cobra.Command) {
	// CE stub - no EE flags to add
}

// Module returns an empty Fx module for Community Edition builds
func Module(_ *cobra.Command) fx.Option {
	return fx.Options()
}
