//go:build !ee

package ee

import (
	"github.com/spf13/cobra"
)

// Config holds EE-specific configuration (CE stub)
type Config struct{}

// LoadConfig loads EE configuration (CE stub - returns empty config)
func LoadConfig(cmd *cobra.Command) (*Config, error) {
	return &Config{}, nil
}

// AddFlags adds EE-specific flags (CE stub - does nothing)
func AddFlags(cmd *cobra.Command) {
	// CE: No EE flags
}
