//go:build ee

package ee

import (
	"github.com/spf13/cobra"
)

// Config holds all EE-specific configuration
type Config struct {
	Audit AuditConfig
}

// AuditConfig holds audit configuration (EE only)
type AuditConfig struct {
	Enabled          bool
	MaxBodySize      int64
	ExcludedPaths    []string
	SensitiveHeaders []string
}

// LoadConfig loads EE configuration from command flags
func LoadConfig(cmd *cobra.Command) (*Config, error) {
	cfg := &Config{}

	// Load audit config
	var err error
	cfg.Audit.Enabled, err = cmd.Flags().GetBool(AuditEnabledFlag)
	if err != nil {
		return nil, err
	}

	cfg.Audit.MaxBodySize, _ = cmd.Flags().GetInt64(AuditMaxBodySizeFlag)
	cfg.Audit.ExcludedPaths, _ = cmd.Flags().GetStringSlice(AuditExcludedPathsFlag)
	cfg.Audit.SensitiveHeaders, _ = cmd.Flags().GetStringSlice(AuditSensitiveHeadersFlag)

	return cfg, nil
}
