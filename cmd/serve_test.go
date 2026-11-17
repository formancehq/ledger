package cmd

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/service"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func loadServeConfig(t *testing.T, args ...string) *ServeCommandConfig {
	t.Helper()

	cmd := NewServeCommand()
	require.NoError(t, cmd.ParseFlags(args))
	service.BindEnvToFlagSet(cmd.Flags())

	cfg, err := LoadConfig[ServeCommandConfig](cmd)
	require.NoError(t, err)

	return cfg
}

func TestServeConfigSchemaEnforcementMode(t *testing.T) {
	t.Run("default is audit", func(t *testing.T) {
		cfg := loadServeConfig(t)
		require.Equal(t, ledgercontroller.SchemaEnforcementAudit, cfg.SchemaEnforcementMode)
		require.NoError(t, cfg.Validate())
	})

	t.Run("empty env var keeps the default", func(t *testing.T) {
		t.Setenv("SCHEMA_ENFORCEMENT_MODE", "")
		cfg := loadServeConfig(t)
		require.Equal(t, ledgercontroller.SchemaEnforcementAudit, cfg.SchemaEnforcementMode)
		require.NoError(t, cfg.Validate())
	})

	for _, mode := range []string{"audit", "strict"} {
		t.Run("flag "+mode, func(t *testing.T) {
			cfg := loadServeConfig(t, "--"+SchemaEnforcementMode, mode)
			require.Equal(t, ledgercontroller.SchemaEnforcementMode(mode), cfg.SchemaEnforcementMode)
			require.NoError(t, cfg.Validate())
		})
	}

	t.Run("env var strict", func(t *testing.T) {
		t.Setenv("SCHEMA_ENFORCEMENT_MODE", "strict")
		cfg := loadServeConfig(t)
		require.Equal(t, ledgercontroller.SchemaEnforcementStrict, cfg.SchemaEnforcementMode)
		require.NoError(t, cfg.Validate())
	})

	for _, mode := range []string{"Strict", "STRICT", "stric", ""} {
		t.Run("flag invalid "+mode, func(t *testing.T) {
			cfg := loadServeConfig(t, "--"+SchemaEnforcementMode+"="+mode)
			require.EqualError(t, cfg.Validate(),
				`invalid schema-enforcement-mode "`+mode+`": expected "audit" or "strict"`)
		})
	}

	t.Run("env var invalid", func(t *testing.T) {
		t.Setenv("SCHEMA_ENFORCEMENT_MODE", "Strict")
		cfg := loadServeConfig(t)
		require.EqualError(t, cfg.Validate(),
			`invalid schema-enforcement-mode "Strict": expected "audit" or "strict"`)
	})

	t.Run("serve refuses to start", func(t *testing.T) {
		cmd := NewServeCommand()
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs([]string{"--" + SchemaEnforcementMode, "Strict"})
		require.EqualError(t, cmd.Execute(),
			`invalid schema-enforcement-mode "Strict": expected "audit" or "strict"`)
	})
}
