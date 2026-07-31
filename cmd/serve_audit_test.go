package cmd

import (
	"testing"

	"github.com/formancehq/go-libs/v5/pkg/audit"
	"github.com/stretchr/testify/require"
)

func TestAuditDefaultsToDisabled(t *testing.T) {
	cmd := NewServeCommand()

	enabled, err := cmd.Flags().GetBool(audit.AuditEnabledFlag)
	require.NoError(t, err)
	require.False(t, enabled, "HTTP audit must stay opt-in")
}
