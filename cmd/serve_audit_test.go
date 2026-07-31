package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/audit"
)

func TestAuditDefaultsToDisabled(t *testing.T) {
	cmd := NewServeCommand()

	enabled, err := cmd.Flags().GetBool(audit.AuditEnabledFlag)
	require.NoError(t, err)
	require.False(t, enabled, "HTTP audit must stay opt-in")
}
