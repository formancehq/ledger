package queries

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdateRejectsExplicitlyEmptyFilterBeforeConnecting(t *testing.T) {
	t.Parallel()

	for _, filter := range []string{"", "   \t"} {
		cmd := NewUpdateCommand()
		cmd.SilenceErrors = true
		cmd.SilenceUsage = true
		cmd.SetArgs([]string{"all-accounts", "--ledger", "test", "--filter", filter})

		err := cmd.Execute()
		require.ErrorContains(t, err, "--filter must contain at least one condition")
	}
}
