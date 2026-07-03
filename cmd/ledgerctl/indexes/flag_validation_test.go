package indexes

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestRejectMetadataOnlyFlags(t *testing.T) {
	t.Parallel()

	newCmd := func() *cobra.Command {
		cmd := &cobra.Command{}
		cmd.Flags().String("target", "", "")
		cmd.Flags().String("key", "", "")

		return cmd
	}

	t.Run("metadata accepts target and key", func(t *testing.T) {
		t.Parallel()

		cmd := newCmd()
		require.NoError(t, cmd.Flags().Set("target", "account"))
		require.NoError(t, cmd.Flags().Set("key", "address"))
		require.NoError(t, rejectMetadataOnlyFlags(cmd, "metadata"))
	})

	t.Run("non-metadata without extras is ok", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, rejectMetadataOnlyFlags(newCmd(), "address"))
	})

	// The confusing case: user tries to scope a builtin address index to
	// accounts. There is no ACCT_BUILTIN_INDEX_ADDRESS in the proto, so the
	// old CLI silently issued the transaction-scoped one and reported success.
	t.Run("target rejected for non-metadata", func(t *testing.T) {
		t.Parallel()

		cmd := newCmd()
		require.NoError(t, cmd.Flags().Set("target", "account"))
		require.ErrorContains(t, rejectMetadataOnlyFlags(cmd, "address"), "--target is only valid with --type metadata")
	})

	t.Run("key rejected for non-metadata", func(t *testing.T) {
		t.Parallel()

		cmd := newCmd()
		require.NoError(t, cmd.Flags().Set("key", "address"))
		require.ErrorContains(t, rejectMetadataOnlyFlags(cmd, "reference"), "--key is only valid with --type metadata")
	})
}
