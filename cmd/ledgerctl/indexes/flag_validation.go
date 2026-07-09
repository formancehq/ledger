package indexes

import (
	"fmt"

	"github.com/spf13/cobra"
)

// rejectMetadataOnlyFlags surfaces an error when --target or --key was passed
// alongside a non-metadata index type. The switch statements in create.go /
// drop.go used to consume those flags only in the `metadata` case, silently
// dropping them for every other type — letting a caller believe that
// `--type address --target account` produced an account-scoped address index
// even though no such builtin exists (AccountBuiltinIndex only carries
// ACCT_BUILTIN_INDEX_ASSET). Failing fast prevents the misleading "success"
// message that actually re-issued the transaction-scoped address index.
func rejectMetadataOnlyFlags(cmd *cobra.Command, indexType string) error {
	if indexType == "metadata" {
		return nil
	}

	for _, name := range []string{"target", "key"} {
		if cmd.Flags().Changed(name) {
			return fmt.Errorf("--%s is only valid with --type metadata (got --type %s)", name, indexType)
		}
	}

	return nil
}
