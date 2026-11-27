package pebble

import (
	"github.com/cockroachdb/pebble/v2/tool"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/internal/storage/readstore"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
)

// NewCommand exposes Pebble's offline inspection commands through ledgerctl.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pebble",
		Short: "Inspect Pebble files and databases offline",
	}
	cmd.AddCommand(tool.New(tool.Comparers(
		readstore.ReadStoreComparer,
		usagestore.UsageStoreComparer,
	)).Commands...)

	return cmd
}
