package pebble

import (
	"github.com/cockroachdb/pebble/v2/tool"
	"github.com/spf13/cobra"
)

// NewCommand exposes Pebble's offline inspection commands through ledgerctl.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pebble",
		Short: "Inspect Pebble files and databases offline",
	}
	cmd.AddCommand(tool.New().Commands...)

	return cmd
}
