package wasm

import "github.com/spf13/cobra"

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "wasm", Short: "Manage WASM modules"}
	cmd.AddCommand(newUploadCommand(), newListCommand(), newGetCommand(), newStopCommand(), newDeleteCommand())
	return cmd
}
