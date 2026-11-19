package store

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// NewIncrementalBackupCommand creates the store incremental-backup command.
func NewIncrementalBackupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "incremental-backup",
		Aliases:           []string{"ibk"},
		Short:             "Deprecated alias for store backup",
		Long:              "Deprecated alias for store backup; creates a complete Pebble checkpoint.",
		Hidden:            true,
		RunE:              runBackup,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("path", "", "Reserved for future use")
	cmd.Flags().String("bucket-id", "", "Namespace prefix for backup files (default: cluster-id)")
	cmdutil.AddBackupStorageFlags(cmd)
	cmd.Flags().Duration("timeout", 10*cmdutil.DefaultTimeout, "Request timeout")
	cmdutil.AddOutputFlags(cmd)

	return cmd
}
