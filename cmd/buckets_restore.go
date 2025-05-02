package cmd

import (
	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/otlp/otlptraces"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/spf13/cobra"
)

func NewBucketRestoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore [bucket]",
		Short: "Restore a bucket that was marked for deletion",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withStorageDriver(cmd, func(driver *driver.Driver) error {
				err := driver.RestoreBucket(cmd.Context(), args[0])
				if err != nil {
					return err
				}
				
				cmd.Printf("Bucket %s restored\n", args[0])
				return nil
			})
		},
	}
	
	bunconnect.AddFlags(cmd.Flags())
	otlp.AddFlags(cmd.Flags())
	otlptraces.AddFlags(cmd.Flags())
	
	return cmd
}
