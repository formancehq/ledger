package cmd

import (
	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/otlp/otlptraces"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/spf13/cobra"
)

func NewBucketListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all buckets with their deletion status",
		RunE: func(cmd *cobra.Command, args []string) error {
			return withStorageDriver(cmd, func(driver *driver.Driver) error {
				buckets, err := driver.ListBucketsWithStatus(cmd.Context())
				if err != nil {
					return err
				}
				
				for _, bucket := range buckets {
					if bucket.DeletedAt == nil {
						cmd.Printf("%s: active\n", bucket.Name)
					} else {
						cmd.Printf("%s: deleted at %s\n", bucket.Name, bucket.DeletedAt.Format(formancetime.RFC3339))
					}
				}
				
				return nil
			})
		},
	}
	
	bunconnect.AddFlags(cmd.Flags())
	otlp.AddFlags(cmd.Flags())
	otlptraces.AddFlags(cmd.Flags())
	
	return cmd
}
