package cmd

import (
	"time"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/otlp/otlptraces"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/spf13/cobra"
)

func NewBucketDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete buckets that were marked for deletion before the specified duration",
		RunE: func(cmd *cobra.Command, args []string) error {
			durationStr, _ := cmd.Flags().GetString("duration")
			duration, err := time.ParseDuration(durationStr)
			if err != nil {
				return err
			}
			
			days := int(duration.Hours() / 24)
			
			return withStorageDriver(cmd, func(driver *driver.Driver) error {
				deletedBuckets, err := driver.GetBucketsMarkedForDeletion(cmd.Context(), days)
				if err != nil {
					return err
				}
				
				for _, bucket := range deletedBuckets {
					if err := driver.PhysicallyDeleteBucket(cmd.Context(), bucket); err != nil {
						return err
					}
					cmd.Printf("Bucket %s physically deleted\n", bucket)
				}
				
				return nil
			})
		},
	}
	
	cmd.Flags().String("duration", "720h", "Delete buckets marked for deletion before this duration (default 30 days)")
	bunconnect.AddFlags(cmd.Flags())
	otlp.AddFlags(cmd.Flags())
	otlptraces.AddFlags(cmd.Flags())
	
	return cmd
}
