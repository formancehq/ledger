package cmd

import (
	"time"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/otlp/otlptraces"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/spf13/cobra"
)

func NewBucketsCommand() *cobra.Command {
	ret := &cobra.Command{
		Use:     "buckets",
		Aliases: []string{"storage"},
	}

	ret.AddCommand(NewBucketUpgrade())
	ret.AddCommand(NewBucketDeleteCommand())
	ret.AddCommand(NewBucketListCommand())
	ret.AddCommand(NewBucketRestoreCommand())
	return ret
}

func addCommonFlags(cmd *cobra.Command) {
	bunconnect.AddFlags(cmd.Flags())
	otlp.AddFlags(cmd.Flags())
	otlptraces.AddFlags(cmd.Flags())
}

func NewBucketDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete buckets that were marked for deletion N days ago",
		RunE: func(cmd *cobra.Command, args []string) error {
			days, _ := cmd.Flags().GetInt("days")
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
	
	cmd.Flags().Int("days", 30, "Delete buckets marked for deletion N days ago")
	addCommonFlags(cmd)
	
	return cmd
}

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
						cmd.Printf("%s: deleted at %s\n", bucket.Name, bucket.DeletedAt.Format(time.RFC3339))
					}
				}
				
				return nil
			})
		},
	}
	
	addCommonFlags(cmd)
	
	return cmd
}

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
	
	addCommonFlags(cmd)
	
	return cmd
}
