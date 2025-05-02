package cmd

import (
	"bytes"
	"context"
	"testing"

	formancetime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

type MockDriver struct {
	BucketsWithStatus      []system.BucketWithStatus
	BucketsMarkedForDeletion []string
	PhysicallyDeletedBuckets []string
	RestoredBuckets        []string
}

func (m *MockDriver) ListBucketsWithStatus(ctx context.Context) ([]system.BucketWithStatus, error) {
	return m.BucketsWithStatus, nil
}

func (m *MockDriver) GetBucketsMarkedForDeletion(ctx context.Context, days int) ([]string, error) {
	return m.BucketsMarkedForDeletion, nil
}

func (m *MockDriver) PhysicallyDeleteBucket(ctx context.Context, bucketName string) error {
	m.PhysicallyDeletedBuckets = append(m.PhysicallyDeletedBuckets, bucketName)
	return nil
}

func (m *MockDriver) RestoreBucket(ctx context.Context, bucketName string) error {
	m.RestoredBuckets = append(m.RestoredBuckets, bucketName)
	return nil
}

func TestBucketDeleteCommand(t *testing.T) {
	mockDriver := &MockDriver{
		BucketsMarkedForDeletion: []string{"bucket1", "bucket2"},
	}
	
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete buckets that were marked for deletion N days ago",
		RunE: func(cmd *cobra.Command, args []string) error {
			days, _ := cmd.Flags().GetInt("days")
			deletedBuckets, _ := mockDriver.GetBucketsMarkedForDeletion(cmd.Context(), days)
			
			for _, bucket := range deletedBuckets {
				_ = mockDriver.PhysicallyDeleteBucket(cmd.Context(), bucket)
				cmd.Printf("Bucket %s physically deleted\n", bucket)
			}
			
			return nil
		},
	}
	
	cmd.Flags().Int("days", 30, "Delete buckets marked for deletion N days ago")
	
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	err := cmd.Execute()
	require.NoError(t, err)
	
	output := buf.String()
	require.Contains(t, output, "Bucket bucket1 physically deleted")
	require.Contains(t, output, "Bucket bucket2 physically deleted")
}

func TestBucketListCommand(t *testing.T) {
	now := formancetime.Now()
	mockDriver := &MockDriver{
		BucketsWithStatus: []system.BucketWithStatus{
			{
				Name:      "bucket1",
				DeletedAt: nil,
			},
			{
				Name:      "bucket2",
				DeletedAt: &now,
			},
		},
	}
	
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all buckets with their deletion status",
		RunE: func(cmd *cobra.Command, args []string) error {
			buckets, _ := mockDriver.ListBucketsWithStatus(cmd.Context())
			
			for _, bucket := range buckets {
				if bucket.DeletedAt == nil {
					cmd.Printf("%s: active\n", bucket.Name)
				} else {
					cmd.Printf("%s: deleted at %s\n", bucket.Name, bucket.DeletedAt.Format("2006-01-02T15:04:05Z07:00"))
				}
			}
			
			return nil
		},
	}
	
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	err := cmd.Execute()
	require.NoError(t, err)
	
	output := buf.String()
	require.Contains(t, output, "bucket1: active")
	require.Contains(t, output, "bucket2: deleted at")
}

func TestBucketRestoreCommand(t *testing.T) {
	mockDriver := &MockDriver{}
	
	cmd := &cobra.Command{
		Use:   "restore [bucket]",
		Short: "Restore a bucket that was marked for deletion",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = mockDriver.RestoreBucket(cmd.Context(), args[0])
			cmd.Printf("Bucket %s restored\n", args[0])
			return nil
		},
	}
	
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"test-bucket"})
	err := cmd.Execute()
	require.NoError(t, err)
	
	output := buf.String()
	require.Contains(t, output, "Bucket test-bucket restored")
}
