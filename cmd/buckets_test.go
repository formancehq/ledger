package cmd

import (
	"bytes"
	"context"
	"testing"

	formancetime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/storage/driver"
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
	
	originalWithStorageDriver := withStorageDriver
	defer func() { withStorageDriver = originalWithStorageDriver }()
	
	withStorageDriver = func(cmd *cobra.Command, fn func(driver *driver.Driver) error) error {
		return fn(mockDriver)
	}
	
	cmd := NewBucketDeleteCommand()
	
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
	
	originalWithStorageDriver := withStorageDriver
	defer func() { withStorageDriver = originalWithStorageDriver }()
	
	withStorageDriver = func(cmd *cobra.Command, fn func(driver *driver.Driver) error) error {
		return fn(mockDriver)
	}
	
	cmd := NewBucketListCommand()
	
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
	
	originalWithStorageDriver := withStorageDriver
	defer func() { withStorageDriver = originalWithStorageDriver }()
	
	withStorageDriver = func(cmd *cobra.Command, fn func(driver *driver.Driver) error) error {
		return fn(mockDriver)
	}
	
	cmd := NewBucketRestoreCommand()
	
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"test-bucket"})
	err := cmd.Execute()
	require.NoError(t, err)
	
	output := buf.String()
	require.Contains(t, output, "Bucket test-bucket restored")
}
