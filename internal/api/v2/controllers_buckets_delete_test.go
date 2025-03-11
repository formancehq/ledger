package v2

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/stretchr/testify/require"
)

func TestDeleteBucket(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		// Create a mock system controller
		mockSystemController := &mockSystemController{
			markBucketAsDeletedFn: func(ctx context.Context, bucketName string) error {
				require.Equal(t, "test-bucket", bucketName)
				return nil
			},
		}

		// Create a request with the bucket name as a query parameter
		req := httptest.NewRequest(http.MethodDelete, "/_system/bucket?name=test-bucket", nil)
		w := httptest.NewRecorder()

		// Call the handler
		deleteBucket(mockSystemController)(w, req)

		// Check the response
		require.Equal(t, http.StatusNoContent, w.Code)
	})

	t.Run("error marking bucket as deleted", func(t *testing.T) {
		// Create a mock system controller with an error
		mockSystemController := &mockSystemController{
			markBucketAsDeletedFn: func(ctx context.Context, bucketName string) error {
				return errors.New("bucket not found")
			},
		}

		// Create a request with the bucket name as a query parameter
		req := httptest.NewRequest(http.MethodDelete, "/_system/bucket?name=test-bucket", nil)
		w := httptest.NewRecorder()

		// Call the handler
		deleteBucket(mockSystemController)(w, req)

		// Check the response
		require.Equal(t, http.StatusBadRequest, w.Code)
	})
}

// Mock system controller for testing
type mockSystemController struct {
	system.Controller
	markBucketAsDeletedFn func(ctx context.Context, bucketName string) error
}

func (m *mockSystemController) MarkBucketAsDeleted(ctx context.Context, bucketName string) error {
	if m.markBucketAsDeletedFn != nil {
		return m.markBucketAsDeletedFn(ctx, bucketName)
	}
	return nil
}
