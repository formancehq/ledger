package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/stretchr/testify/require"
)

// Simple mock implementation for testing
type mockSystemControllerForBucketList struct {
	system.Controller
	buckets []system.BucketWithLedgers
}

func (m *mockSystemControllerForBucketList) ListBuckets(ctx context.Context) ([]system.BucketWithLedgers, error) {
	return m.buckets, nil
}

func TestListBuckets(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		// Create mock data
		mockBuckets := []system.BucketWithLedgers{
			{
				Name:            "bucket1",
				Ledgers:         []string{"ledger1", "ledger2"},
				MarkForDeletion: false,
			},
			{
				Name:            "bucket2",
				Ledgers:         []string{"ledger3"},
				MarkForDeletion: true,
			},
		}

		// Create a mock system controller
		mockSystemController := &mockSystemControllerForBucketList{
			buckets: mockBuckets,
		}

		// Create a request
		req := httptest.NewRequest(http.MethodGet, "/_system/bucket", nil)
		w := httptest.NewRecorder()

		// Call the handler
		listBuckets(mockSystemController)(w, req)

		// Check the response
		require.Equal(t, http.StatusOK, w.Code)

		// Print the response for debugging
		fmt.Printf("Response JSON: %s\n", w.Body.String())

		// Parse the response
		var response struct {
			Data struct {
				Data []BucketInfo `json:"data"`
			} `json:"data"`
		}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		require.NoError(t, err)

		// Verify the response data
		require.Len(t, response.Data.Data, 2)
		require.Equal(t, "bucket1", response.Data.Data[0].Name)
		require.Equal(t, []string{"ledger1", "ledger2"}, response.Data.Data[0].Ledgers)
		require.False(t, response.Data.Data[0].MarkForDeletion)

		require.Equal(t, "bucket2", response.Data.Data[1].Name)
		require.Equal(t, []string{"ledger3"}, response.Data.Data[1].Ledgers)
		require.True(t, response.Data.Data[1].MarkForDeletion)
	})
}
