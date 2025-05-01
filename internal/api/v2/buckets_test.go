package v2

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDeleteBucket(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	systemController := NewMockSystemController(ctrl)
	systemController.EXPECT().
		MarkBucketAsDeleted(gomock.Any(), "test-bucket").
		Return(nil)

	r := chi.NewRouter()
	r.Delete("/_/buckets/{bucket}", deleteBucket(systemController))

	req, err := http.NewRequest("DELETE", "/_/buckets/test-bucket", nil)
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusNoContent, rr.Code)
}

func TestRestoreBucket(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	systemController := NewMockSystemController(ctrl)
	systemController.EXPECT().
		RestoreBucket(gomock.Any(), "test-bucket").
		Return(nil)

	r := chi.NewRouter()
	r.Post("/_/buckets/{bucket}/restore", restoreBucket(systemController))

	req, err := http.NewRequest("POST", "/_/buckets/test-bucket/restore", nil)
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusNoContent, rr.Code)
}

func TestListBuckets(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	buckets := []system.BucketWithStatus{
		{
			Name:      "bucket1",
			DeletedAt: time.Time{},
		},
		{
			Name:      "bucket2",
			DeletedAt: time.Now(),
		},
	}

	systemController := NewMockSystemController(ctrl)
	systemController.EXPECT().
		ListBucketsWithStatus(gomock.Any()).
		Return(buckets, nil)

	r := chi.NewRouter()
	r.Get("/_/buckets", listBuckets(systemController))

	req, err := http.NewRequest("GET", "/_/buckets", nil)
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
}
