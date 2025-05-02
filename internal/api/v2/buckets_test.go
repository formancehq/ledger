package v2

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDeleteBucket(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		systemController := NewSystemController(ctrl)
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
	})

	t.Run("error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		systemController := NewSystemController(ctrl)
		systemController.EXPECT().
			MarkBucketAsDeleted(gomock.Any(), "test-bucket").
			Return(system.ErrLedgerNotFound)

		r := chi.NewRouter()
		r.Delete("/_/buckets/{bucket}", deleteBucket(systemController))

		req, err := http.NewRequest("DELETE", "/_/buckets/test-bucket", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		require.Equal(t, http.StatusNotFound, rr.Code)
	})
}

func TestRestoreBucket(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		systemController := NewSystemController(ctrl)
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
	})

	t.Run("error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		systemController := NewSystemController(ctrl)
		systemController.EXPECT().
			RestoreBucket(gomock.Any(), "test-bucket").
			Return(system.ErrLedgerNotFound)

		r := chi.NewRouter()
		r.Post("/_/buckets/{bucket}/restore", restoreBucket(systemController))

		req, err := http.NewRequest("POST", "/_/buckets/test-bucket/restore", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		require.Equal(t, http.StatusNotFound, rr.Code)
	})
}

func TestListBuckets(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		now := time.Now()
		var nilTime *time.Time
		buckets := []system.BucketWithStatus{
			{
				Name:      "bucket1",
				DeletedAt: nilTime,
			},
			{
				Name:      "bucket2",
				DeletedAt: &now,
			},
		}

		systemController := NewSystemController(ctrl)
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
	})

	t.Run("error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		systemController := NewSystemController(ctrl)
		systemController.EXPECT().
			ListBucketsWithStatus(gomock.Any()).
			Return(nil, system.ErrLedgerNotFound)

		r := chi.NewRouter()
		r.Get("/_/buckets", listBuckets(systemController))

		req, err := http.NewRequest("GET", "/_/buckets", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		require.Equal(t, http.StatusNotFound, rr.Code)
	})
}
