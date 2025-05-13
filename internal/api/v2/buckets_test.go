package v2

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/controller/system"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDeleteBucket(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type testCase struct {
		name               string
		bucket             string
		returnErr          error
		expectedStatusCode int
		expectedErrorCode  string
	}

	for _, tc := range []testCase{
		{
			name:   "success",
			bucket: "test-bucket",
		},
		{
			name:               "not found",
			bucket:             "test-bucket",
			returnErr:          system.ErrLedgerNotFound,
			expectedStatusCode: http.StatusNotFound,
			expectedErrorCode:  api.ErrorCodeNotFound,
		},
		{
			name:               "internal error",
			bucket:             "test-bucket",
			returnErr:          errors.New("internal error"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorCode:  api.ErrorInternal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			systemController, _ := newTestingSystemController(t, false)
			systemController.EXPECT().
				MarkBucketAsDeleted(gomock.Any(), tc.bucket).
				Return(tc.returnErr)

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodDelete, "/_/buckets/"+tc.bucket, nil)
			req = req.WithContext(ctx)

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if tc.expectedStatusCode == 0 || tc.expectedStatusCode == http.StatusNoContent {
				require.Equal(t, http.StatusNoContent, rec.Code)
			} else {
				require.Equal(t, tc.expectedStatusCode, rec.Code)
				errorResponse := api.ErrorResponse{}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errorResponse))
				require.Equal(t, tc.expectedErrorCode, errorResponse.ErrorCode)
			}
		})
	}
}

func TestRestoreBucket(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type testCase struct {
		name               string
		bucket             string
		returnErr          error
		expectedStatusCode int
		expectedErrorCode  string
	}

	for _, tc := range []testCase{
		{
			name:   "success",
			bucket: "test-bucket",
		},
		{
			name:               "not found",
			bucket:             "test-bucket",
			returnErr:          system.ErrLedgerNotFound,
			expectedStatusCode: http.StatusNotFound,
			expectedErrorCode:  api.ErrorCodeNotFound,
		},
		{
			name:               "internal error",
			bucket:             "test-bucket",
			returnErr:          errors.New("internal error"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorCode:  api.ErrorInternal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			systemController, _ := newTestingSystemController(t, false)
			systemController.EXPECT().
				RestoreBucket(gomock.Any(), tc.bucket).
				Return(tc.returnErr)

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodPost, "/_/buckets/"+tc.bucket+"/restore", nil)
			req = req.WithContext(ctx)

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if tc.expectedStatusCode == 0 || tc.expectedStatusCode == http.StatusNoContent {
				require.Equal(t, http.StatusNoContent, rec.Code)
			} else {
				require.Equal(t, tc.expectedStatusCode, rec.Code)
				errorResponse := api.ErrorResponse{}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errorResponse))
				require.Equal(t, tc.expectedErrorCode, errorResponse.ErrorCode)
			}
		})
	}
}

func TestListBuckets(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type testCase struct {
		name               string
		returnData         []system.BucketWithStatus
		returnErr          error
		expectedStatusCode int
		expectedErrorCode  string
	}

	for _, tc := range []testCase{
		{
			name: "nominal",
			returnData: []system.BucketWithStatus{
				{
					Name:      "bucket1",
					DeletedAt: nil,
				},
				{
					Name:      "bucket2",
					DeletedAt: pointer.For(time.Now()),
				},
			},
		},
		{
			name:               "error from backend",
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorCode:  api.ErrorInternal,
			returnErr:          errors.New("undefined error"),
		},
		{
			name:               "with invalid query from core point of view",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  common.ErrValidation,
			returnErr:          storagecommon.ErrInvalidQuery{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			systemController, _ := newTestingSystemController(t, false)

			systemController.EXPECT().
				ListBucketsWithStatus(gomock.Any()).
				Return(tc.returnData, tc.returnErr)

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodGet, "/_/buckets", nil)
			req = req.WithContext(ctx)

			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if tc.expectedStatusCode == 0 || tc.expectedStatusCode == http.StatusOK {
				require.Equal(t, http.StatusOK, rec.Code)
				var buckets []system.BucketWithStatus
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &buckets))
				require.Equal(t, tc.returnData, buckets)
			} else {
				require.Equal(t, tc.expectedStatusCode, rec.Code)
				errorResponse := api.ErrorResponse{}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errorResponse))
				require.Equal(t, tc.expectedErrorCode, errorResponse.ErrorCode)
			}
		})
	}
}
