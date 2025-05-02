package system

import (
	"context"
	"errors"
	"testing"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBucketOperations(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		operation     func(controller *DefaultController, ctx context.Context) error
		setupMock     func(store *MockStore)
		expectedError error
	}{
		{
			name: "mark bucket as deleted",
			operation: func(controller *DefaultController, ctx context.Context) error {
				return controller.MarkBucketAsDeleted(ctx, "test-bucket")
			},
			setupMock: func(store *MockStore) {
				store.EXPECT().
					MarkBucketAsDeleted(gomock.Any(), "test-bucket").
					Return(nil)
			},
			expectedError: nil,
		},
		{
			name: "restore bucket",
			operation: func(controller *DefaultController, ctx context.Context) error {
				return controller.RestoreBucket(ctx, "test-bucket")
			},
			setupMock: func(store *MockStore) {
				store.EXPECT().
					RestoreBucket(gomock.Any(), "test-bucket").
					Return(nil)
			},
			expectedError: nil,
		},
	}

	for _, tc := range testCases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := NewMockStore(ctrl)
			tc.setupMock(store)

			controller := NewDefaultController(store, nil)
			err := tc.operation(controller, context.Background())

			if tc.expectedError != nil {
				require.Error(t, err)
				require.Equal(t, tc.expectedError, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBucketsListWithStatus(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	var nilTime *time.Time
	buckets := []BucketWithStatus{
		{
			Name:      "bucket1",
			DeletedAt: nilTime,
		},
		{
			Name:      "bucket2",
			DeletedAt: &now,
		},
	}

	query := common.ColumnPaginatedQuery[any]{
		PageSize: 15,
	}
	
	expectedCursor := bunpaginate.NewCursor(buckets, "", 15, true)
	store := NewMockStore(ctrl)
	store.EXPECT().
		ListBucketsWithStatus(gomock.Any(), query).
		Return(expectedCursor, nil)

	controller := NewDefaultController(store, nil)
	result, err := controller.ListBucketsWithStatus(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, expectedCursor, result)
}

func TestLedgerGet(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		ledger        *ledger.Ledger
		storeError    error
		expectedError error
	}{
		{
			name:          "with store error",
			ledger:        nil,
			storeError:    errors.New("database error"),
			expectedError: errors.New("database error"),
		},
		{
			name: "with active ledger",
			ledger: &ledger.Ledger{
				Name: "test-ledger",
			},
			storeError:    nil,
			expectedError: nil,
		},
	}

	for _, tc := range testCases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := NewMockStore(ctrl)
			store.EXPECT().
				GetLedger(gomock.Any(), "test-ledger").
				Return(tc.ledger, tc.storeError)

			controller := NewDefaultController(store, nil)
			result, err := controller.GetLedger(context.Background(), "test-ledger")

			if tc.expectedError != nil {
				require.Error(t, err)
				require.Equal(t, tc.expectedError.Error(), err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.ledger, result)
			}
		})
	}
}

func TestLedgersList(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := NewMockStore(ctrl)
	store.EXPECT().
		ListLedgers(gomock.Any(), gomock.Any()).
		Return(nil, nil)

	controller := NewDefaultController(store, nil)
	_, err := controller.ListLedgers(context.Background(), common.ColumnPaginatedQuery[any]{})
	require.NoError(t, err)
}

func TestGetLedgerWithDeletedBucket(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	l := &ledger.Ledger{
		Name:      "test-ledger",
		DeletedAt: &now,
	}

	store := NewMockStore(ctrl)
	store.EXPECT().
		GetLedger(gomock.Any(), "test-ledger").
		Return(l, nil)

	controller := NewDefaultController(store, nil)
	_, err := controller.GetLedger(context.Background(), "test-ledger")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrLedgerNotFound)
}

func TestGetLedgerWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	expectedErr := errors.New("database error")
	
	store := NewMockStore(ctrl)
	store.EXPECT().
		GetLedger(gomock.Any(), "test-ledger").
		Return(nil, expectedErr)

	controller := NewDefaultController(store, nil)
	_, err := controller.GetLedger(context.Background(), "test-ledger")
	require.Error(t, err)
	require.Equal(t, expectedErr, err)
}

func TestGetLedgerWithActiveBucket(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := &ledger.Ledger{
		Name:      "test-ledger",
		DeletedAt: nil,
	}

	store := NewMockStore(ctrl)
	store.EXPECT().
		GetLedger(gomock.Any(), "test-ledger").
		Return(l, nil)

	controller := NewDefaultController(store, nil)
	result, err := controller.GetLedger(context.Background(), "test-ledger")
	require.NoError(t, err)
	require.Equal(t, l, result)
}

func TestListLedgersWithDeletedBucket(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := NewMockStore(ctrl)
	store.EXPECT().
		ListLedgers(gomock.Any(), gomock.Any()).
		Return(nil, nil)

	controller := NewDefaultController(store, nil)
	_, err := controller.ListLedgers(context.Background(), common.ColumnPaginatedQuery[any]{})
	require.NoError(t, err)
}
