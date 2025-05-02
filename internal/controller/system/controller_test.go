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

func TestMarkBucketAsDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := NewMockStore(ctrl)
	store.EXPECT().
		MarkBucketAsDeleted(gomock.Any(), "test-bucket").
		Return(nil)

	controller := NewDefaultController(store, nil)
	err := controller.MarkBucketAsDeleted(context.Background(), "test-bucket")
	require.NoError(t, err)
}

func TestRestoreBucket(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := NewMockStore(ctrl)
	store.EXPECT().
		RestoreBucket(gomock.Any(), "test-bucket").
		Return(nil)

	controller := NewDefaultController(store, nil)
	err := controller.RestoreBucket(context.Background(), "test-bucket")
	require.NoError(t, err)
}

func TestListBucketsWithStatus(t *testing.T) {
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

func TestGetLedgerWithDeletedBucket(t *testing.T) {
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
