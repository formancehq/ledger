package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/storage/migrations"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

// stubSystemStore is a minimal systemstore.Store for error-path unit tests.
// Methods not exercised by a given test panic to make accidental calls visible.
type stubSystemStore struct {
	updateLedgerMetadataErr error
	deleteLedgerMetadataErr error
	deleteBucketErr         error
	restoreBucketErr        error
	getLedgerFn             func() (*ledger.Ledger, error)
	countLedgersResult      int
	countLedgersInBucketErr error
}

func (s *stubSystemStore) UpdateLedgerMetadata(_ context.Context, _ string, _ metadata.Metadata) error {
	return s.updateLedgerMetadataErr
}

func (s *stubSystemStore) DeleteLedgerMetadata(_ context.Context, _ string, _ string) error {
	return s.deleteLedgerMetadataErr
}

func (s *stubSystemStore) DeleteBucket(_ context.Context, _ string) error {
	return s.deleteBucketErr
}

func (s *stubSystemStore) RestoreBucket(_ context.Context, _ string) error {
	return s.restoreBucketErr
}

func (s *stubSystemStore) GetLedger(_ context.Context, _ string) (*ledger.Ledger, error) {
	if s.getLedgerFn != nil {
		return s.getLedgerFn()
	}
	return nil, nil
}

func (s *stubSystemStore) CountLedgersInBucket(_ context.Context, _ string) (int, error) {
	return s.countLedgersResult, s.countLedgersInBucketErr
}

func (s *stubSystemStore) Ledgers() common.PaginatedResource[ledger.Ledger, systemstore.ListLedgersQueryPayload] {
	panic("Ledgers: not expected in stub")
}

func (s *stubSystemStore) CreateLedger(_ context.Context, _ *ledger.Ledger) error {
	panic("CreateLedger: not expected in stub")
}

func (s *stubSystemStore) GetDistinctBuckets(_ context.Context) ([]string, error) {
	panic("GetDistinctBuckets: not expected in stub")
}

func (s *stubSystemStore) GetDeletedBucketsOlderThan(_ context.Context, _ time.Time) ([]string, error) {
	panic("GetDeletedBucketsOlderThan: not expected in stub")
}

func (s *stubSystemStore) HardDeleteBucket(_ context.Context, _ string) error {
	panic("HardDeleteBucket: not expected in stub")
}

func (s *stubSystemStore) Migrate(_ context.Context, _ ...migrations.Option) error {
	panic("Migrate: not expected in stub")
}

func (s *stubSystemStore) GetMigrator(_ ...migrations.Option) *migrations.Migrator {
	panic("GetMigrator: not expected in stub")
}

func (s *stubSystemStore) IsUpToDate(_ context.Context) (bool, error) {
	panic("IsUpToDate: not expected in stub")
}

// stubStoreFactory always returns the same stubSystemStore, ignoring the db argument.
type stubStoreFactory struct{ store systemstore.Store }

func (f stubStoreFactory) Create(_ bun.IDB) systemstore.Store { return f.store }

// newDriverWithStub builds a Driver with nil bucket/ledger factories — safe for
// code paths that return before reaching those fields.
func newDriverWithStub(store *stubSystemStore) *Driver {
	return New(nil, nil, nil, stubStoreFactory{store: store}, WithCacheTTL(time.Minute))
}

func TestUpdateLedgerMetadataStoreError(t *testing.T) {
	storeErr := errors.New("store unavailable")
	d := newDriverWithStub(&stubSystemStore{updateLedgerMetadataErr: storeErr})
	err := d.UpdateLedgerMetadata(context.Background(), "test-ledger", metadata.Metadata{"k": "v"})
	require.ErrorIs(t, err, storeErr)
}

func TestUpdateLedgerMetadataSuccess(t *testing.T) {
	d := newDriverWithStub(&stubSystemStore{})
	require.NoError(t, d.UpdateLedgerMetadata(context.Background(), "test-ledger", metadata.Metadata{"k": "v"}))
}

func TestDeleteLedgerMetadataStoreError(t *testing.T) {
	storeErr := errors.New("store unavailable")
	d := newDriverWithStub(&stubSystemStore{deleteLedgerMetadataErr: storeErr})
	err := d.DeleteLedgerMetadata(context.Background(), "test-ledger", "k")
	require.ErrorIs(t, err, storeErr)
}

func TestDeleteLedgerMetadataSuccess(t *testing.T) {
	d := newDriverWithStub(&stubSystemStore{})
	require.NoError(t, d.DeleteLedgerMetadata(context.Background(), "test-ledger", "k"))
}

func TestDeleteBucketStoreError(t *testing.T) {
	storeErr := errors.New("store unavailable")
	d := newDriverWithStub(&stubSystemStore{deleteBucketErr: storeErr})
	err := d.DeleteBucket(context.Background(), "bucket-1")
	require.ErrorIs(t, err, storeErr)
}

func TestDeleteBucketSuccess(t *testing.T) {
	d := newDriverWithStub(&stubSystemStore{})
	require.NoError(t, d.DeleteBucket(context.Background(), "bucket-1"))
}

func TestRestoreBucketStoreError(t *testing.T) {
	storeErr := errors.New("store unavailable")
	d := newDriverWithStub(&stubSystemStore{restoreBucketErr: storeErr})
	err := d.RestoreBucket(context.Background(), "bucket-1")
	require.ErrorIs(t, err, storeErr)
}

func TestRestoreBucketSuccess(t *testing.T) {
	d := newDriverWithStub(&stubSystemStore{})
	require.NoError(t, d.RestoreBucket(context.Background(), "bucket-1"))
}

func TestOpenLedgerGetLedgerError(t *testing.T) {
	storeErr := errors.New("ledger not found")
	d := newDriverWithStub(&stubSystemStore{
		getLedgerFn: func() (*ledger.Ledger, error) { return nil, storeErr },
	})
	_, _, err := d.OpenLedger(context.Background(), "missing-ledger")
	require.ErrorIs(t, err, storeErr)
}

func TestOpenLedgerCountLedgersInBucketError(t *testing.T) {
	l := ledger.MustNewWithDefault("test-ledger")
	countErr := errors.New("count query failed")
	d := newDriverWithStub(&stubSystemStore{
		getLedgerFn:             func() (*ledger.Ledger, error) { return &l, nil },
		countLedgersInBucketErr: countErr,
	})
	_, _, err := d.OpenLedger(context.Background(), l.Name)
	require.Error(t, err)
	require.ErrorContains(t, err, countErr.Error())
}

func TestOpenLedgerColdCacheSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	l := ledger.MustNewWithDefault("test-ledger")

	mockBucket := NewMockBucket(ctrl)

	bucketFact := NewBucketFactory(ctrl)
	bucketFact.EXPECT().Create(l.Bucket).Return(mockBucket).AnyTimes()

	ledgerFact := NewLedgerStoreFactory(ctrl)
	ledgerFact.EXPECT().Create(gomock.Any(), gomock.Any()).Return(&ledgerstore.Store{}).AnyTimes()

	stub := &stubSystemStore{
		getLedgerFn:        func() (*ledger.Ledger, error) { return &l, nil },
		countLedgersResult: 1,
	}

	d := New(nil, ledgerFact, bucketFact, stubStoreFactory{store: stub}, WithCacheTTL(time.Minute))

	// Cold call — populates cache.
	_, got, err := d.OpenLedger(context.Background(), l.Name)
	require.NoError(t, err)
	require.Equal(t, l.Name, got.Name)

	// Warm call — exercises the outer cache-hit branch (no DB queries).
	_, got2, err := d.OpenLedger(context.Background(), l.Name)
	require.NoError(t, err)
	require.Equal(t, l.Name, got2.Name)
}

// TestOpenLedgerContextCancelled verifies that a pre-cancelled context causes
// OpenLedger to return context.Canceled without waiting for the DB query to
// finish. A blocking stub ensures the singleflight goroutine is still running
// when the select statement is reached, making the ctx.Done case deterministic.
func TestOpenLedgerContextCancelled(t *testing.T) {
	block := make(chan struct{})

	d := newDriverWithStub(&stubSystemStore{
		getLedgerFn: func() (*ledger.Ledger, error) {
			<-block
			return nil, errors.New("unblocked")
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := d.OpenLedger(ctx, "blocking-ledger")
	require.ErrorIs(t, err, context.Canceled)

	// Unblock the goroutine so the singleflight group can clean up.
	close(block)
}
