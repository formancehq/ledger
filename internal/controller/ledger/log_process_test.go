package ledger

import (
	"context"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/pointer"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"testing"
)

func TestForgeLogWithIKConflict(t *testing.T) {

	t.Parallel()
	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)

	store.EXPECT().
		ReadLogWithIdempotencyKey(gomock.Any(), "foo").
		Return(nil, postgres.ErrNotFound)

	store.EXPECT().
		BeginTX(gomock.Any(), gomock.Any()).
		Return(store, &bun.Tx{}, nil)

	store.EXPECT().
		Rollback().
		Return(nil)

	store.EXPECT().
		ReadLogWithIdempotencyKey(gomock.Any(), "foo").
		Return(&ledger.Log{
			Data: ledger.CreatedTransaction{},
		}, nil)

	lp := newLogProcessor[RunScript, ledger.CreatedTransaction]("foo", noop.Int64Counter{})
	_, _, err := lp.forgeLog(ctx, store, Parameters[RunScript]{
		IdempotencyKey: "foo",
	}, func(ctx context.Context, store Store, parameters Parameters[RunScript]) (*ledger.CreatedTransaction, error) {
		return nil, NewErrIdempotencyKeyConflict("foo")
	})
	require.NoError(t, err)
}

func TestForgeLogWithDeadlock(t *testing.T) {

	t.Parallel()
	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)

	// First call returns a deadlock
	store.EXPECT().
		BeginTX(gomock.Any(), gomock.Any()).
		Return(store, &bun.Tx{}, nil)

	store.EXPECT().
		Rollback().
		Return(nil)

	// Second call is ok
	store.EXPECT().
		BeginTX(gomock.Any(), gomock.Any()).
		Return(store, &bun.Tx{}, nil)

	store.EXPECT().
		InsertLog(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, log *ledger.Log) error {
			log.ID = pointer.For(uint64(0))
			return nil
		})

	store.EXPECT().
		Commit().
		Return(nil)

	firstCall := true
	lp := newLogProcessor[RunScript, ledger.CreatedTransaction]("foo", noop.Int64Counter{})
	_, _, err := lp.forgeLog(ctx, store, Parameters[RunScript]{}, func(ctx context.Context, store Store, parameters Parameters[RunScript]) (*ledger.CreatedTransaction, error) {
		if firstCall {
			firstCall = false
			return nil, postgres.ErrDeadlockDetected
		}
		return &ledger.CreatedTransaction{}, nil
	})
	require.NoError(t, err)
}
