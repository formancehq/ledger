package ledger

import (
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
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
		WithTX(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(ErrIdempotencyKeyConflict{})

	store.EXPECT().
		ReadLogWithIdempotencyKey(gomock.Any(), "foo").
		Return(&ledger.Log{
			Data: ledger.CreatedTransaction{},
		}, nil)

	lp := newLogProcessor[RunScript, ledger.CreatedTransaction]("foo", noop.Int64Counter{})
	_, err := lp.forgeLog(ctx, store, Parameters[RunScript]{
		IdempotencyKey: "foo",
	}, nil)
	require.NoError(t, err)
}

func TestForgeLogWithDeadlock(t *testing.T) {

	t.Parallel()
	ctx := logging.TestingContext()
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)

	// First call returns a deadlock
	store.EXPECT().
		WithTX(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(postgres.ErrDeadlockDetected)

	// Second call is ok
	store.EXPECT().
		WithTX(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	lp := newLogProcessor[RunScript, ledger.CreatedTransaction]("foo", noop.Int64Counter{})
	_, err := lp.forgeLog(ctx, store, Parameters[RunScript]{}, nil)
	require.NoError(t, err)
}
