package ledger

import (
	"errors"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/mock/gomock"
	"testing"
)

func TestNewControllerWithTooManyClientHandling(t *testing.T) {
	t.Parallel()

	t.Run("finally passing", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		underlyingLedgerController := NewMockController(ctrl)
		delayCalculator := NewMockDelayCalculator(ctrl)
		ctx := logging.TestingContext()

		parameters := Parameters[RunScript]{}

		underlyingLedgerController.EXPECT().
			CreateTransaction(gomock.Any(), parameters).
			Return(nil, postgres.ErrTooManyClient{}).
			Times(2)

		underlyingLedgerController.EXPECT().
			CreateTransaction(gomock.Any(), parameters).
			Return(&ledger.CreatedTransaction{
				Transaction: ledger.NewTransaction(),
			}, nil)

		delayCalculator.EXPECT().
			Next(0).
			Return(time.Millisecond)

		delayCalculator.EXPECT().
			Next(1).
			Return(10 * time.Millisecond)

		ledgerController := NewControllerWithTooManyClientHandling(underlyingLedgerController, noop.Tracer{}, delayCalculator)
		_, err := ledgerController.CreateTransaction(ctx, parameters)
		require.NoError(t, err)
	})

	t.Run("finally failing", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		underlyingLedgerController := NewMockController(ctrl)
		delayCalculator := NewMockDelayCalculator(ctrl)
		ctx := logging.TestingContext()

		parameters := Parameters[RunScript]{}

		underlyingLedgerController.EXPECT().
			CreateTransaction(gomock.Any(), parameters).
			Return(nil, postgres.ErrTooManyClient{}).
			Times(2)

		delayCalculator.EXPECT().
			Next(0).
			Return(time.Millisecond)

		delayCalculator.EXPECT().
			Next(1).
			Return(time.Duration(0))

		ledgerController := NewControllerWithTooManyClientHandling(underlyingLedgerController, noop.Tracer{}, delayCalculator)
		_, err := ledgerController.CreateTransaction(ctx, parameters)
		require.Error(t, err)
		require.True(t, errors.Is(err, postgres.ErrTooManyClient{}))
	})
}
