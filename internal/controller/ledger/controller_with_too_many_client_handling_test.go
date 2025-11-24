package ledger

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/time"

	ledger "github.com/formancehq/ledger/internal"
)

func TestNewControllerWithTooManyClientHandling(t *testing.T) {
	t.Parallel()

	t.Run("finally passing", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		underlyingLedgerController := NewMockController(ctrl)
		delayCalculator := NewMockDelayCalculator(ctrl)
		ctx := logging.TestingContext()

		parameters := Parameters[CreateTransaction]{}

		underlyingLedgerController.EXPECT().
			CreateTransaction(gomock.Any(), parameters).
			Return(nil, nil, false, postgres.ErrTooManyClient{}).
			Times(2)

		underlyingLedgerController.EXPECT().
			CreateTransaction(gomock.Any(), parameters).
			Return(&ledger.Log{}, &ledger.CreatedTransaction{
				Transaction: ledger.NewTransaction(),
			}, false, nil)

		delayCalculator.EXPECT().
			Next(0).
			Return(time.Millisecond)

		delayCalculator.EXPECT().
			Next(1).
			Return(10 * time.Millisecond)

		ledgerController := NewControllerWithTooManyClientHandling(underlyingLedgerController, noop.Tracer{}, delayCalculator)
		_, _, _, err := ledgerController.CreateTransaction(ctx, parameters)
		require.NoError(t, err)
	})

	t.Run("finally failing", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		underlyingLedgerController := NewMockController(ctrl)
		delayCalculator := NewMockDelayCalculator(ctrl)
		ctx := logging.TestingContext()

		parameters := Parameters[CreateTransaction]{}

		underlyingLedgerController.EXPECT().
			CreateTransaction(gomock.Any(), parameters).
			Return(nil, nil, false, postgres.ErrTooManyClient{}).
			Times(2)

		delayCalculator.EXPECT().
			Next(0).
			Return(time.Millisecond)

		delayCalculator.EXPECT().
			Next(1).
			Return(time.Duration(0))

		ledgerController := NewControllerWithTooManyClientHandling(underlyingLedgerController, noop.Tracer{}, delayCalculator)
		_, _, _, err := ledgerController.CreateTransaction(ctx, parameters)
		require.Error(t, err)
		require.True(t, errors.Is(err, postgres.ErrTooManyClient{}))
	})
}
