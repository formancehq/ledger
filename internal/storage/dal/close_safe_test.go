package dal_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestCloseSafe(t *testing.T) {
	t.Parallel()

	t.Run("passes the close result through", func(t *testing.T) {
		t.Parallel()

		closeErr := errors.New("close failed")
		require.ErrorIs(t, dal.CloseSafe(func() error { return closeErr }), closeErr)
		require.NoError(t, dal.CloseSafe(func() error { return nil }))
	})

	t.Run("turns a panic into an error", func(t *testing.T) {
		t.Parallel()

		err := dal.CloseSafe(func() error { panic("element has outstanding references") })
		require.ErrorContains(t, err, "panic during DB close (recovered): element has outstanding references")
	})
}
