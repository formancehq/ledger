package ctrl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
)

func TestDefaultControllerGetSigningConfig(t *testing.T) {
	t.Parallel()

	store := newCtrlTestStore(t)
	ctrl := NewDefaultController(nil, store, logging.NopZap(), attributes.New(), nil, nil, noop.NewMeterProvider().Meter("test"))

	requireSignatures, err := ctrl.GetSigningConfig(context.Background())
	require.NoError(t, err)
	require.False(t, requireSignatures, "an unset policy reads as not required")

	for _, want := range []bool{true, false} {
		batch := store.OpenWriteSession()
		require.NoError(t, state.SaveSigningConfig(batch, want))
		require.NoError(t, batch.Commit())

		requireSignatures, err = ctrl.GetSigningConfig(context.Background())
		require.NoError(t, err)
		require.Equal(t, want, requireSignatures)
	}
}
