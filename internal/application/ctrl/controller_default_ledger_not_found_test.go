package ctrl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestDefaultController_LedgerNotFound asserts that the read paths gated on
// ledger existence map the internal domain.ErrNotFound sentinel returned by
// query.GetLedgerByName to a typed *domain.ErrLedgerNotFound (Reason
// LEDGER_NOT_FOUND -> HTTP 404), for both a missing and a soft-deleted target
// ledger.
func TestDefaultController_LedgerNotFound(t *testing.T) {
	t.Parallel()

	const ledger = "missing-ledger"

	seedNothing := func(*testing.T, *dal.Store) {}

	seedSoftDeleted := func(t *testing.T, store *dal.Store) {
		batch := store.OpenWriteSession()
		require.NoError(t, state.SaveLedger(batch, ledger, &ledgerpb.LedgerInfo{
			Name:      ledger,
			CreatedAt: ledgerpb.NewTimestamp(libtime.Now()),
			DeletedAt: ledgerpb.NewTimestamp(libtime.Now()),
		}))
		require.NoError(t, batch.Commit())
	}

	call := map[string]func(ctrl *DefaultController) error{
		"GetLedgerStats": func(ctrl *DefaultController) error {
			_, err := ctrl.GetLedgerStats(context.Background(), ledger)

			return err
		},
		"GetNumscript": func(ctrl *DefaultController) error {
			_, err := ctrl.GetNumscript(context.Background(), ledger, "script", "")

			return err
		},
		"GetTemplateUsage": func(ctrl *DefaultController) error {
			_, err := ctrl.GetTemplateUsage(context.Background(), ledger, "script")

			return err
		},
		"ListNumscripts": func(ctrl *DefaultController) error {
			_, err := ctrl.ListNumscripts(context.Background(), ledger)

			return err
		},
		"ListPreparedQueries": func(ctrl *DefaultController) error {
			_, err := ctrl.ListPreparedQueries(context.Background(), ledger)

			return err
		},
	}

	for _, seedTC := range []struct {
		name string
		seed func(t *testing.T, store *dal.Store)
	}{
		{name: "missing ledger", seed: seedNothing},
		{name: "soft-deleted ledger", seed: seedSoftDeleted},
	} {
		for method, invoke := range call {
			t.Run(method+"/"+seedTC.name, func(t *testing.T) {
				t.Parallel()

				store := newCtrlTestStore(t)
				attrs := attributes.New()
				logger := logging.FromContext(logging.TestingContext())
				meter := noop.NewMeterProvider().Meter("test")

				ctrl := NewDefaultController(nil, store, logger, attrs, nil, nil, meter)

				seedTC.seed(t, store)

				err := invoke(ctrl)
				require.Error(t, err)

				var notFound *domain.ErrLedgerNotFound
				require.ErrorAs(t, err, &notFound)
				require.Equal(t, ledger, notFound.Name)
			})
		}
	}
}

func newCtrlTestStore(t *testing.T) *dal.Store {
	t.Helper()

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	// Best-effort test cleanup: nothing to assert on close.
	t.Cleanup(func() { _ = store.Close() })

	return store
}
