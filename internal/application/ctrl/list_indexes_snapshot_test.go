package ctrl

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// The SCOPE_ALL listing stitches a registry scan with a liveness check per
// owning ledger. Both must observe the same commit: the scan is pinned when
// the cursor opens, so a liveness check reading the live store can drop an
// entry whose ledger was deleted after the scan began, while an index created
// in that window is never seen. Found by the Antithesis model driver: the
// result below was empty, a set no committed state ever held.
func TestListIndexes_LivenessChecksShareScanSnapshot(t *testing.T) {
	t.Parallel()

	const (
		ledgerA = "ledger-a"
		ledgerB = "ledger-b"
	)

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")
	store := newCtrlTestStore(t)
	attrs := attributes.New()

	k0 := indexes.MetadataID(ledgerpb.TargetType_TARGET_TYPE_TRANSACTION, "k0")
	k1 := indexes.MetadataID(ledgerpb.TargetType_TARGET_TYPE_TRANSACTION, "k1")

	batch := store.OpenWriteSession()
	for _, name := range []string{ledgerA, ledgerB} {
		require.NoError(t, state.SaveLedger(batch, name, &ledgerpb.LedgerInfo{
			Name:      name,
			CreatedAt: ledgerpb.NewTimestamp(libtime.Now()),
		}))
	}
	seedIndex(t, attrs, batch, ledgerB, k0)
	require.NoError(t, batch.Commit())

	ctrl := NewDefaultController(nil, store, logger, attrs, nil, nil, meter)

	c, err := ctrl.ListIndexes(t.Context(), &servicepb.ListIndexesRequest{
		Scope: servicepb.ListIndexesRequest_SCOPE_ALL,
	})
	require.NoError(t, err)

	// Two commits land between opening the cursor and draining it, in the
	// same shape as two applied proposals.
	batch = store.OpenWriteSession()
	seedIndex(t, attrs, batch, ledgerA, k1)
	require.NoError(t, batch.Commit())

	batch = store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, ledgerB, &ledgerpb.LedgerInfo{
		Name:      ledgerB,
		CreatedAt: ledgerpb.NewTimestamp(libtime.Now()),
		DeletedAt: ledgerpb.NewTimestamp(libtime.Now()),
	}))
	require.NoError(t, state.DeleteLedgerData(batch, ledgerB))
	require.NoError(t, batch.Commit())

	got, err := cursor.Collect(c)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, ledgerB, got[0].GetLedger())
	require.Equal(t, indexes.Canonical(k0), indexes.Canonical(got[0].GetId()))
}

func seedIndex(t *testing.T, attrs *attributes.Attributes, batch *dal.WriteSession, ledger string, id *ledgerpb.IndexID) {
	t.Helper()

	_, err := attrs.Index.Set(batch, indexes.KeyFor(ledger, id).Bytes(), &ledgerpb.Index{
		Id:                     id,
		Ledger:                 ledger,
		ForwardEncodingVersion: 1,
	})
	require.NoError(t, err)
}
