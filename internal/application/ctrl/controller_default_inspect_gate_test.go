package ctrl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// TestDefaultController_InspectIndex_ServingWindowGate pins InspectIndex to
// the same serving-window decision as the query compile gate: a nonzero
// binding more than one schema revision behind (a rewound read store
// re-walking a retype chain) refuses as INDEX_BUILDING instead of scanning
// the superseded keyspace, while the direct predecessor still serves.
func TestDefaultController_InspectIndex_ServingWindowGate(t *testing.T) {
	t.Parallel()

	const (
		ledger = "test"
		key    = "grade"
	)

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")

	newInspectController := func(t *testing.T, bindingRevision uint32) *DefaultController {
		t.Helper()

		store := newReceiptTestStore(t)

		attrs := attributes.New()
		indexID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)

		batch := store.OpenWriteSession()
		require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{
			Name: ledger,
			MetadataSchema: &commonpb.MetadataSchema{
				AccountFields: map[string]*commonpb.MetadataFieldSchema{
					key: {Type: commonpb.MetadataType_METADATA_TYPE_INT8, Revision: 3},
				},
			},
		}))
		_, err := attrs.Index.Set(batch, indexes.KeyFor(ledger, indexID).Bytes(), &commonpb.Index{
			Id:     indexID,
			Ledger: ledger,
		})
		require.NoError(t, err)
		require.NoError(t, batch.Commit())

		rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = rs.Close() })

		rsBatch := rs.NewBatch()
		require.NoError(t, rs.WriteIndexVersionState(rsBatch, ledger, indexes.Canonical(indexID), readstore.IndexVersionState{
			CurrentVersion:      1,
			CurrentType:         commonpb.MetadataType_METADATA_TYPE_INT32,
			CurrentTypeDeclared: true,
			CurrentRevision:     bindingRevision,
		}))
		require.NoError(t, rsBatch.Commit())

		return NewDefaultController(nil, store, logger, attrs, rs, nil, nil, meter)
	}

	req := &servicepb.InspectIndexRequest{
		Ledger:      ledger,
		TargetType:  commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		MetadataKey: key,
	}

	t.Run("two revisions behind refuses as INDEX_BUILDING", func(t *testing.T) {
		t.Parallel()

		c := newInspectController(t, 1)

		_, err := c.InspectIndex(context.Background(), req)
		var building *domain.ErrIndexBuilding
		require.ErrorAs(t, err, &building)
	})

	t.Run("direct predecessor serves", func(t *testing.T) {
		t.Parallel()

		c := newInspectController(t, 2)

		resp, err := c.InspectIndex(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

// TestDefaultController_InspectIndex_WaitsForFoldAlignment pins the property
// that closes the drop/re-declare rewind hole: InspectIndex reads the version
// state through a snapshot fold-aligned to its main-store handle, exactly as
// the query executor does. A rewound read store therefore cannot expose a
// dropped incarnation's binding to the gate — by the time the fold covers the
// main store's sequence, the DropIndex tombstone and any re-declaration have
// been re-applied. The detector: with the fold cursor behind the main store,
// InspectIndex blocks on alignment (bounded by the caller's context) instead
// of serving the unaligned state.
func TestDefaultController_InspectIndex_WaitsForFoldAlignment(t *testing.T) {
	t.Parallel()

	const (
		ledger = "test"
		key    = "grade"
	)

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")

	store := newReceiptTestStore(t)
	attrs := attributes.New()
	indexID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{
		Name: ledger,
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				key: {Type: commonpb.MetadataType_METADATA_TYPE_STRING, Revision: 1},
			},
		},
	}))
	_, err := attrs.Index.Set(batch, indexes.KeyFor(ledger, indexID).Bytes(), &commonpb.Index{
		Id:     indexID,
		Ledger: ledger,
	})
	require.NoError(t, err)
	// Advance the main store's last log sequence past the (empty) fold
	// cursor, so the read is unaligned until the fold catches up.
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{{Sequence: 1}}))
	require.NoError(t, batch.Commit())

	rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	// The rewound-store shape: the version state still shows a previous
	// incarnation's binding (revision 2 against a schema at revision 1).
	rsBatch := rs.NewBatch()
	require.NoError(t, rs.WriteIndexVersionState(rsBatch, ledger, indexes.Canonical(indexID), readstore.IndexVersionState{
		CurrentVersion:      1,
		CurrentType:         commonpb.MetadataType_METADATA_TYPE_INT32,
		CurrentTypeDeclared: true,
		CurrentRevision:     2,
	}))
	require.NoError(t, rsBatch.Commit())

	c := NewDefaultController(nil, store, logger, attrs, rs, nil, nil, meter)
	req := &servicepb.InspectIndexRequest{
		Ledger:      ledger,
		TargetType:  commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		MetadataKey: key,
	}

	// Unaligned: the call must wait for the fold, not serve the dead binding.
	shortCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err = c.InspectIndex(shortCtx, req)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"an unaligned inspect must block on fold alignment, not serve")

	// Fold caught up: the aligned state is served (the stale binding shape
	// here would be unreachable in production once aligned; the empty scan
	// stands in for the re-folded state).
	progress := rs.NewBatch()
	require.NoError(t, rs.WriteProgress(progress, 1))
	require.NoError(t, progress.Commit())

	rsBatch = rs.NewBatch()
	require.NoError(t, rs.WriteIndexVersionState(rsBatch, ledger, indexes.Canonical(indexID), readstore.IndexVersionState{
		CurrentVersion:      1,
		CurrentType:         commonpb.MetadataType_METADATA_TYPE_STRING,
		CurrentTypeDeclared: true,
		CurrentRevision:     1,
	}))
	require.NoError(t, rsBatch.Commit())

	resp, err := c.InspectIndex(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
}
