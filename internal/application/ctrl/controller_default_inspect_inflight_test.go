package ctrl

import (
	"context"
	"testing"

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

// InspectIndex scans the served version directly, so it must apply the same
// hold-off as the query resolver: a promotion whose flush has not completed
// is not yet servable and reads as INDEX_BUILDING.
func TestDefaultController_InspectIndex_RefusesAPromotionInFlight(t *testing.T) {
	t.Parallel()

	const (
		ledger = "test"
		key    = "grade"
	)

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")

	store := newCtrlTestStore(t)
	attrs := attributes.New()
	indexID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	canonical := indexes.Canonical(indexID)

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{
		Name: ledger,
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				key: {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
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

	rs.MarkPromotion(ledger, canonical)

	rsBatch := rs.NewBatch()
	require.NoError(t, rs.WriteIndexVersionState(rsBatch, ledger, canonical, readstore.IndexVersionState{
		CurrentVersion:      1,
		HighWater:           1,
		CurrentType:         commonpb.MetadataType_METADATA_TYPE_INT64,
		CurrentTypeDeclared: true,
	}))
	require.NoError(t, rsBatch.Commit())

	c := NewDefaultController(nil, store, logger, attrs, rs, nil, meter)
	req := &servicepb.InspectIndexRequest{
		Ledger:      ledger,
		TargetType:  commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		MetadataKey: key,
	}

	_, err = c.InspectIndex(context.Background(), req)
	var building *domain.ErrIndexBuilding
	require.ErrorAs(t, err, &building, "the promotion is committed but not flushed")

	require.NoError(t, rs.FlushPromotions())

	_, err = c.InspectIndex(context.Background(), req)
	require.NoError(t, err, "flushed, the promotion serves")
}
