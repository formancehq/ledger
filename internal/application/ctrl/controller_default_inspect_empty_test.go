package ctrl

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// TestInspectIndexEmptyIndexAnswersRequestedArm pins the response arm to the
// requested mode for an index that is built but holds no live value. The arm
// is how a client tells "empty index" from "the server answered a different
// question", so an empty scan must still answer on the arm that was asked for.
func TestInspectIndexEmptyIndexAnswersRequestedArm(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		mode   ledgerpb.InspectIndexMode
		assert func(t *testing.T, resp *ledgerpb.InspectIndexResponse)
	}{
		{
			name: "distinct values",
			mode: ledgerpb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES,
			assert: func(t *testing.T, resp *ledgerpb.InspectIndexResponse) {
				t.Helper()
				require.IsType(t, &ledgerpb.InspectIndexResponse_DistinctValues{}, resp.GetResult())
				require.Empty(t, resp.GetDistinctValues().GetValues())
				require.False(t, resp.GetDistinctValues().GetHasMore())
				require.Empty(t, resp.GetDistinctValues().GetNextCursor())
			},
		},
		{
			name: "facets",
			mode: ledgerpb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS,
			assert: func(t *testing.T, resp *ledgerpb.InspectIndexResponse) {
				t.Helper()
				require.IsType(t, &ledgerpb.InspectIndexResponse_Facets{}, resp.GetResult())
				require.Empty(t, resp.GetFacets().GetFacets())
				require.False(t, resp.GetFacets().GetHasMore())
				require.Empty(t, resp.GetFacets().GetNextCursor())
			},
		},
		{
			name: "summary",
			mode: ledgerpb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY,
			assert: func(t *testing.T, resp *ledgerpb.InspectIndexResponse) {
				t.Helper()
				require.IsType(t, &ledgerpb.InspectIndexResponse_Summary{}, resp.GetResult())
				require.Zero(t, resp.GetSummary().GetCardinality())
				require.Zero(t, resp.GetSummary().GetEntitiesWithKey())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl, ledger, metadataKey, horizon := newEmptyIndexController(t)

			resp, err := ctrl.InspectIndex(query.WithReadBarrierHorizon(t.Context(), horizon), &ledgerpb.InspectIndexRequest{
				Ledger:      ledger,
				TargetType:  ledgerpb.TargetType_TARGET_TYPE_TRANSACTION,
				MetadataKey: metadataKey,
				Mode:        tc.mode,
				PageSize:    100,
			})
			require.NoError(t, err)
			tc.assert(t, resp)
		})
	}
}

// newEmptyIndexController builds a controller whose ledger declares a
// transaction metadata field and whose read store has the matching index built
// at version 1, with no metadata event ever written for it.
func newEmptyIndexController(t *testing.T) (*DefaultController, string, string, uint64) {
	t.Helper()

	const (
		ledger       = "inspect-empty"
		metadataKey  = "k2"
		mainSequence = uint64(20)
		raftHorizon  = uint64(42)
	)

	logger := logging.NopZap()
	meter := noop.NewMeterProvider().Meter("test")
	store := newCtrlTestStore(t)
	attrs := attributes.New()
	indexID := indexes.MetadataID(ledgerpb.TargetType_TARGET_TYPE_TRANSACTION, metadataKey)

	mainBatch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(mainBatch, ledger, &ledgerpb.LedgerInfo{
		Name: ledger,
		MetadataSchema: &ledgerpb.MetadataSchema{TransactionFields: map[string]*ledgerpb.MetadataFieldSchema{
			metadataKey: {Type: ledgerpb.MetadataType_METADATA_TYPE_UINT32},
		}},
	}))
	_, err := attrs.Index.Set(mainBatch, indexes.KeyFor(ledger, indexID).Bytes(), &ledgerpb.Index{
		Id:                     indexID,
		Ledger:                 ledger,
		ForwardEncodingVersion: 1,
	})
	require.NoError(t, err)
	require.NoError(t, state.AppendLogs(mainBatch, []*ledgerpb.Log{{Sequence: mainSequence}}))
	require.NoError(t, state.SetAppliedIndex(mainBatch, raftHorizon))
	require.NoError(t, mainBatch.Commit())

	rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	indexBatch := rs.NewBatch()
	require.NoError(t, rs.WriteIndexVersionState(indexBatch, ledger, indexes.Canonical(indexID), readstore.IndexVersionState{
		CurrentVersion: 1,
		HighWater:      1,
	}))
	require.NoError(t, rs.WriteProgress(indexBatch, mainSequence))
	require.NoError(t, rs.WriteRaftProgress(indexBatch, raftHorizon))
	require.NoError(t, indexBatch.Commit())
	rs.NotifyProgress()

	return NewDefaultController(nil, store, logger, attrs, rs, nil, meter), ledger, metadataKey, raftHorizon
}
