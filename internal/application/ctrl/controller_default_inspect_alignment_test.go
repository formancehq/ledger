package ctrl

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestInspectIndexUsesRoutedBarrierAndMainSnapshotHorizon(t *testing.T) {
	t.Parallel()

	const (
		ledger       = "inspect-alignment"
		metadataKey  = "tier"
		mainSequence = uint64(20)
		raftHorizon  = uint64(42)
	)

	logger := logging.NopZap()
	meter := noop.NewMeterProvider().Meter("test")
	store := newCtrlTestStore(t)
	attrs := attributes.New()
	indexID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, metadataKey)

	mainBatch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(mainBatch, ledger, &commonpb.LedgerInfo{
		Name: ledger,
		MetadataSchema: &commonpb.MetadataSchema{AccountFields: map[string]*commonpb.MetadataFieldSchema{
			metadataKey: {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
		}},
	}))
	_, err := attrs.Index.Set(mainBatch, indexes.KeyFor(ledger, indexID).Bytes(), &commonpb.Index{
		Id:                     indexID,
		Ledger:                 ledger,
		ForwardEncodingVersion: 1,
	})
	require.NoError(t, err)
	require.NoError(t, state.AppendLogs(mainBatch, []*commonpb.Log{{Sequence: mainSequence}}))
	require.NoError(t, state.SetAppliedIndex(mainBatch, raftHorizon))
	require.NoError(t, mainBatch.Commit())

	rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	gold := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("gold"))
	silver := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("silver"))
	kb := dal.NewKeyBuilder()
	indexBatch := rs.NewBatch()
	require.NoError(t, indexBatch.SetBytes(readstore.MetadataIndexEventKeyV(
		kb, ledger, readstore.NamespaceAccount, metadataKey, 1, gold, []byte("alice"), 10, readstore.MetadataEventAdd,
	), nil))
	require.NoError(t, indexBatch.SetBytes(readstore.MetadataIndexEventKeyV(
		kb, ledger, readstore.NamespaceAccount, metadataKey, 1, gold, []byte("alice"), 30, readstore.MetadataEventDel,
	), nil))
	require.NoError(t, indexBatch.SetBytes(readstore.MetadataIndexEventKeyV(
		kb, ledger, readstore.NamespaceAccount, metadataKey, 1, silver, []byte("alice"), 30, readstore.MetadataEventAdd,
	), nil))
	require.NoError(t, rs.WriteIndexVersionState(indexBatch, ledger, indexes.Canonical(indexID), readstore.IndexVersionState{
		CurrentVersion: 1,
		HighWater:      1,
	}))
	require.NoError(t, rs.WriteProgress(indexBatch, 30))
	require.NoError(t, rs.WriteRaftProgress(indexBatch, raftHorizon))
	require.NoError(t, indexBatch.Commit())
	rs.NotifyProgress()

	ctrl := NewDefaultController(nil, store, logger, attrs, rs, nil, meter)
	resp, err := ctrl.InspectIndex(query.WithReadBarrierHorizon(t.Context(), raftHorizon), &servicepb.InspectIndexRequest{
		Ledger:      ledger,
		TargetType:  commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		MetadataKey: metadataKey,
		Mode:        servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES,
		PageSize:    10,
	})
	require.NoError(t, err)
	values := resp.GetDistinctValues().GetValues()
	require.Len(t, values, 1)
	require.Equal(t, "gold", values[0].GetStringValue(),
		"membership committed after the main snapshot must stay invisible")
}
