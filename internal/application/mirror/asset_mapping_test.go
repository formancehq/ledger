package mirror

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func coloredTxLog(id uint64, asset string) v2.V2Log {
	return v2.V2Log{
		ID:   id,
		Type: "NEW_TRANSACTION",
		Date: "2023-11-14T22:13:20Z",
		Data: []byte(`{"transaction":{"id":` + "0" + `,"postings":[{"source":"world","destination":"users:001","amount":100,"asset":"` + asset + `"}],"timestamp":"2023-11-14T22:13:20Z"}}`),
	}
}

func seedVolume(t *testing.T, store *dal.Store, ledgerName, account, asset, color string) {
	t.Helper()

	attrs := attributes.New()
	session := store.OpenWriteSession()
	_, err := attrs.Volume.Set(session, domain.NewVolumeKey(ledgerName, account, asset, color).Bytes(), &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(0),
	})
	require.NoError(t, err)
	require.NoError(t, session.Commit())
}

func TestWorkerAssetMappingRejectsBeforeProposal(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	writeMirrorMetadataPolicy(t, store, mirrorMetadataPolicy())

	source := v2.NewMockSource(gomock.NewController(t))
	source.EXPECT().FetchLogs(gomock.Any(), uint64(0), gomock.Any()).Return([]v2.V2Log{coloredTxLog(1, "USDT_ETHEREUM/6")}, false, nil).Times(2)
	proposer := &stubProposer{outcome: proposeApplied}
	w := newWorkerWithProposer(t, "mirrored", source, store, builder, proposer)

	for range 2 {
		_, err := w.processBatch(context.Background())
		require.ErrorContains(t, err, `mapping assets of v2 log 1 (type=NEW_TRANSACTION): no mapping for v2 asset "USDT_ETHEREUM/6"`)
		var mappingErr *v2.AssetMappingError
		require.ErrorAs(t, err, &mappingErr)
		require.Empty(t, proposer.recorded())
		require.Zero(t, w.lastAppliedV2LogID)
	}
}

func TestWorkerAssetMappingProposesColoredPostingAndCoverage(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	writeMirrorMetadataPolicy(t, store, mirrorMetadataPolicy())

	source := v2.NewMockSource(gomock.NewController(t))
	source.EXPECT().FetchLogs(gomock.Any(), uint64(0), gomock.Any()).Return([]v2.V2Log{coloredTxLog(1, "USDT_ETHEREUM/6")}, false, nil)
	proposer := &stubProposer{outcome: proposeApplied}

	assets, err := v2.NewAssetMapper([]*commonpb.MirrorAssetMapping{{SourceAsset: "USDT_ETHEREUM/6", Asset: "USDT/6", Color: "ETHEREUM"}})
	require.NoError(t, err)
	ctx := logging.TestingContext()
	w := NewWorker("mirrored", 100, source, v2.TranslateOptions{Assets: assets}, store, proposer, builder, logging.FromContext(ctx), noop.NewMeterProvider())

	_, err = w.processBatch(context.Background())
	require.NoError(t, err)
	require.Len(t, proposer.recorded(), 1)

	cmd := proposer.recorded()[0].cmd
	posting := cmd.GetOrders()[0].GetLedgerScoped().GetMirrorIngest().GetEntry().GetCreatedTransaction().GetPostings()[0]
	require.Equal(t, "USDT/6", posting.GetAsset())
	require.Equal(t, "ETHEREUM", posting.GetColor())

	aggregate, _ := w.extractMirrorNeeds(cmd)
	for _, account := range []string{"world", "users:001"} {
		require.True(t, aggregate.Has(dal.SubAttrVolume, domain.NewVolumeKey("mirrored", account, "USDT/6", "ETHEREUM").Bytes()), account)
		require.False(t, aggregate.Has(dal.SubAttrVolume, domain.NewVolumeKey("mirrored", account, "USDT/6", "").Bytes()), account)
	}
}

func TestWorkerStartupInventoryBlocksIngestion(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	seedVolume(t, store, "mirrored", "users:001", "USDT_ETHEREUM/6", "")
	seedVolume(t, store, "mirrored", "users:001", "USD/2", "")

	// gomock fails the test on any FetchLogs call: a blocked worker must not ingest.
	source := v2.NewMockSource(gomock.NewController(t))
	proposer := &stubProposer{outcome: proposeApplied}
	w := newWorkerWithProposer(t, "mirrored", source, store, builder, proposer)

	w.processLogs(context.Background())
	w.processLogs(context.Background())

	recorded := proposer.recorded()
	require.Len(t, recorded, 1, "a confirmed block report is not re-proposed")
	require.Equal(t,
		"ledger contains assets ingested before asset-mapping support: USDT_ETHEREUM/6; delete and recreate the mirror with assetMappings",
		recorded[0].update.GetError().GetMessage(),
	)
}

func TestWorkerStartupInventoryRetriesUnconfirmedReport(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	seedVolume(t, store, "mirrored", "users:001", "USDT_ETHEREUM/6", "")

	source := v2.NewMockSource(gomock.NewController(t))
	proposer := &stubProposer{outcome: proposeRaftReject}
	w := newWorkerWithProposer(t, "mirrored", source, store, builder, proposer)

	w.processLogs(context.Background())
	proposer.setOutcome(proposeApplied)
	w.processLogs(context.Background())
	w.processLogs(context.Background())

	require.Len(t, proposer.recorded(), 2, "the rejected report is retried once, then the confirmed one sticks")
}

func TestWorkerStartupInventoryAllowsValidLedger(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	seedVolume(t, store, "mirrored", "users:001", "USD/2", "")
	seedVolume(t, store, "mirrored", "users:001", "USDT/6", "ETHEREUM")

	source := v2.NewMockSource(gomock.NewController(t))
	source.EXPECT().FetchLogs(gomock.Any(), uint64(0), gomock.Any()).Return(nil, false, nil)
	w := newWorkerForTest(t, "mirrored", source, store, builder)

	w.processLogs(context.Background())

	require.True(t, w.inventoryChecked)
	require.Empty(t, w.inventoryBlock)
}
