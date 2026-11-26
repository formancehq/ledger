package indexbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// Exercise both log dispatchers and the fast backfill's wire decoder/writer.
// Historical membership outlives an individual EPHEMERAL volume cell, while
// current has-asset membership must still exclude that cell.
func TestHistoricalAddressMappingsSurvivePurgedCells(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"live", "replay", "fast-postings"} {
		for _, eviction := range []string{"draining", "same-log-net-zero", "compensating"} {
			t.Run(mode+"/"+eviction, func(t *testing.T) {
				t.Parallel()
				b := newTestBuilderWithStore(t)
				cfg := acctAssetConfig()
				for _, role := range []commonpb.TransactionBuiltinIndex{
					commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS,
					commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS,
					commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS,
				} {
					id := indexes.TxBuiltinID(role)
					cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}
				}
				postings := []*commonpb.Posting{
					{Source: "hold", Destination: "normal", Asset: "USD", Color: "red"},
					{Source: "normal", Destination: "hold", Asset: "EUR", Color: "blue"},
					{Source: "transient", Destination: "normal", Asset: "USD"},
					{Source: "transient", Destination: "normal", Asset: "USD", Color: "blue"},
					{Source: "transient-only-source", Destination: "transient-only-destination", Asset: "USD"},
				}
				log := makeCreatedTxLog(10, "test", 9, postings)
				if eviction == "compensating" {
					log = makeRevertedTxLog(10, "test", 8, 9, postings)
				}
				ll := log.GetPayload().GetApply().GetLog()
				cell := &commonpb.TouchedVolume{Account: "hold", Asset: "USD", Color: "red"}
				if eviction == "same-log-net-zero" {
					ll.EphemeralVolumes = []*commonpb.TouchedVolume{cell}
				} else {
					ll.PurgedVolumes = []*commonpb.TouchedVolume{cell}
				}
				proposals := newTestAppliedProposalSync(testAppliedProposal(1, 10, 10, "test", "transient", "transient-only-source", "transient-only-destination"))
				b.initBatch(b.readStore.NewBatch())
				b.wb.SetEventSequence(10)
				switch mode {
				case "live":
					require.NoError(t, b.indexPayload(b.kb, cfg, "test", ll.GetData().GetPayload(), proposals.excludedForLog(10, "test", ll)))
				case "replay":
					require.NoError(t, b.indexLogEntry(cfg, log, proposals))
				case "fast-postings":
					data, err := log.MarshalVT()
					require.NoError(t, err)
					var parsed parsedLog
					require.NoError(t, parsePostingsFromLog(data, &parsed))
					excluded := proposals.excludedForLog(parsed.Sequence, parsed.Ledger, &parsed)
					for _, p := range parsed.Postings {
						require.NoError(t, b.indexPostingAddressMappings(b.kb, cfg, parsed.Ledger, parsed.TxID, p.Source, p.Destination, p.Asset, p.Color, true, true, true, excluded))
					}
				}
				require.NoError(t, proposals.err())
				require.NoError(t, b.wb.Flush())
				for _, prefix := range []byte{readstore.PrefixAccountTx, readstore.PrefixSourceAccountTx, readstore.PrefixDestinationAccountTx} {
					require.True(t, readStoreKeyExists(t, b.readStore, readstore.AccountTxKey(dal.NewKeyBuilder(), prefix, "test", "hold", 9)), "purged source must retain history in bucket %x", prefix)
				}
				require.NotContains(t, scanAccountByAsset(t, b.readStore, "test", "USD", 0), "hold")
				require.Contains(t, scanAccountByAsset(t, b.readStore, "test", "EUR", 0), "hold")
				for _, address := range []string{"transient-only-source", "transient-only-destination"} {
					for _, prefix := range []byte{readstore.PrefixAccountTx, readstore.PrefixSourceAccountTx, readstore.PrefixDestinationAccountTx} {
						require.False(t, readStoreKeyExists(t, b.readStore, readstore.AccountTxKey(dal.NewKeyBuilder(), prefix, "test", address, 9)), "TRANSIENT-only address %s must not have history in bucket %x", address, prefix)
					}
					require.NotContains(t, scanAccountByAsset(t, b.readStore, "test", "USD", 0), address)
				}
				// The blue cell on the TRANSIENT address remains historical: the
				// proposal exclusion is specific to the uncolored USD cell.
				require.True(t, readStoreKeyExists(t, b.readStore, readstore.AccountTxKey(dal.NewKeyBuilder(), readstore.PrefixSourceAccountTx, "test", "transient", 9)))
				require.Contains(t, scanAccountByAsset(t, b.readStore, "test", "USD", 0), "transient")
			})
		}
	}
}
