package v2

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/adapter/v2/celrewrite"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func assetMapping(source, asset, color string) *commonpb.MirrorAssetMapping {
	return &commonpb.MirrorAssetMapping{SourceAsset: source, Asset: asset, Color: color}
}

func mustAssetMapper(t *testing.T, mappings ...*commonpb.MirrorAssetMapping) *AssetMapper {
	t.Helper()

	m, err := NewAssetMapper(mappings)
	require.NoError(t, err)

	return m
}

func mustRewriter(t *testing.T, rules ...*commonpb.MirrorRewriteRule) *celrewrite.Rewriter {
	t.Helper()

	r, err := celrewrite.NewRewriter(rules)
	require.NoError(t, err)

	return r
}

func newTxLog(t *testing.T, logID, txID uint64, assets ...string) V2Log {
	t.Helper()

	postings := make([]V2Posting, 0, len(assets))
	for _, asset := range assets {
		postings = append(postings, V2Posting{Source: "world", Destination: "users:001", Amount: "100", Asset: asset})
	}

	return V2Log{
		ID:   logID,
		Type: "NEW_TRANSACTION",
		Date: "2023-11-14T22:13:20Z",
		Data: mustMarshal(t, V2NewTransactionData{Transaction: V2Transaction{ID: txID, Postings: postings}}),
	}
}

func TestNewAssetMapper_Validation(t *testing.T) {
	t.Parallel()

	tooMany := make([]*commonpb.MirrorAssetMapping, MaxAssetMappings+1)
	for i := range tooMany {
		tooMany[i] = assetMapping(fmt.Sprintf("SRC_%d", i), fmt.Sprintf("T%d", i), "")
	}

	tests := []struct {
		name     string
		mappings []*commonpb.MirrorAssetMapping
		wantErr  string
	}{
		{"too many", tooMany, "too many asset mappings"},
		{"empty source", []*commonpb.MirrorAssetMapping{assetMapping("", "USD", "")}, "sourceAsset must not be empty"},
		{"long source", []*commonpb.MirrorAssetMapping{assetMapping(strings.Repeat("A", MaxSourceAssetLength+1), "USD", "")}, "exceeds 64 bytes"},
		{"duplicate source", []*commonpb.MirrorAssetMapping{assetMapping("USD/0", "USD", ""), assetMapping("USD/0", "USDZERO", "")}, `duplicate sourceAsset "USD/0"`},
		{"invalid target asset", []*commonpb.MirrorAssetMapping{assetMapping("USD_RED/2", "USD_RED/2", "")}, `asset "USD_RED/2"`},
		{"zero precision target", []*commonpb.MirrorAssetMapping{assetMapping("USD/0", "USD/0", "")}, `asset "USD/0"`},
		{"invalid color", []*commonpb.MirrorAssetMapping{assetMapping("USD_red/2", "USD/2", "red")}, `color "red"`},
		{"duplicate target", []*commonpb.MirrorAssetMapping{assetMapping("USD_RED/2", "USD/2", "RED"), assetMapping("USD_R/2", "USD/2", "RED")}, `already used by sourceAsset "USD_RED/2"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, err := NewAssetMapper(tt.mappings)
			require.ErrorContains(t, err, tt.wantErr)
			require.Nil(t, m)
		})
	}

	t.Run("empty list", func(t *testing.T) {
		t.Parallel()

		m, err := NewAssetMapper(nil)
		require.NoError(t, err)
		require.Nil(t, m)
	})

	t.Run("same asset different colors", func(t *testing.T) {
		t.Parallel()

		_, err := NewAssetMapper([]*commonpb.MirrorAssetMapping{
			assetMapping("USDT_ETHEREUM/6", "USDT/6", "ETHEREUM"),
			assetMapping("USDT_TRON/6", "USDT/6", "TRON"),
		})
		require.NoError(t, err)
	})
}

func TestTranslateBatch_AssetMapping_CreatedTransaction(t *testing.T) {
	t.Parallel()

	m := mustAssetMapper(t,
		assetMapping("USDT_ETHEREUM/6", "USDT/6", "ETHEREUM"),
		assetMapping("USD/0", "USDZERO", ""),
	)

	orders, _, _, err := TranslateBatch("default", []V2Log{newTxLog(t, 1, 0, "USDT_ETHEREUM/6", "USD/0", "USD/2")}, 1, 0, TranslateOptions{Assets: m})
	require.NoError(t, err)
	require.Len(t, orders, 1)

	postings := orders[0].GetLedgerScoped().GetMirrorIngest().GetEntry().GetCreatedTransaction().GetPostings()
	require.Len(t, postings, 3)
	require.Equal(t, "USDT/6", postings[0].GetAsset())
	require.Equal(t, "ETHEREUM", postings[0].GetColor())
	require.Equal(t, "USDZERO", postings[1].GetAsset())
	require.Empty(t, postings[1].GetColor())
	require.Equal(t, "USD/2", postings[2].GetAsset(), "valid unmapped assets pass through")
	require.Empty(t, postings[2].GetColor())
}

func TestTranslateBatch_AssetMapping_RevertedTransaction(t *testing.T) {
	t.Parallel()

	m := mustAssetMapper(t, assetMapping("USDT_ETHEREUM/6", "USDT/6", "ETHEREUM"))

	v2Logs := []V2Log{{
		ID:   3,
		Type: "REVERTED_TRANSACTION",
		Date: "2023-11-14T22:14:00Z",
		Data: mustMarshal(t, V2RevertedTransactionData{
			RevertedTransaction: V2TransactionIdentity{ID: new(uint64(1))},
			RevertTransaction: V2Transaction{
				ID:       5,
				Postings: []V2Posting{{Source: "users:001", Destination: "world", Amount: "100", Asset: "USDT_ETHEREUM/6"}},
			},
		}),
	}}

	orders, _, _, err := TranslateBatch("default", v2Logs, 3, 1, TranslateOptions{Assets: m})
	require.NoError(t, err)

	reverse := orders[0].GetLedgerScoped().GetMirrorIngest().GetEntry().GetRevertedTransaction().GetReversePostings()
	require.Len(t, reverse, 1)
	require.Equal(t, "USDT/6", reverse[0].GetAsset())
	require.Equal(t, "ETHEREUM", reverse[0].GetColor())
}

func TestTranslateBatch_AssetMapping_Rejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mapper  []*commonpb.MirrorAssetMapping
		asset   string
		wantErr string
	}{
		{"unmapped underscore asset", nil, "USDT_ETHEREUM/6", `no mapping for v2 asset "USDT_ETHEREUM/6"`},
		{"zero precision", nil, "USD/0", `no mapping for v2 asset "USD/0"`},
		{"leading zero precision", nil, "USD/02", `no mapping for v2 asset "USD/02"`},
		{"precision above 255", nil, "USD/1000", `no mapping for v2 asset "USD/1000"`},
		{
			"pass-through collides with uncolored target",
			[]*commonpb.MirrorAssetMapping{assetMapping("USD/0", "USD", "")},
			"USD",
			`pass-through asset "USD" collides with mapping target ("USD", "") of sourceAsset "USD/0"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var m *AssetMapper
			if tt.mapper != nil {
				m = mustAssetMapper(t, tt.mapper...)
			}

			orders, nextLog, nextTx, err := TranslateBatch("default", []V2Log{newTxLog(t, 7, 0, tt.asset)}, 7, 0, TranslateOptions{Assets: m})
			require.ErrorContains(t, err, "mapping assets of v2 log 7 (type=NEW_TRANSACTION)")
			require.ErrorContains(t, err, tt.wantErr)
			var mappingErr *AssetMappingError
			require.ErrorAs(t, err, &mappingErr)
			require.Equal(t, tt.asset, mappingErr.Asset)
			require.Nil(t, orders)
			require.Zero(t, nextLog)
			require.Zero(t, nextTx)
		})
	}
}

func TestTranslateBatch_AssetMapping_ColoredTargetDoesNotBlockUncolored(t *testing.T) {
	t.Parallel()

	m := mustAssetMapper(t, assetMapping("USDT_ETHEREUM/6", "USDT/6", "ETHEREUM"))

	orders, _, _, err := TranslateBatch("default", []V2Log{newTxLog(t, 1, 0, "USDT/6", "USDT_ETHEREUM/6")}, 1, 0, TranslateOptions{Assets: m})
	require.NoError(t, err)

	postings := orders[0].GetLedgerScoped().GetMirrorIngest().GetEntry().GetCreatedTransaction().GetPostings()
	require.Equal(t, [2]string{"USDT/6", ""}, [2]string{postings[0].GetAsset(), postings[0].GetColor()})
	require.Equal(t, [2]string{"USDT/6", "ETHEREUM"}, [2]string{postings[1].GetAsset(), postings[1].GetColor()})
}

func TestTranslateBatch_AssetMapping_DropRuleSkipsMapping(t *testing.T) {
	t.Parallel()

	r := mustRewriter(t, createdRule(`log.postings.exists(p, p.asset == "USDT_ETHEREUM/6")`, dropCreated()))

	orders, _, _, err := TranslateBatch("default", []V2Log{newTxLog(t, 1, 0, "USDT_ETHEREUM/6")}, 1, 0, TranslateOptions{Rewriter: r})
	require.NoError(t, err, "a dropped transaction never reaches asset validation")
	require.Len(t, orders, 1)
	require.NotNil(t, orders[0].GetLedgerScoped().GetMirrorIngest().GetEntry().GetFillGap())
}

func TestTranslateBatch_AssetMapping_MetadataUntouched(t *testing.T) {
	t.Parallel()

	m := mustAssetMapper(t, assetMapping("USDT_ETHEREUM/6", "USDT/6", "ETHEREUM"))

	orders, _, _, err := TranslateBatch("default", []V2Log{{
		ID:   1,
		Type: "SET_METADATA",
		Date: "2023-11-14T22:13:20Z",
		Data: mustMarshal(t, V2SetMetadataData{TargetType: "ACCOUNT", TargetID: []byte(`"users:001"`), Metadata: map[string]string{"k": "USDT_ETHEREUM/6"}}),
	}}, 1, 0, TranslateOptions{Assets: m})
	require.NoError(t, err)
	require.Len(t, orders, 1)

	entry := orders[0].GetLedgerScoped().GetMirrorIngest().GetEntry()
	require.IsType(t, &raftcmdpb.MirrorLogEntry_SavedMetadata{}, entry.GetData())
}
