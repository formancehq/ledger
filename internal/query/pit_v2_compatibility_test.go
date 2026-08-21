package query_test

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	historydomain "github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

type pitV2ReferenceMove struct {
	Compatibility string `json:"compatibility"`
	LedgerID      uint32 `json:"ledgerId"`
	AuditSequence uint64 `json:"auditSequence"`
	LogSequence   uint64 `json:"logSequence"`
	Account       string `json:"account"`
	Asset         string `json:"asset"`
	Color         string `json:"color"`
	Amount        string `json:"amount"`
	IsSource      bool   `json:"isSource"`
	EffectiveAt   uint64 `json:"effectiveAt"`
	InsertedAt    uint64 `json:"insertedAt"`
}

type pitV2FixtureLog struct {
	position historydomain.Position
	log      *commonpb.Log
}

type pitV2VolumeKey struct {
	account string
	asset   string
	color   string
}

type pitV2AssetKey struct {
	asset string
	color string
}

type pitV2Volume struct {
	input  *big.Int
	output *big.Int
}

func TestPITV2CompatibilityResolvedMoves(t *testing.T) {
	t.Parallel()

	reference := pitV2LoadReferenceMoves(t)
	effects := pitV2ReduceFixture(t)

	actual := make([]pitV2ReferenceMove, 0, len(effects))
	for _, effect := range effects {
		actual = append(actual, pitV2MoveFromEffect(t, effect))
	}

	// Compatibility annotations describe the source contract and are not
	// produced by the reducer itself.
	for index := range reference {
		actual[index].Compatibility = reference[index].Compatibility
	}
	require.Equal(t, reference, actual)
}

func TestPITV2CompatibilityNumscriptAndMirrorResolvedTransactions(t *testing.T) {
	t.Parallel()

	effects := pitV2ReduceFixture(t)
	require.Equal(t, []pitV2ReferenceMove{
		{
			LedgerID: 7, AuditSequence: 3, LogSequence: 12,
			Account: "users:alice", Asset: "USD/2", Amount: "30", IsSource: true,
			EffectiveAt: 200, InsertedAt: 520,
		},
		{
			LedgerID: 7, AuditSequence: 3, LogSequence: 12,
			Account: "merchants:one", Asset: "USD/2", Amount: "30",
			EffectiveAt: 200, InsertedAt: 520,
		},
	}, pitV2MovesForAudit(t, effects, 3), "Numscript must be reduced from its resolved transaction log")
	require.Equal(t, []pitV2ReferenceMove{
		{
			LedgerID: 7, AuditSequence: 4, LogSequence: 13,
			Account: "world", Asset: "EUR/4", Amount: "9", IsSource: true,
			EffectiveAt: 250, InsertedAt: 540,
		},
		{
			LedgerID: 7, AuditSequence: 4, LogSequence: 13,
			Account: "users:bob", Asset: "EUR/4", Amount: "9",
			EffectiveAt: 250, InsertedAt: 540,
		},
	}, pitV2MovesForAudit(t, effects, 4), "mirror ingest must be reduced from its resolved transaction log")
}

func TestPITV2CompatibilityEffectiveAndInsertionAxes(t *testing.T) {
	t.Parallel()

	reference := pitV2LoadReferenceMoves(t)
	view := pitV2HistoryView(t, pitV2ReduceFixture(t))

	tests := []struct {
		name     string
		ledgerID uint32
		axis     balancehistorystore.Temporality
		at       uint64
	}{
		{name: "effective before all moves", ledgerID: 7, axis: balancehistorystore.TemporalityEffective, at: 99},
		{name: "effective includes backdated transaction before insertion", ledgerID: 7, axis: balancehistorystore.TemporalityEffective, at: 100},
		{name: "insertion excludes backdated transaction before commit", ledgerID: 7, axis: balancehistorystore.TemporalityInsertion, at: 499},
		{name: "insertion includes backdated transaction at commit boundary", ledgerID: 7, axis: balancehistorystore.TemporalityInsertion, at: 500},
		{name: "effective excludes both sides of at-effective-date reversal before timestamp", ledgerID: 7, axis: balancehistorystore.TemporalityEffective, at: 299},
		{name: "effective includes both sides of at-effective-date reversal atomically", ledgerID: 7, axis: balancehistorystore.TemporalityEffective, at: 300},
		{name: "effective exposes normal transaction before later reversal", ledgerID: 7, axis: balancehistorystore.TemporalityEffective, at: 699},
		{name: "effective includes normal reversal at boundary", ledgerID: 7, axis: balancehistorystore.TemporalityEffective, at: 700},
		{name: "insertion exposes at-effective-date original before reversal commit", ledgerID: 7, axis: balancehistorystore.TemporalityInsertion, at: 799},
		{name: "insertion includes at-effective-date reversal at commit boundary", ledgerID: 7, axis: balancehistorystore.TemporalityInsertion, at: 800},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			wantVolumes := pitV2FoldMoves(reference, test.ledgerID, test.axis, test.at)
			gotVolumes, err := view.ReadVolumes("default", test.axis, test.at, nil)
			require.NoError(t, err)
			require.Equal(t, pitV2ComparableVolumes(wantVolumes), pitV2ComparableStoredVolumes(gotVolumes))

			wantAssets := pitV2AggregateAssets(wantVolumes)
			gotAggregate, err := query.AggregateHistoricalVolumesSelected(
				context.Background(),
				view,
				"default",
				test.axis,
				test.at,
				nil,
				nil,
				nil,
				query.AggregateOptions{},
			)
			require.NoError(t, err)
			require.Equal(t, pitV2ComparableAssets(wantAssets), pitV2ComparableAggregate(gotAggregate))
		})
	}
}

func TestPITV2CompatibilityReversalBalanceBoundaries(t *testing.T) {
	t.Parallel()

	view := pitV2HistoryView(t, pitV2ReduceFixture(t))

	// atEffectiveDate reuses the original effective timestamp. The effective
	// temporality therefore never exposes users:carol between the original and its
	// reversal, while insertion temporality does until the reversal is committed.
	pitV2RequireBalance(t, view, balancehistorystore.TemporalityEffective, 299, "users:carol", "USD/3", "", "0")
	pitV2RequireBalance(t, view, balancehistorystore.TemporalityEffective, 300, "users:carol", "USD/3", "", "0")
	pitV2RequireBalance(t, view, balancehistorystore.TemporalityInsertion, 799, "users:carol", "USD/3", "", "1000")
	pitV2RequireBalance(t, view, balancehistorystore.TemporalityInsertion, 800, "users:carol", "USD/3", "", "0")

	// A normal reversal takes effect at the reversal timestamp.
	pitV2RequireBalance(t, view, balancehistorystore.TemporalityEffective, 699, "users:alice", "USD/2", "", "70")
	pitV2RequireBalance(t, view, balancehistorystore.TemporalityEffective, 700, "users:alice", "USD/2", "", "100")
}

func TestPITV2CompatibilityRetainsEphemeralAndTransientMoves(t *testing.T) {
	t.Parallel()

	view := pitV2HistoryView(t, pitV2ReduceFixture(t))

	// These accounts are zero after their transaction boundary and can be
	// absent from the live volume projection. Their accepted moves remain
	// additive history, matching the historical-balance design contract.
	pitV2RequireVolume(t, view, balancehistorystore.TemporalityEffective, 400, "temp:ephemeral", "GBP", "", "25", "25")
	pitV2RequireVolume(t, view, balancehistorystore.TemporalityEffective, 450, "temp:transient", "JPY", "", "40", "40")
}

func TestPITV2CompatibilityColorAndPrecisionExtensions(t *testing.T) {
	t.Parallel()

	view := pitV2HistoryView(t, pitV2ReduceFixture(t))

	defaultResult, err := query.AggregateHistoricalVolumesSelected(
		context.Background(),
		view,
		"default",
		balancehistorystore.TemporalityInsertion,
		850,
		nil,
		nil,
		nil,
		query.AggregateOptions{},
	)
	require.NoError(t, err)

	usdBuckets := make(map[pitV2AssetKey][2]string)
	for key, volume := range pitV2ComparableAggregate(defaultResult) {
		if key.asset == "USD/2" || key.asset == "USD/3" {
			usdBuckets[key] = volume
		}
	}
	require.Equal(t, map[pitV2AssetKey][2]string{
		{asset: "USD/2", color: ""}:     {"160", "160"},
		{asset: "USD/2", color: "RED"}:  {"11", "11"},
		{asset: "USD/3", color: ""}:     {"2000", "2000"},
		{asset: "USD/3", color: "BLUE"}: {"100", "100"},
	}, usdBuckets)

	merged, err := query.AggregateHistoricalVolumesSelected(
		context.Background(),
		view,
		"default",
		balancehistorystore.TemporalityInsertion,
		850,
		nil,
		nil,
		nil,
		query.AggregateOptions{UseMaxPrecision: true, CollapseColors: true},
	)
	require.NoError(t, err)

	mergedUSD, ok := pitV2ComparableAggregate(merged)[pitV2AssetKey{asset: "USD/3"}]
	require.True(t, ok)
	// 160@/2 + 11@/2 rescale to 1710@/3, then 2000 + 100@/3.
	require.Equal(t, [2]string{"3810", "3810"}, mergedUSD)
}

func TestPITV2CompatibilityUsesTheSelectedCurrentAccountSet(t *testing.T) {
	t.Parallel()

	view := pitV2HistoryView(t, pitV2ReduceFixture(t))
	result, err := query.AggregateHistoricalVolumesSelected(
		context.Background(),
		view,
		"default",
		balancehistorystore.TemporalityInsertion,
		850,
		[]string{"users:alice"},
		nil,
		nil,
		query.AggregateOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, map[pitV2AssetKey][2]string{
		{asset: "USD/2"}: {"130", "30"},
	}, pitV2ComparableAggregate(result))
}

func pitV2LoadReferenceMoves(t *testing.T) []pitV2ReferenceMove {
	t.Helper()

	encoded, err := os.ReadFile("testdata/pit_v2_compatibility/moves.json")
	require.NoError(t, err)

	var moves []pitV2ReferenceMove
	require.NoError(t, json.Unmarshal(encoded, &moves))
	require.NotEmpty(t, moves)

	return moves
}

func pitV2ReduceFixture(t *testing.T) []historydomain.Effect {
	t.Helper()

	reducer := historydomain.NewReducer()
	effects := make([]historydomain.Effect, 0)
	for _, fixture := range pitV2FixtureLogs() {
		logEffects, err := reducer.Reduce(fixture.position, fixture.log)
		require.NoError(t, err)
		effects = append(effects, logEffects...)
	}

	return effects
}

func pitV2FixtureLogs() []pitV2FixtureLog {
	return []pitV2FixtureLog{
		pitV2LifecycleLog(1, 10, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "default", Id: 7}}}),
		pitV2ApplyLog(2, 11, pitV2CreatedTransaction(100, 500, pitV2Posting("world", "users:alice", "USD/2", "", 100))),
		// Numscript and mirror sources are deliberately represented by their
		// resolved transaction logs: that is the authoritative v2/v3 boundary.
		pitV2ApplyLog(3, 12, pitV2CreatedTransaction(200, 520, pitV2Posting("users:alice", "merchants:one", "USD/2", "", 30))),
		pitV2ApplyLog(4, 13, pitV2CreatedTransaction(250, 540, pitV2Posting("world", "users:bob", "EUR/4", "", 9))),
		pitV2ApplyLog(5, 14, pitV2RevertedTransaction(700, 700, pitV2Posting("merchants:one", "users:alice", "USD/2", "", 30))),
		pitV2ApplyLog(6, 15, pitV2CreatedTransaction(300, 550, pitV2Posting("world", "users:carol", "USD/3", "", 1000))),
		pitV2ApplyLog(7, 16, pitV2RevertedTransaction(300, 800, pitV2Posting("users:carol", "world", "USD/3", "", 1000))),
		pitV2ApplyLog(8, 17, pitV2CreatedTransaction(350, 560, pitV2Posting("world", "temp:ephemeral", "GBP", "", 25))),
		pitV2ApplyLog(9, 18, pitV2CreatedTransaction(400, 570, pitV2Posting("temp:ephemeral", "settlement", "GBP", "", 25))),
		pitV2ApplyLog(10, 19, pitV2CreatedTransaction(
			450,
			580,
			pitV2Posting("world", "temp:transient", "JPY", "", 40),
			pitV2Posting("temp:transient", "settlement", "JPY", "", 40),
		)),
		pitV2ApplyLog(11, 20, pitV2CreatedTransaction(460, 590, pitV2Posting("world", "colored:red", "USD/2", "RED", 11))),
		pitV2ApplyLog(12, 21, pitV2CreatedTransaction(470, 600, pitV2Posting("world", "colored:blue", "USD/3", "BLUE", 100))),
	}
}

func pitV2LifecycleLog(audit, sequence uint64, payload *commonpb.LogPayload) pitV2FixtureLog {
	return pitV2FixtureLog{
		position: historydomain.Position{AuditSequence: audit, LogSequence: sequence},
		log: &commonpb.Log{
			Sequence: sequence,
			Payload:  payload,
		},
	}
}

func pitV2ApplyLog(audit, sequence uint64, payload *commonpb.LedgerLogPayload) pitV2FixtureLog {
	return pitV2FixtureLog{
		position: historydomain.Position{AuditSequence: audit, LogSequence: sequence},
		log: &commonpb.Log{
			Sequence: sequence,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
				LedgerName: "default",
				Log:        &commonpb.LedgerLog{Id: sequence, Data: payload},
			}}},
		},
	}
}

func pitV2CreatedTransaction(effective, inserted uint64, postings ...*commonpb.Posting) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
		CreatedTransaction: &commonpb.CreatedTransaction{Transaction: pitV2Transaction(effective, inserted, postings...)},
	}}
}

func pitV2RevertedTransaction(effective, inserted uint64, postings ...*commonpb.Posting) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
		RevertedTransaction: &commonpb.RevertedTransaction{RevertTransaction: pitV2Transaction(effective, inserted, postings...)},
	}}
}

func pitV2Transaction(effective, inserted uint64, postings ...*commonpb.Posting) *commonpb.Transaction {
	return &commonpb.Transaction{
		Postings:   postings,
		Timestamp:  &commonpb.Timestamp{Data: effective},
		InsertedAt: &commonpb.Timestamp{Data: inserted},
	}
}

func pitV2Posting(source, destination, asset, color string, amount uint64) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      source,
		Destination: destination,
		Asset:       asset,
		Color:       color,
		Amount:      commonpb.NewUint256FromUint64(amount),
	}
}

func pitV2MoveFromEffect(t *testing.T, effect historydomain.Effect) pitV2ReferenceMove {
	t.Helper()

	move := pitV2ReferenceMove{
		LedgerID:      7,
		AuditSequence: effect.AuditSequence,
		LogSequence:   effect.LogSequence,
		Account:       effect.Account,
		Asset:         domain.FormatAsset(effect.AssetBase, effect.AssetPrecision),
		Color:         effect.Color,
		EffectiveAt:   effect.EffectiveAt,
		InsertedAt:    effect.InsertedAt,
	}
	switch {
	case !effect.Output.IsZero() && effect.Input.IsZero():
		move.IsSource = true
		move.Amount = effect.Output.BigInt().String()
	case !effect.Input.IsZero() && effect.Output.IsZero():
		move.Amount = effect.Input.BigInt().String()
	default:
		t.Fatalf("effect must map to exactly one v2 move side: %+v", effect)
	}

	return move
}

func pitV2MovesForAudit(t *testing.T, effects []historydomain.Effect, auditSequence uint64) []pitV2ReferenceMove {
	t.Helper()

	ret := make([]pitV2ReferenceMove, 0, 2)
	for _, effect := range effects {
		if effect.AuditSequence == auditSequence {
			ret = append(ret, pitV2MoveFromEffect(t, effect))
		}
	}

	return ret
}

func pitV2HistoryView(t *testing.T, effects []historydomain.Effect) *balancehistorystore.View {
	t.Helper()

	store, err := balancehistorystore.New(t.TempDir(), logging.NopZap(), balancehistorystore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	_, err = store.Publish(balancehistorystore.Publication{
		Effects: effects,
		Coverage: balancehistorystore.Coverage{
			AuditSequence:  15,
			LogSequence:    24,
			SourceComplete: true,
		},
	})
	require.NoError(t, err)

	view, err := store.OpenView(24)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })

	return view
}

func pitV2FoldMoves(
	moves []pitV2ReferenceMove,
	ledgerID uint32,
	axis balancehistorystore.Temporality,
	at uint64,
) map[pitV2VolumeKey]pitV2Volume {
	ret := make(map[pitV2VolumeKey]pitV2Volume)
	for _, move := range moves {
		moveAt := move.EffectiveAt
		if axis == balancehistorystore.TemporalityInsertion {
			moveAt = move.InsertedAt
		}
		if move.LedgerID != ledgerID || moveAt > at {
			continue
		}

		key := pitV2VolumeKey{account: move.Account, asset: move.Asset, color: move.Color}
		volume, ok := ret[key]
		if !ok {
			volume = pitV2Volume{input: new(big.Int), output: new(big.Int)}
		}
		amount, ok := new(big.Int).SetString(move.Amount, 10)
		if !ok {
			panic("invalid checked-in PIT v2 amount")
		}
		if move.IsSource {
			volume.output.Add(volume.output, amount)
		} else {
			volume.input.Add(volume.input, amount)
		}
		ret[key] = volume
	}

	return ret
}

func pitV2AggregateAssets(volumes map[pitV2VolumeKey]pitV2Volume) map[pitV2AssetKey]pitV2Volume {
	ret := make(map[pitV2AssetKey]pitV2Volume)
	for key, volume := range volumes {
		assetKey := pitV2AssetKey{asset: key.asset, color: key.color}
		total, ok := ret[assetKey]
		if !ok {
			total = pitV2Volume{input: new(big.Int), output: new(big.Int)}
		}
		total.input.Add(total.input, volume.input)
		total.output.Add(total.output, volume.output)
		ret[assetKey] = total
	}

	return ret
}

func pitV2ComparableVolumes(volumes map[pitV2VolumeKey]pitV2Volume) map[pitV2VolumeKey][2]string {
	ret := make(map[pitV2VolumeKey][2]string, len(volumes))
	for key, volume := range volumes {
		ret[key] = [2]string{volume.input.String(), volume.output.String()}
	}

	return ret
}

func pitV2ComparableStoredVolumes(volumes []balancehistorystore.Volume) map[pitV2VolumeKey][2]string {
	ret := make(map[pitV2VolumeKey][2]string, len(volumes))
	for _, volume := range volumes {
		ret[pitV2VolumeKey{
			account: volume.Account,
			asset:   domain.FormatAsset(volume.AssetBase, volume.AssetPrecision),
			color:   volume.Color,
		}] = [2]string{volume.Input.String(), volume.Output.String()}
	}

	return ret
}

func pitV2ComparableAssets(volumes map[pitV2AssetKey]pitV2Volume) map[pitV2AssetKey][2]string {
	ret := make(map[pitV2AssetKey][2]string, len(volumes))
	for key, volume := range volumes {
		ret[key] = [2]string{volume.input.String(), volume.output.String()}
	}

	return ret
}

func pitV2ComparableAggregate(result *commonpb.AggregateResult) map[pitV2AssetKey][2]string {
	ret := make(map[pitV2AssetKey][2]string, len(result.GetVolumes()))
	for _, volume := range result.GetVolumes() {
		ret[pitV2AssetKey{asset: volume.GetAsset(), color: volume.GetColor()}] = [2]string{
			volume.GetInput().ToBigInt().String(),
			volume.GetOutput().ToBigInt().String(),
		}
	}

	return ret
}

func pitV2RequireBalance(
	t *testing.T,
	view *balancehistorystore.View,
	axis balancehistorystore.Temporality,
	at uint64,
	account, asset, color, want string,
) {
	t.Helper()

	volume := pitV2FindVolume(t, view, axis, at, account, asset, color)
	require.Equal(t, want, new(big.Int).Sub(volume.Input, volume.Output).String())
}

func pitV2RequireVolume(
	t *testing.T,
	view *balancehistorystore.View,
	axis balancehistorystore.Temporality,
	at uint64,
	account, asset, color, input, output string,
) {
	t.Helper()

	volume := pitV2FindVolume(t, view, axis, at, account, asset, color)
	require.Equal(t, input, volume.Input.String())
	require.Equal(t, output, volume.Output.String())
}

func pitV2FindVolume(
	t *testing.T,
	view *balancehistorystore.View,
	axis balancehistorystore.Temporality,
	at uint64,
	account, asset, color string,
) balancehistorystore.Volume {
	t.Helper()

	volumes, err := view.ReadVolumes("default", axis, at, []string{account})
	require.NoError(t, err)
	sort.Slice(volumes, func(i, j int) bool {
		return domain.FormatAsset(volumes[i].AssetBase, volumes[i].AssetPrecision) <
			domain.FormatAsset(volumes[j].AssetBase, volumes[j].AssetPrecision)
	})
	for _, volume := range volumes {
		if domain.FormatAsset(volume.AssetBase, volume.AssetPrecision) == asset && volume.Color == color {
			return volume
		}
	}

	return balancehistorystore.Volume{Input: new(big.Int), Output: new(big.Int)}
}
