package internal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestFoldPITDualAxisSeparatesAxesAndDoesNotDoubleInvertReversals(t *testing.T) {
	t.Parallel()

	expected := pitDualAxisExpectedTransactions()
	oracle := &PITDualAxisOracle{
		Ledger:   PITDualAxisLedgerName(),
		LedgerID: 7,
		Effects: []PITDualAxisEffect{
			{LogSequence: 3, ProposalEndLogSequence: 4, EffectiveAt: 300, InsertedAt: 700, Postings: expected[2].postings},
			{LogSequence: 5, ProposalEndLogSequence: 5, EffectiveAt: 300, InsertedAt: 800, Postings: expected[4].postings},
			{LogSequence: 4, ProposalEndLogSequence: 4, EffectiveAt: 400, InsertedAt: 700, Postings: expected[3].postings},
			{LogSequence: 6, ProposalEndLogSequence: 6, EffectiveAt: 900, InsertedAt: 900, Postings: expected[5].postings},
		},
	}

	require.Empty(t, FoldPITDualAxis(
		oracle,
		servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
		299,
		6,
		"pitaxis:reversal:at-effective",
	))
	require.Equal(t, []CanonicalVolume{{Asset: "EUR/4", Color: "BLUE", Input: "70", Output: "70"}}, FoldPITDualAxis(
		oracle,
		servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
		300,
		6,
		"pitaxis:reversal:at-effective",
	))
	require.Equal(t, []CanonicalVolume{{Asset: "EUR/4", Color: "BLUE", Input: "70", Output: "0"}}, FoldPITDualAxis(
		oracle,
		servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
		799,
		6,
		"pitaxis:reversal:at-effective",
	))
	require.Equal(t, []CanonicalVolume{{Asset: "EUR/4", Color: "BLUE", Input: "70", Output: "70"}}, FoldPITDualAxis(
		oracle,
		servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
		800,
		6,
		"pitaxis:reversal:at-effective",
	))
	require.Equal(t, []CanonicalVolume{{Asset: "GBP", Input: "90", Output: "0"}}, FoldPITDualAxis(
		oracle,
		servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
		899,
		6,
		"pitaxis:reversal:normal",
	))
	require.Equal(t, []CanonicalVolume{{Asset: "GBP", Input: "90", Output: "90"}}, FoldPITDualAxis(
		oracle,
		servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
		900,
		6,
		"pitaxis:reversal:normal",
	))
}

func TestFoldPITDualAxisNeverExposesPartOfAnAtomicProposal(t *testing.T) {
	t.Parallel()

	expected := pitDualAxisExpectedTransactions()
	oracle := &PITDualAxisOracle{Effects: []PITDualAxisEffect{
		{LogSequence: 101, ProposalEndLogSequence: 104, EffectiveAt: 100, InsertedAt: 200, Postings: expected[0].postings},
		{LogSequence: 102, ProposalEndLogSequence: 104, EffectiveAt: 100, InsertedAt: 200, Postings: expected[1].postings},
	}}

	require.Empty(t, FoldPITDualAxis(
		oracle,
		servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
		200,
		103,
		"",
	))
	require.True(t, oracle.IsAtomicLogWatermark(100))
	require.False(t, oracle.IsAtomicLogWatermark(101))
	require.False(t, oracle.IsAtomicLogWatermark(103))
	require.True(t, oracle.IsAtomicLogWatermark(104))
	require.True(t, oracle.IsAtomicLogWatermark(999), "unrelated later global logs remain a legal cutoff")
}

func TestBuildPITDualAxisOracleUsesGlobalSequencesAndLocalOrdering(t *testing.T) {
	t.Parallel()

	expected := pitDualAxisExpectedTransactions()
	globalSequences := []uint64{101, 103, 106, 108, 120, 145}
	insertedAt := []uint64{1_000, 1_000, 1_000, 1_000, 2_000, 3_000}
	ordered := make([]*commonpb.Log, 0, len(expected))
	for index, transaction := range expected {
		ordered = append(ordered, pitDualAxisOracleTestLog(
			"pitaxis-oracle",
			globalSequences[index],
			transaction,
			insertedAt[index],
		))
	}
	shuffled := []*commonpb.Log{ordered[5], ordered[1], ordered[3], ordered[0], ordered[4], ordered[2]}

	oracle, err := buildPITDualAxisOracle("pitaxis-oracle", 77, shuffled)
	require.NoError(t, err)
	require.Equal(t, uint64(145), oracle.MaxLogSequence())
	require.Equal(t, uint32(77), oracle.LedgerID)
	require.Equal(t, uint64(145), shuffled[0].GetSequence(), "the loader must not reorder the caller's slice")
	require.Equal(t, globalSequences, []uint64{
		oracle.Effects[0].LogSequence,
		oracle.Effects[1].LogSequence,
		oracle.Effects[2].LogSequence,
		oracle.Effects[3].LogSequence,
		oracle.Effects[4].LogSequence,
		oracle.Effects[5].LogSequence,
	})
	require.Equal(t, []uint64{108, 108, 108, 108, 120, 145}, []uint64{
		oracle.Effects[0].ProposalEndLogSequence,
		oracle.Effects[1].ProposalEndLogSequence,
		oracle.Effects[2].ProposalEndLogSequence,
		oracle.Effects[3].ProposalEndLogSequence,
		oracle.Effects[4].ProposalEndLogSequence,
		oracle.Effects[5].ProposalEndLogSequence,
	})
	require.True(t, oracle.IsAtomicLogWatermark(100))
	require.False(t, oracle.IsAtomicLogWatermark(101))
	require.False(t, oracle.IsAtomicLogWatermark(107))
	require.True(t, oracle.IsAtomicLogWatermark(108))
	require.True(t, oracle.IsAtomicLogWatermark(119))
	require.True(t, oracle.IsAtomicLogWatermark(999))

	_, err = buildPITDualAxisOracle("pitaxis-oracle", 77, ordered[:5])
	require.ErrorIs(t, err, ErrPITDualAxisFixtureIncomplete)

	_, err = buildPITDualAxisOracle("pitaxis-oracle", 77, append(ordered, nil))
	require.ErrorContains(t, err, "contains 7 logs, expected 6")
}

func pitDualAxisOracleTestLog(
	ledger string,
	globalSequence uint64,
	expected pitDualAxisExpectedTransaction,
	insertedAt uint64,
) *commonpb.Log {
	effectiveAt := expected.effectiveAt
	if expected.normalReversal {
		effectiveAt = insertedAt
	}
	transaction := &commonpb.Transaction{
		Id:                 expected.id,
		Postings:           expected.postings,
		Timestamp:          &commonpb.Timestamp{Data: effectiveAt},
		InsertedAt:         &commonpb.Timestamp{Data: insertedAt},
		RevertsTransaction: expected.revertsTransaction,
	}
	var payload *commonpb.LedgerLogPayload
	if expected.revertsTransaction == 0 {
		payload = &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{Transaction: transaction},
		}}
	} else {
		payload = &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: expected.revertsTransaction,
				RevertTransaction:     transaction,
			},
		}}
	}

	return &commonpb.Log{
		Sequence: globalSequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: ledger,
			Log:        &commonpb.LedgerLog{Id: expected.id, Data: payload},
		}}},
	}
}

func TestPITDualAxisCasesCoverBothAxesEveryScopeAndTimestampNeighborhood(t *testing.T) {
	t.Parallel()

	oracle := &PITDualAxisOracle{Effects: []PITDualAxisEffect{
		{EffectiveAt: 100, InsertedAt: 200},
		{EffectiveAt: 100, InsertedAt: 300},
	}}
	cases := PITDualAxisCases(oracle)

	// Effective has one distinct boundary; insertion has two. Each contributes
	// t-1/t/t+1 crossed with the ledger plus every property-owned account.
	require.Len(t, cases, (1+2)*3*(1+len(pitDualAxisAccounts)))
	seen := make(map[servicepb.HistoricalBalanceTemporality]map[uint64]struct{})
	for _, testCase := range cases {
		if seen[testCase.Selector.GetTemporality()] == nil {
			seen[testCase.Selector.GetTemporality()] = make(map[uint64]struct{})
		}
		seen[testCase.Selector.GetTemporality()][testCase.Selector.GetAt().GetData()] = struct{}{}
	}
	require.Equal(t, map[uint64]struct{}{99: {}, 100: {}, 101: {}}, seen[servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE])
	require.Equal(t, map[uint64]struct{}{199: {}, 200: {}, 201: {}, 299: {}, 300: {}, 301: {}}, seen[servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION])
}

func TestFoldPITDualAxisLedgerScopeConservesEveryBucket(t *testing.T) {
	t.Parallel()

	expected := pitDualAxisExpectedTransactions()
	oracle := &PITDualAxisOracle{Effects: make([]PITDualAxisEffect, 0, len(expected))}
	for _, transaction := range expected {
		oracle.Effects = append(oracle.Effects, PITDualAxisEffect{
			LogSequence:            transaction.id,
			ProposalEndLogSequence: transaction.proposalEndLocalLogID,
			EffectiveAt:            transaction.effectiveAt,
			InsertedAt:             transaction.id * 100,
			Postings:               transaction.postings,
		})
	}

	for _, volume := range FoldPITDualAxis(
		oracle,
		servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
		1000,
		pitDualAxisEffectCount,
		"",
	) {
		require.Equal(t, volume.Input, volume.Output, "asset=%s color=%s", volume.Asset, volume.Color)
	}
}
