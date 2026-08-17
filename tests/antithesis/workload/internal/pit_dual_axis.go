package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"sort"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

const pitDualAxisEffectCount = 6

var (
	// ErrPITDualAxisFixtureIncomplete means the async log index has not exposed
	// all six setup logs yet; callers may retry without treating it as a safety
	// failure.
	ErrPITDualAxisFixtureIncomplete = errors.New("point-in-time dual-axis fixture is not fully indexed")

	pitDualAxisAccounts = []string{
		"world",
		"pitaxis:users:alice",
		"pitaxis:users:bob",
		"pitaxis:reversal:at-effective",
		"pitaxis:reversal:normal",
	}
)

type pitDualAxisExpectedTransaction struct {
	id                    uint64
	proposalEndLocalLogID uint64
	postings              []*commonpb.Posting
	effectiveAt           uint64
	revertsTransaction    uint64
	normalReversal        bool
}

// PITDualAxisEffect is one independently modelled committed transaction. The
// property treats a compensating reversal transaction exactly like any other
// resolved transaction: its already-reversed postings are never inverted a
// second time.
type PITDualAxisEffect struct {
	LogSequence            uint64
	ProposalEndLogSequence uint64
	EffectiveAt            uint64
	InsertedAt             uint64
	Postings               []*commonpb.Posting
}

// PITDualAxisOracle contains only property-owned fixture data plus the commit
// timestamps exposed by the public transaction API. It deliberately does not
// call the production balance-history reducer or read its physical store.
type PITDualAxisOracle struct {
	Ledger   string
	LedgerID uint32
	Effects  []PITDualAxisEffect
}

// PITDualAxisCase identifies one temporal boundary and one aggregation scope.
// Scope is empty for the full ledger and otherwise names one exact account.
type PITDualAxisCase struct {
	Name     string
	AxisName string
	Scope    string
	Selector *servicepb.HistoricalBalanceSelector
}

// PITDualAxisLedgerName returns the fixed ledger seeded before chaos begins.
func PITDualAxisLedgerName() string {
	return PrefixPITDualAxis.WithSuffix("oracle")
}

func pitDualAxisExpectedTransactions() []pitDualAxisExpectedTransaction {
	backdatedAt := uint64(time.Date(2020, time.January, 2, 3, 4, 5, 0, time.UTC).UnixMicro())
	atEffectiveAt := uint64(time.Date(2021, time.June, 7, 8, 9, 10, 0, time.UTC).UnixMicro())
	normalAt := uint64(time.Date(2022, time.July, 8, 9, 10, 11, 0, time.UTC).UnixMicro())
	futureAt := uint64(time.Date(2035, time.December, 30, 12, 13, 14, 0, time.UTC).UnixMicro())

	originals := []pitDualAxisExpectedTransaction{
		{
			id:                    1,
			proposalEndLocalLogID: 4,
			effectiveAt:           backdatedAt,
			postings: []*commonpb.Posting{
				actions.NewColoredPosting("world", "pitaxis:users:alice", big.NewInt(100), "USD/2", "RED"),
			},
		},
		{
			id:                    2,
			proposalEndLocalLogID: 4,
			effectiveAt:           futureAt,
			postings: []*commonpb.Posting{
				actions.NewColoredPosting("pitaxis:users:alice", "pitaxis:users:bob", big.NewInt(40), "USD/2", "RED"),
			},
		},
		{
			id:                    3,
			proposalEndLocalLogID: 4,
			effectiveAt:           atEffectiveAt,
			postings: []*commonpb.Posting{
				actions.NewColoredPosting("world", "pitaxis:reversal:at-effective", big.NewInt(70), "EUR/4", "BLUE"),
			},
		},
		{
			id:                    4,
			proposalEndLocalLogID: 4,
			effectiveAt:           normalAt,
			postings: []*commonpb.Posting{
				actions.NewPosting("world", "pitaxis:reversal:normal", big.NewInt(90), "GBP"),
			},
		},
	}

	return append(originals,
		pitDualAxisExpectedTransaction{
			id:                    5,
			proposalEndLocalLogID: 5,
			effectiveAt:           atEffectiveAt,
			postings:              reversePITDualAxisPostings(originals[2].postings),
			revertsTransaction:    3,
		},
		pitDualAxisExpectedTransaction{
			id:                    6,
			proposalEndLocalLogID: 6,
			postings:              reversePITDualAxisPostings(originals[3].postings),
			revertsTransaction:    4,
			normalReversal:        true,
		},
	)
}

func reversePITDualAxisPostings(postings []*commonpb.Posting) []*commonpb.Posting {
	reversed := make([]*commonpb.Posting, len(postings))
	for index, posting := range postings {
		reversed[index] = &commonpb.Posting{
			Source:      posting.GetDestination(),
			Destination: posting.GetSource(),
			Amount:      posting.GetAmount().CloneVT(),
			Asset:       posting.GetAsset(),
			Color:       posting.GetColor(),
		}
	}

	return reversed
}

func equalPITDualAxisPostings(left, right []*commonpb.Posting) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !proto.Equal(left[index], right[index]) {
			return false
		}
	}

	return true
}

// SeedPITDualAxisFixture commits four explicit transactions in one batch, then
// reverts one at its effective date and one at the current FSM date. The
// isolated ledger contains no non-transaction ledger logs, so transaction IDs
// and ledger-log IDs are both the contiguous range 1..6; the seed validates
// that structural premise before the oracle relies on it.
func SeedPITDualAxisFixture(ctx context.Context, client servicepb.BucketServiceClient) error {
	ledger := PITDualAxisLedgerName()
	if err := CreateLedger(ctx, client, ledger); err != nil {
		return err
	}

	expected := pitDualAxisExpectedTransactions()
	requests := make([]*servicepb.Request, 0, 4)
	for _, transaction := range expected[:4] {
		requests = append(requests, actions.WithTimestamp(
			actions.CreateForceTransactionAction(ledger, transaction.postings, nil),
			time.UnixMicro(int64(transaction.effectiveAt)).UTC(),
		))
	}
	response, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(
		"pit-dual-axis-fixture-transactions-v1",
		requests...,
	))
	if err != nil {
		return fmt.Errorf("applying point-in-time dual-axis fixture: %w", err)
	}
	if len(response.GetLogs()) != len(requests) {
		return fmt.Errorf("point-in-time dual-axis fixture returned %d logs, expected %d", len(response.GetLogs()), len(requests))
	}
	for index, log := range response.GetLogs() {
		if err := validatePITDualAxisSeedLog(log, ledger, expected[index]); err != nil {
			return err
		}
	}

	for _, transaction := range expected[4:] {
		response, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(
			fmt.Sprintf("pit-dual-axis-fixture-revert-%d-v1", transaction.revertsTransaction),
			actions.RevertTransactionAction(
				ledger,
				transaction.revertsTransaction,
				true,
				!transaction.normalReversal,
				nil,
			),
		))
		if err != nil {
			return fmt.Errorf("reverting point-in-time dual-axis transaction %d: %w", transaction.revertsTransaction, err)
		}
		if len(response.GetLogs()) != 1 {
			return fmt.Errorf("point-in-time dual-axis revert returned %d logs, expected 1", len(response.GetLogs()))
		}
		if err := validatePITDualAxisSeedLog(response.GetLogs()[0], ledger, transaction); err != nil {
			return err
		}
	}
	if err := ConfigureHistoricalBalances(ctx, client, ledger); err != nil {
		return fmt.Errorf("configuring historical balances for dual-temporality fixture: %w", err)
	}

	assert.Reachable(
		"pit: dual-axis fixture committed a backdated effect",
		Details{"ledger": ledger, "transaction_id": 1},
	)
	assert.Reachable(
		"pit: dual-axis fixture committed an at-effective-date reversal",
		Details{"ledger": ledger, "transaction_id": 5, "reverted_transaction_id": 3},
	)
	assert.Reachable(
		"pit: dual-axis fixture committed a normal reversal",
		Details{"ledger": ledger, "transaction_id": 6, "reverted_transaction_id": 4},
	)

	return nil
}

func validatePITDualAxisSeedLog(log *commonpb.Log, ledger string, expected pitDualAxisExpectedTransaction) error {
	if log == nil || log.GetPayload().GetApply() == nil {
		return fmt.Errorf("point-in-time dual-axis log for transaction %d is not an apply log", expected.id)
	}
	apply := log.GetPayload().GetApply()
	if apply.GetLedgerName() != ledger || apply.GetLog() == nil {
		return fmt.Errorf("point-in-time dual-axis log %d has invalid ledger payload", expected.id)
	}
	if apply.GetLog().GetId() != expected.id {
		return fmt.Errorf("point-in-time dual-axis ledger log id %d, expected %d", apply.GetLog().GetId(), expected.id)
	}

	var transaction *commonpb.Transaction
	if expected.revertsTransaction == 0 {
		transaction = apply.GetLog().GetData().GetCreatedTransaction().GetTransaction()
	} else {
		reverted := apply.GetLog().GetData().GetRevertedTransaction()
		if reverted.GetRevertedTransactionId() != expected.revertsTransaction {
			return fmt.Errorf("point-in-time dual-axis revert targets transaction %d, expected %d", reverted.GetRevertedTransactionId(), expected.revertsTransaction)
		}
		transaction = reverted.GetRevertTransaction()
	}
	if transaction == nil || transaction.GetId() != expected.id {
		return fmt.Errorf("point-in-time dual-axis transaction id %d, expected %d", transaction.GetId(), expected.id)
	}
	if !equalPITDualAxisPostings(transaction.GetPostings(), expected.postings) {
		return fmt.Errorf("point-in-time dual-axis transaction %d returned unexpected postings", expected.id)
	}
	if transaction.GetTimestamp() == nil || transaction.GetInsertedAt() == nil {
		return fmt.Errorf("point-in-time dual-axis transaction %d has incomplete timestamps", expected.id)
	}
	if expected.normalReversal {
		if transaction.GetTimestamp().GetData() != transaction.GetInsertedAt().GetData() {
			return fmt.Errorf("point-in-time dual-axis normal reversal %d does not use insertion time as effective time", expected.id)
		}
	} else if transaction.GetTimestamp().GetData() != expected.effectiveAt {
		return fmt.Errorf("point-in-time dual-axis transaction %d effective timestamp %d, expected %d", expected.id, transaction.GetTimestamp().GetData(), expected.effectiveAt)
	}

	return nil
}

// LoadPITDualAxisOracle authenticates the six property-owned public logs and
// obtains their global source sequences plus server-assigned timestamps. The
// outer common.Log.Sequence is the PIT trailer's watermark coordinate; the
// nested LedgerLog.ID is ledger-local and is used only to order/authenticate
// this isolated fixture.
func LoadPITDualAxisOracle(ctx context.Context, client servicepb.BucketServiceClient) (*PITDualAxisOracle, error) {
	ledger := PITDualAxisLedgerName()
	ledgerInfo, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
	if err != nil {
		return nil, fmt.Errorf("getting point-in-time dual-axis ledger: %w", err)
	}
	if ledgerInfo.GetId() == 0 {
		return nil, fmt.Errorf("point-in-time dual-axis ledger has no numeric incarnation")
	}

	stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{
		Ledger: ledger,
		Options: &commonpb.ListOptions{
			PageSize: pitDualAxisEffectCount + 1,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("listing point-in-time dual-axis logs: %w", err)
	}
	logs := make([]*commonpb.Log, 0, pitDualAxisEffectCount+1)
	for {
		entry, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return nil, fmt.Errorf("receiving point-in-time dual-axis logs: %w", recvErr)
		}
		data := entry.GetPayload().GetApply().GetLog().GetData()
		if data.GetCreatedTransaction() != nil || data.GetRevertedTransaction() != nil {
			logs = append(logs, entry)
		}
	}

	return buildPITDualAxisOracle(ledger, ledgerInfo.GetId(), logs)
}

func buildPITDualAxisOracle(ledger string, ledgerID uint32, logs []*commonpb.Log) (*PITDualAxisOracle, error) {
	if len(logs) < pitDualAxisEffectCount {
		return nil, fmt.Errorf("%w: found %d of %d logs", ErrPITDualAxisFixtureIncomplete, len(logs), pitDualAxisEffectCount)
	}
	if len(logs) > pitDualAxisEffectCount {
		return nil, fmt.Errorf("point-in-time dual-axis ledger contains %d logs, expected %d", len(logs), pitDualAxisEffectCount)
	}
	orderedLogs := append([]*commonpb.Log(nil), logs...)
	sort.Slice(orderedLogs, func(left, right int) bool {
		return orderedLogs[left].GetPayload().GetApply().GetLog().GetId() < orderedLogs[right].GetPayload().GetApply().GetLog().GetId()
	})

	expected := pitDualAxisExpectedTransactions()
	oracle := &PITDualAxisOracle{
		Ledger:   ledger,
		LedgerID: ledgerID,
		Effects:  make([]PITDualAxisEffect, 0, len(expected)),
	}
	var (
		batchInsertedAt    uint64
		previousInsertedAt uint64
		previousSequence   uint64
	)
	for index, transaction := range expected {
		entry := orderedLogs[index]
		if entry.GetSequence() == 0 || entry.GetSequence() <= previousSequence {
			return nil, fmt.Errorf("point-in-time dual-axis global log sequence %d does not follow %d", entry.GetSequence(), previousSequence)
		}
		apply := entry.GetPayload().GetApply()
		if apply == nil || apply.GetLedgerName() != ledger || apply.GetLog() == nil || apply.GetLog().GetId() != transaction.id {
			return nil, fmt.Errorf("point-in-time dual-axis log %d has invalid ledger-local identity", transaction.id)
		}
		var got *commonpb.Transaction
		if transaction.revertsTransaction == 0 {
			got = apply.GetLog().GetData().GetCreatedTransaction().GetTransaction()
		} else {
			reverted := apply.GetLog().GetData().GetRevertedTransaction()
			if reverted.GetRevertedTransactionId() != transaction.revertsTransaction {
				return nil, fmt.Errorf("point-in-time dual-axis log %d reverts %d, expected %d", transaction.id, reverted.GetRevertedTransactionId(), transaction.revertsTransaction)
			}
			got = reverted.GetRevertTransaction()
		}
		if got == nil || got.GetId() != transaction.id {
			return nil, fmt.Errorf("point-in-time dual-axis transaction id %d, expected %d", got.GetId(), transaction.id)
		}
		if !equalPITDualAxisPostings(got.GetPostings(), transaction.postings) {
			return nil, fmt.Errorf("point-in-time dual-axis transaction %d projection has unexpected postings", transaction.id)
		}
		if got.GetTimestamp() == nil || got.GetInsertedAt() == nil || got.GetInsertedAt().GetData() == 0 {
			return nil, fmt.Errorf("point-in-time dual-axis transaction %d projection has incomplete timestamps", transaction.id)
		}
		if transaction.normalReversal {
			if got.GetTimestamp().GetData() != got.GetInsertedAt().GetData() {
				return nil, fmt.Errorf("point-in-time dual-axis normal reversal %d has effective timestamp %d, expected insertion timestamp %d", transaction.id, got.GetTimestamp().GetData(), got.GetInsertedAt().GetData())
			}
		} else if got.GetTimestamp().GetData() != transaction.effectiveAt {
			return nil, fmt.Errorf("point-in-time dual-axis transaction %d has effective timestamp %d, expected %d", transaction.id, got.GetTimestamp().GetData(), transaction.effectiveAt)
		}
		if got.GetInsertedAt().GetData() < previousInsertedAt {
			return nil, fmt.Errorf("point-in-time dual-axis transaction %d insertion timestamp regressed from %d to %d", transaction.id, previousInsertedAt, got.GetInsertedAt().GetData())
		}
		if transaction.proposalEndLocalLogID == 4 {
			if batchInsertedAt == 0 {
				batchInsertedAt = got.GetInsertedAt().GetData()
			} else if got.GetInsertedAt().GetData() != batchInsertedAt {
				return nil, fmt.Errorf("point-in-time dual-axis atomic fixture batch has insertion timestamps %d and %d", batchInsertedAt, got.GetInsertedAt().GetData())
			}
		}
		if got.GetRevertsTransaction() != transaction.revertsTransaction {
			return nil, fmt.Errorf("point-in-time dual-axis transaction %d reverts %d, expected %d", transaction.id, got.GetRevertsTransaction(), transaction.revertsTransaction)
		}

		oracle.Effects = append(oracle.Effects, PITDualAxisEffect{
			LogSequence: entry.GetSequence(),
			EffectiveAt: got.GetTimestamp().GetData(),
			InsertedAt:  got.GetInsertedAt().GetData(),
			Postings:    transaction.postings,
		})
		previousInsertedAt = got.GetInsertedAt().GetData()
		previousSequence = entry.GetSequence()
	}
	proposalEnd := oracle.Effects[3].LogSequence
	for index := range oracle.Effects[:4] {
		oracle.Effects[index].ProposalEndLogSequence = proposalEnd
	}
	oracle.Effects[4].ProposalEndLogSequence = oracle.Effects[4].LogSequence
	oracle.Effects[5].ProposalEndLogSequence = oracle.Effects[5].LogSequence

	return oracle, nil
}

// IsPITDualAxisFixtureIncomplete reports ordinary read-index lag while the six
// setup logs have not all become visible to ListLogs yet.
func IsPITDualAxisFixtureIncomplete(err error) bool {
	return errors.Is(err, ErrPITDualAxisFixtureIncomplete)
}

// MaxLogSequence is the complete fixture watermark.
func (oracle *PITDualAxisOracle) MaxLogSequence() uint64 {
	if oracle == nil || len(oracle.Effects) == 0 {
		return 0
	}

	return oracle.Effects[len(oracle.Effects)-1].LogSequence
}

// IsAtomicLogWatermark reports whether the returned global source cutoff would
// expose only part of one property-owned proposal. Unrelated global logs may
// legitimately make the watermark fall between or after this fixture's three
// proposals, so the predicate rejects only a cutoff that includes one of the
// four batch logs without reaching their shared proposal end.
func (oracle *PITDualAxisOracle) IsAtomicLogWatermark(watermark uint64) bool {
	if oracle == nil {
		return false
	}
	for _, effect := range oracle.Effects {
		if effect.LogSequence <= watermark && effect.ProposalEndLogSequence > watermark {
			return false
		}
	}

	return true
}

// PITDualAxisCases returns the bounded interesting-value menu. Every distinct
// timestamp observed on both axes is queried immediately before, exactly at,
// and immediately after the inclusive boundary, for both the ledger-wide
// scope and every property-owned account.
func PITDualAxisCases(oracle *PITDualAxisOracle) []PITDualAxisCase {
	if oracle == nil {
		return nil
	}

	type axisSpec struct {
		name string
		axis servicepb.HistoricalBalanceTemporality
		at   func(PITDualAxisEffect) uint64
	}
	axes := []axisSpec{
		{name: "effective", axis: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE, at: func(effect PITDualAxisEffect) uint64 { return effect.EffectiveAt }},
		{name: "insertion", axis: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION, at: func(effect PITDualAxisEffect) uint64 { return effect.InsertedAt }},
	}
	scopes := append([]string{""}, pitDualAxisAccounts...)

	var cases []PITDualAxisCase
	for _, axis := range axes {
		boundaries := make(map[uint64]struct{}, len(oracle.Effects))
		for _, effect := range oracle.Effects {
			boundaries[axis.at(effect)] = struct{}{}
		}
		ordered := make([]uint64, 0, len(boundaries))
		for boundary := range boundaries {
			ordered = append(ordered, boundary)
		}
		sort.Slice(ordered, func(left, right int) bool { return ordered[left] < ordered[right] })

		for _, boundary := range ordered {
			for _, point := range timestampNeighborhood(boundary) {
				for _, scope := range scopes {
					scopeName := scope
					if scopeName == "" {
						scopeName = "ledger"
					}
					cases = append(cases, PITDualAxisCase{
						Name:     fmt.Sprintf("%s/%d/%s", axis.name, point, scopeName),
						AxisName: axis.name,
						Scope:    scope,
						Selector: &servicepb.HistoricalBalanceSelector{
							At:          &commonpb.Timestamp{Data: point},
							Temporality: axis.axis,
						},
					})
				}
			}
		}
	}

	return cases
}

func timestampNeighborhood(boundary uint64) []uint64 {
	values := make([]uint64, 0, 3)
	if boundary > 0 {
		values = append(values, boundary-1)
	}
	values = append(values, boundary)
	if boundary < math.MaxUint64 {
		values = append(values, boundary+1)
	}

	return values
}

// FoldPITDualAxis computes cumulative input/output volumes independently from
// resolved postings through the immutable response watermark. An empty account
// selects the full ledger; a non-empty account selects exactly that account.
func FoldPITDualAxis(
	oracle *PITDualAxisOracle,
	axis servicepb.HistoricalBalanceTemporality,
	at uint64,
	logWatermark uint64,
	account string,
) []CanonicalVolume {
	type volume struct {
		input  *big.Int
		output *big.Int
	}
	type key struct {
		asset string
		color string
	}
	volumes := make(map[key]*volume)
	if oracle == nil {
		return nil
	}

	for _, effect := range oracle.Effects {
		if effect.ProposalEndLogSequence > logWatermark {
			continue
		}
		effectAt := effect.EffectiveAt
		if axis == servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION {
			effectAt = effect.InsertedAt
		}
		if effectAt > at {
			continue
		}

		for _, posting := range effect.Postings {
			bucket := key{asset: posting.GetAsset(), color: posting.GetColor()}
			amount := posting.GetAmount().ToBigInt()
			if account == "" || posting.GetSource() == account {
				current := volumes[bucket]
				if current == nil {
					current = &volume{input: new(big.Int), output: new(big.Int)}
					volumes[bucket] = current
				}
				current.output.Add(current.output, amount)
			}
			if account == "" || posting.GetDestination() == account {
				current := volumes[bucket]
				if current == nil {
					current = &volume{input: new(big.Int), output: new(big.Int)}
					volumes[bucket] = current
				}
				current.input.Add(current.input, amount)
			}
		}
	}

	canonical := make([]CanonicalVolume, 0, len(volumes))
	for key, volume := range volumes {
		canonical = append(canonical, CanonicalVolume{
			Asset:  key.asset,
			Color:  key.color,
			Input:  volume.input.String(),
			Output: volume.output.String(),
		})
	}
	sort.Slice(canonical, func(left, right int) bool {
		if canonical[left].Asset != canonical[right].Asset {
			return canonical[left].Asset < canonical[right].Asset
		}

		return canonical[left].Color < canonical[right].Color
	})

	return canonical
}

// ComparePITDualAxisCase validates one successful public response. Classified
// fail-closed/transient outcomes are inconclusive. minimumLogSequence is zero
// during fault-time sampling and the complete fixture watermark in the
// quiescent command.
func ComparePITDualAxisCase(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	nodeAddress string,
	nodeID uint32,
	oracle *PITDualAxisOracle,
	testCase PITDualAxisCase,
	minimumLogSequence uint64,
) bool {
	request := &servicepb.AggregateVolumesRequest{
		Ledger:            oracle.Ledger,
		MinLogSequence:    minimumLogSequence,
		HistoricalBalance: testCase.Selector,
		UseMaxPrecision:   false,
		CollapseColors:    false,
	}
	if testCase.Scope != "" {
		request.Filter = actions.AddressExactFilter(testCase.Scope)
	}
	details := Details{
		"ledger":               oracle.Ledger,
		"ledger_id":            oracle.LedgerID,
		"node_address":         nodeAddress,
		"node_id":              nodeID,
		"case":                 testCase.Name,
		"axis":                 testCase.AxisName,
		"requested_at":         testCase.Selector.GetAt().GetData(),
		"account":              testCase.Scope,
		"minimum_log_sequence": minimumLogSequence,
	}
	result, view, err := AggregatePointInTime(ctx, client, request)
	if err != nil {
		if IsTransient(err) || IsCanceled(err) || IsClassifiedPointInTimeFailure(err) {
			return false
		}
		assert.Unreachable(
			"pit: dual-axis aggregate returned unexpected error",
			details.With(Details{"error": err}),
		)

		return false
	}

	details = details.With(Details{
		"view_token":       view.GetViewToken(),
		"audit_watermark":  view.GetAuditWatermark(),
		"log_watermark":    view.GetLogWatermark(),
		"manifest_version": view.GetManifestVersion(),
	})
	watermarkValid := view.GetLogWatermark() >= minimumLogSequence && oracle.IsAtomicLogWatermark(view.GetLogWatermark())
	assert.Always(
		watermarkValid,
		"pit: dual-axis fixture view watermark stays on an acknowledged atomic log boundary",
		details.With(Details{"property_final_log_sequence": oracle.MaxLogSequence()}),
	)
	if !watermarkValid {
		return false
	}

	actual, canonicalErr := CanonicalFlatAggregate(result)
	assert.Always(
		canonicalErr == nil,
		"pit: dual-axis aggregate returns canonical flat volume buckets",
		details.With(Details{"error": canonicalErr}),
	)
	expected := FoldPITDualAxis(
		oracle,
		testCase.Selector.GetTemporality(),
		testCase.Selector.GetAt().GetData(),
		view.GetLogWatermark(),
		testCase.Scope,
	)
	exact := canonicalErr == nil && reflect.DeepEqual(actual, expected)
	assert.Always(
		exact,
		"pit: effective and insertion axes exactly fold resolved postings and reversals",
		details.With(Details{"actual": actual, "expected": expected, "canonical_error": canonicalErr}),
	)
	if !exact {
		return false
	}

	if testCase.Scope == "" {
		conserved := true
		for _, volume := range actual {
			if volume.Input != volume.Output {
				conserved = false
				break
			}
		}
		assert.Always(
			conserved,
			"pit: dual-axis ledger aggregate conserves every asset and color",
			details.With(Details{"actual": actual}),
		)
		if !conserved {
			return false
		}
	}

	return true
}
