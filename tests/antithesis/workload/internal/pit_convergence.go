package internal

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"reflect"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

const (
	pitConvergencePreFaultAsset   = "PITCVGPRE"
	pitConvergencePostFaultAsset  = "PITCVGPOST"
	pitConvergencePreFaultAmount  = "18446744073709551617"                    // 2^64 + 1
	pitConvergencePostFaultAmount = "340282366920938463463374607431768211457" // 2^128 + 1
)

// PITConvergenceLedgerName returns the fixed ledger whose history spans both
// sides of the Antithesis fault window. No generic driver owns this ledger.
func PITConvergenceLedgerName() string {
	return PrefixPITScope.WithSuffix("convergence")
}

// SeedPITConvergenceFixture creates the pre-fault half of the independent
// monetary oracle. The value crosses a uint64 limb boundary so replay and wire
// conversion cannot accidentally pass through a narrower integer path.
func SeedPITConvergenceFixture(ctx context.Context, client servicepb.BucketServiceClient) error {
	if err := CreateLedger(ctx, client, PITConvergenceLedgerName()); err != nil {
		return fmt.Errorf("creating PIT convergence fixture ledger: %w", err)
	}
	if err := ConfigureHistoricalBalances(ctx, client, PITConvergenceLedgerName()); err != nil {
		return fmt.Errorf("configuring PIT convergence fixture ledger: %w", err)
	}

	_, err := applyPITConvergencePosting(
		ctx,
		client,
		"pit-convergence-pre-fault-v1",
		"pit:convergence:pre",
		pitConvergencePreFaultAsset,
		pitConvergencePreFaultAmount,
	)
	if err != nil {
		return fmt.Errorf("applying PIT convergence pre-fault marker: %w", err)
	}

	return nil
}

// CommitPITConvergencePostFaultMarker appends the second half of the oracle
// after Antithesis has stopped faults and competing writers. The deterministic
// idempotency key makes an ambiguous acknowledgement safe to retry.
func CommitPITConvergencePostFaultMarker(
	ctx context.Context,
	client servicepb.BucketServiceClient,
) (uint64, error) {
	sequence, err := applyPITConvergencePosting(
		ctx,
		client,
		"pit-convergence-post-fault-v1",
		"pit:convergence:post",
		pitConvergencePostFaultAsset,
		pitConvergencePostFaultAmount,
	)
	if err != nil {
		return 0, fmt.Errorf("applying PIT convergence post-fault marker: %w", err)
	}

	return sequence, nil
}

// ObservePITConvergenceFixture performs one direct stale PIT read, authenticates
// its view provenance, enforces the acknowledged post-fault log floor, and
// compares the complete unfiltered aggregate with the independent oracle.
func ObservePITConvergenceFixture(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	expectedLedgerID uint32,
	minLogSequence uint64,
) ([]CanonicalVolume, *servicepb.HistoricalBalanceView, error) {
	request := &servicepb.AggregateVolumesRequest{
		Ledger:         PITConvergenceLedgerName(),
		MinLogSequence: minLogSequence,
		HistoricalBalance: &servicepb.HistoricalBalanceSelector{
			At:          &commonpb.Timestamp{Data: math.MaxUint64},
			Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
		},
	}
	result, view, err := AggregatePointInTime(
		WithStaleConsistency(ctx),
		client,
		request,
	)
	if err != nil {
		return nil, nil, err
	}
	if view.GetLogWatermark() < minLogSequence {
		return nil, view, fmt.Errorf(
			"PIT convergence log watermark %d is below acknowledged marker %d",
			view.GetLogWatermark(),
			minLogSequence,
		)
	}

	canonical, err := CanonicalFlatAggregate(result)
	if err != nil {
		return nil, view, err
	}
	if expected := PITConvergenceExpectedVolumes(); !reflect.DeepEqual(canonical, expected) {
		return canonical, view, fmt.Errorf(
			"PIT convergence aggregate %+v differs from oracle %+v",
			canonical,
			expected,
		)
	}

	return canonical, view, nil
}

// PITConvergenceExpectedVolumes returns a fresh copy of the complete monetary
// oracle. Menu axis: the action vocabulary is fixed; the two amounts are the
// boundary family immediately above 64-bit and 128-bit limbs.
func PITConvergenceExpectedVolumes() []CanonicalVolume {
	return []CanonicalVolume{
		{
			Asset:  pitConvergencePreFaultAsset,
			Input:  pitConvergencePreFaultAmount,
			Output: pitConvergencePreFaultAmount,
		},
		{
			Asset:  pitConvergencePostFaultAsset,
			Input:  pitConvergencePostFaultAmount,
			Output: pitConvergencePostFaultAmount,
		},
	}
}

func applyPITConvergencePosting(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	idempotencyKey string,
	destination string,
	asset string,
	amountString string,
) (uint64, error) {
	amount, ok := new(big.Int).SetString(amountString, 10)
	if !ok {
		return 0, fmt.Errorf("parsing PIT convergence amount %q", amountString)
	}
	response, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(
		idempotencyKey,
		&servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: PITConvergenceLedgerName(),
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{
								commonpb.NewPosting("world", destination, asset, amount),
							},
							Force: true,
						},
					}},
				},
			},
		},
	))
	if err != nil {
		return 0, err
	}
	if ExtractCreatedTransaction(response) == nil || len(response.GetLogs()) != 1 {
		return 0, fmt.Errorf(
			"PIT convergence marker returned transaction=%t logs=%d",
			ExtractCreatedTransaction(response) != nil,
			len(response.GetLogs()),
		)
	}
	sequence := response.GetLogs()[0].GetSequence()
	if sequence == 0 {
		return 0, fmt.Errorf("PIT convergence marker returned an empty log sequence")
	}

	return sequence, nil
}
