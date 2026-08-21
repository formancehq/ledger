package internal

import (
	"context"
	"fmt"
	"math"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	LinearizablePITProbeMetadataKey          = "x-formance-antithesis-linearizable-pit-probe"
	LinearizablePITBarrierReachedMetadataKey = "x-formance-antithesis-linearizable-pit-barrier-reached"
	linearizablePITAsset                     = "COIN"
	linearizablePITAmount                    = "18446744073709551617" // 2^64 + 1
)

// LinearizablePITFixture is a prefix-independent monetary oracle for one
// dedicated ledger. The ledger contains exactly one posting, so its complete
// unfiltered aggregate is known without consulting the SUT read path.
type LinearizablePITFixture struct {
	Ledger         string
	LedgerID       uint32
	MinLogSequence uint64
	Request        *servicepb.AggregateVolumesRequest
	Expected       []CanonicalVolume
}

// PrepareLinearizablePITFixture creates one isolated ledger and commits its
// distinctive marker before any fault is injected. The insertion-axis maximum
// selects every effect known at the immutable view watermark while the request
// floor binds success to the acknowledged marker log.
func PrepareLinearizablePITFixture(
	ctx context.Context,
	client servicepb.BucketServiceClient,
) (*LinearizablePITFixture, error) {
	ledger := PrefixSentinel.WithSuffix(fmt.Sprintf("pit-quorum-%016x", Rand().Uint64()))
	if err := CreateLedger(ctx, client, ledger); err != nil {
		return nil, fmt.Errorf("creating linearizable PIT fixture ledger: %w", err)
	}
	if err := ConfigureHistoricalBalances(ctx, client, ledger); err != nil {
		return nil, fmt.Errorf("configuring linearizable PIT fixture ledger: %w", err)
	}

	ledgerInfo, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
	if err != nil {
		return nil, fmt.Errorf("reading linearizable PIT fixture ledger: %w", err)
	}
	if ledgerInfo.GetId() == 0 {
		return nil, fmt.Errorf("linearizable PIT fixture ledger has an empty incarnation ID")
	}

	amount, ok := new(big.Int).SetString(linearizablePITAmount, 10)
	if !ok {
		return nil, fmt.Errorf("parsing linearizable PIT fixture amount")
	}
	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							commonpb.NewPosting("world", "pit:quorum:marker", linearizablePITAsset, amount),
						},
						Force: true,
					},
				}},
			},
		},
	}
	response, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(
		fmt.Sprintf("pit-linearizable-%016x%016x", Rand().Uint64(), Rand().Uint64()),
		request,
	))
	if err != nil {
		return nil, fmt.Errorf("committing linearizable PIT fixture marker: %w", err)
	}
	if ExtractCreatedTransaction(response) == nil || len(response.GetLogs()) != 1 {
		return nil, fmt.Errorf("linearizable PIT fixture returned no single CreatedTransaction log")
	}
	minLogSequence := response.GetLogs()[0].GetSequence()
	if minLogSequence == 0 {
		return nil, fmt.Errorf("linearizable PIT fixture returned an empty log sequence")
	}

	return &LinearizablePITFixture{
		Ledger:         ledger,
		LedgerID:       ledgerInfo.GetId(),
		MinLogSequence: minLogSequence,
		Request: &servicepb.AggregateVolumesRequest{
			Ledger:         ledger,
			MinLogSequence: minLogSequence,
			HistoricalBalance: &servicepb.HistoricalBalanceSelector{
				At:          &commonpb.Timestamp{Data: math.MaxUint64},
				Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
			},
		},
		Expected: []CanonicalVolume{{
			Asset:  linearizablePITAsset,
			Input:  linearizablePITAmount,
			Output: linearizablePITAmount,
		}},
	}, nil
}

// CheckLinearizablePIT executes one temporally attributable default-
// consistency request. x-consistency is removed rather than set to an explicit
// value, so the probe covers the public omitted-header contract exactly.
func CheckLinearizablePIT(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	fixture *LinearizablePITFixture,
	probeID string,
) (*servicepb.HistoricalBalanceView, bool, error) {
	if fixture == nil || fixture.Request == nil {
		return nil, false, fmt.Errorf("linearizable PIT fixture is required")
	}

	ctx = withLinearizablePITProbe(ctx, probeID)

	var header metadata.MD
	result, view, err := aggregatePointInTime(
		ctx,
		client,
		fixture.Request,
		grpc.Header(&header),
	)
	barrierReached := linearizablePITBarrierReached(header, probeID)
	if err != nil {
		return nil, barrierReached, err
	}
	if err := validateLinearizablePITResult(result, view, fixture); err != nil {
		return nil, barrierReached, err
	}

	return view, barrierReached, nil
}

func linearizablePITBarrierReached(header metadata.MD, probeID string) bool {
	if probeID == "" {
		return false
	}

	values := header.Get(LinearizablePITBarrierReachedMetadataKey)

	return len(values) == 1 && values[0] == probeID
}

func withLinearizablePITProbe(ctx context.Context, probeID string) context.Context {
	outgoing, _ := metadata.FromOutgoingContext(ctx)
	outgoing = outgoing.Copy()
	outgoing.Delete(metadataKeyConsistency)
	outgoing.Delete(LinearizablePITProbeMetadataKey)
	if probeID != "" {
		outgoing.Set(LinearizablePITProbeMetadataKey, probeID)
	}

	return metadata.NewOutgoingContext(ctx, outgoing)
}

func validateLinearizablePITResult(
	result *commonpb.AggregateResult,
	view *servicepb.HistoricalBalanceView,
	fixture *LinearizablePITFixture,
) error {
	if view.GetLogWatermark() < fixture.MinLogSequence {
		return fmt.Errorf(
			"point-in-time log watermark %d is below acknowledged marker %d",
			view.GetLogWatermark(),
			fixture.MinLogSequence,
		)
	}

	canonical, err := CanonicalFlatAggregate(result)
	if err != nil {
		return err
	}
	if len(canonical) != len(fixture.Expected) {
		return fmt.Errorf("point-in-time aggregate has %d buckets, expected %d", len(canonical), len(fixture.Expected))
	}
	for index := range canonical {
		if canonical[index] != fixture.Expected[index] {
			return fmt.Errorf(
				"point-in-time bucket %d is %+v, expected %+v",
				index,
				canonical[index],
				fixture.Expected[index],
			)
		}
	}

	return nil
}

// IsLinearizablePITPartitionTransient is intentionally narrower than
// IsTransient. The targeted fault only removes Raft quorum; object-store,
// source-integrity, read-index-filter and business errors are counterexamples.
func IsLinearizablePITPartitionTransient(err error) bool {
	if err == nil {
		return false
	}

	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	reason := ErrorReason(err)
	switch st.Code() {
	case codes.DeadlineExceeded:
		return reason == ""
	case codes.Unavailable:
		return reason == "" || reason == "HISTORY_BUILDING" || reason == "HISTORY_BEHIND"
	default:
		return false
	}
}
