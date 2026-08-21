package internal

import (
	"context"
	"fmt"
	"sort"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const pointInTimeViewTrailerKey = "x-historical-balance-view-bin"

// ConfigureHistoricalBalances enables the rebuildable projection through the
// public client contract. Antithesis fixtures call this explicitly so their
// projection scope is part of the audited workload, not process configuration.
func ConfigureHistoricalBalances(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	ledger string,
) error {
	response, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_ConfigureHistoricalBalances{
			ConfigureHistoricalBalances: &servicepb.ConfigureHistoricalBalancesRequest{
				Ledger:  ledger,
				Enabled: true,
			},
		},
	}))
	if err != nil {
		return err
	}
	if len(response.GetLogs()) != 1 ||
		response.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetConfiguredHistoricalBalances() == nil {
		return fmt.Errorf("unexpected historical-balance configuration response for ledger %q", ledger)
	}

	return nil
}

// CanonicalVolume is the order-independent representation used to compare
// AggregateVolumes results across independent PIT storage scopes.
type CanonicalVolume struct {
	Asset  string
	Color  string
	Input  string
	Output string
}

// AggregatePointInTime performs one PIT aggregate and validates the mandatory
// immutable-view trailer against the request selector. It does not retry on its
// own; retry policy remains owned by the supplied client connection.
func AggregatePointInTime(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	request *servicepb.AggregateVolumesRequest,
) (*commonpb.AggregateResult, *servicepb.HistoricalBalanceView, error) {
	return aggregatePointInTime(ctx, client, request)
}

func aggregatePointInTime(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	request *servicepb.AggregateVolumesRequest,
	callOptions ...grpc.CallOption,
) (*commonpb.AggregateResult, *servicepb.HistoricalBalanceView, error) {
	if request.GetHistoricalBalance() == nil || request.GetHistoricalBalance().GetAt() == nil {
		return nil, nil, fmt.Errorf("point-in-time selector is required")
	}

	var trailer metadata.MD
	callOptions = append(callOptions, grpc.Trailer(&trailer))
	result, err := client.AggregateVolumes(ctx, request, callOptions...)
	if err != nil {
		return nil, nil, err
	}

	view, err := decodeHistoricalBalanceViewTrailer(trailer)
	if err != nil {
		return nil, nil, err
	}
	if err := validateHistoricalBalanceView(request, view); err != nil {
		return nil, nil, err
	}

	return result, view, nil
}

func validateHistoricalBalanceView(
	request *servicepb.AggregateVolumesRequest,
	view *servicepb.HistoricalBalanceView,
) error {
	if request.GetLedger() == "" {
		return fmt.Errorf("expected ledger name must be non-empty")
	}
	if view.GetLedger() != request.GetLedger() {
		return fmt.Errorf(
			"historical-balance view ledger %q differs from requested ledger %q",
			view.GetLedger(),
			request.GetLedger(),
		)
	}
	if view.GetRequestedAt().GetData() != request.GetHistoricalBalance().GetAt().GetData() {
		return fmt.Errorf(
			"point-in-time view requested timestamp %d differs from selector %d",
			view.GetRequestedAt().GetData(),
			request.GetHistoricalBalance().GetAt().GetData(),
		)
	}
	if view.GetTemporality() != request.GetHistoricalBalance().GetTemporality() {
		return fmt.Errorf(
			"point-in-time view axis %s differs from selector %s",
			view.GetTemporality(),
			request.GetHistoricalBalance().GetTemporality(),
		)
	}

	return nil
}

// IsClassifiedPointInTimeFailure reports fail-closed PIT outcomes for which no
// numerical sample exists. Dedicated properties validate when these reasons
// are allowed; the scope-equivalence property only constrains paired success.
func IsClassifiedPointInTimeFailure(err error) bool {
	switch ErrorReason(err) {
	case "HISTORY_BUILDING",
		"HISTORY_BEHIND",
		"HISTORY_SOURCE_MISSING",
		"HISTORY_CORRUPT":
		return true
	default:
		return false
	}
}

func decodeHistoricalBalanceViewTrailer(trailer metadata.MD) (*servicepb.HistoricalBalanceView, error) {
	values := trailer.Get(pointInTimeViewTrailerKey)
	if len(values) != 1 {
		return nil, fmt.Errorf(
			"expected one %s trailer value, got %d",
			pointInTimeViewTrailerKey,
			len(values),
		)
	}

	view := &servicepb.HistoricalBalanceView{}
	if err := view.UnmarshalVT([]byte(values[0])); err != nil {
		return nil, fmt.Errorf("decoding point-in-time view trailer: %w", err)
	}
	if view.GetRequestedAt() == nil || view.GetViewToken() == "" {
		return nil, fmt.Errorf("point-in-time view trailer is incomplete")
	}

	return view, nil
}

// CanonicalFlatAggregate validates and sorts a non-grouped aggregate result.
// Duplicate (asset, color) buckets are rejected instead of being silently
// merged, because the wire contract already requires one entry per bucket.
func CanonicalFlatAggregate(result *commonpb.AggregateResult) ([]CanonicalVolume, error) {
	if result == nil {
		return nil, fmt.Errorf("aggregate result is nil")
	}
	if len(result.GetGroups()) != 0 {
		return nil, fmt.Errorf("flat aggregate unexpectedly contains %d groups", len(result.GetGroups()))
	}

	canonical := make([]CanonicalVolume, 0, len(result.GetVolumes()))
	seen := make(map[string]struct{}, len(result.GetVolumes()))
	for _, volume := range result.GetVolumes() {
		if volume == nil {
			return nil, fmt.Errorf("aggregate contains a nil volume")
		}
		key := volume.GetAsset() + "\x00" + volume.GetColor()
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf(
				"aggregate contains duplicate bucket asset=%q color=%q",
				volume.GetAsset(),
				volume.GetColor(),
			)
		}
		seen[key] = struct{}{}
		canonical = append(canonical, CanonicalVolume{
			Asset:  volume.GetAsset(),
			Color:  volume.GetColor(),
			Input:  volume.GetInput().ToBigInt().String(),
			Output: volume.GetOutput().ToBigInt().String(),
		})
	}

	sort.Slice(canonical, func(left, right int) bool {
		if canonical[left].Asset != canonical[right].Asset {
			return canonical[left].Asset < canonical[right].Asset
		}

		return canonical[left].Color < canonical[right].Color
	})

	return canonical, nil
}
