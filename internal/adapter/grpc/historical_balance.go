package grpc

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

// HistoricalBalanceViewTrailerKey carries the immutable projection view used by a response.
const HistoricalBalanceViewTrailerKey = "x-historical-balance-view-bin"

func selectorToProto(selector *ctrl.HistoricalBalanceSelector) (*servicepb.HistoricalBalanceSelector, error) {
	if selector == nil {
		return nil, nil
	}
	temporality, err := temporalityToProto(selector.Temporality)
	if err != nil {
		return nil, err
	}

	return &servicepb.HistoricalBalanceSelector{
		At:          &commonpb.Timestamp{Data: selector.At},
		Temporality: temporality,
	}, nil
}

func selectorFromProto(selector *servicepb.HistoricalBalanceSelector) (*ctrl.HistoricalBalanceSelector, error) {
	if selector == nil {
		return nil, nil
	}
	if selector.GetAt() == nil {
		return nil, errors.New("historical_balance.at is required")
	}

	temporality, err := temporalityFromProto(selector.GetTemporality())
	if err != nil {
		return nil, err
	}

	return &ctrl.HistoricalBalanceSelector{At: selector.GetAt().GetData(), Temporality: temporality}, nil
}

func temporalityToProto(temporality balancehistorystore.Temporality) (servicepb.HistoricalBalanceTemporality, error) {
	switch temporality {
	case balancehistorystore.TemporalityEffective:
		return servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE, nil
	case balancehistorystore.TemporalityInsertion:
		return servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION, nil
	default:
		return 0, fmt.Errorf("unknown historical-balance temporality value %d", temporality)
	}
}

func temporalityFromProto(temporality servicepb.HistoricalBalanceTemporality) (balancehistorystore.Temporality, error) {
	switch temporality {
	case servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE:
		return balancehistorystore.TemporalityEffective, nil
	case servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION:
		return balancehistorystore.TemporalityInsertion, nil
	default:
		return 0, fmt.Errorf("unknown historical_balance.temporality value %d", temporality)
	}
}

func historicalBalanceViewToProto(token *ctrl.HistoricalBalanceViewToken) (*servicepb.HistoricalBalanceView, error) {
	if token == nil {
		return nil, nil
	}
	if token.Token == "" {
		return nil, errors.New("historical-balance immutable view token is empty")
	}
	temporality, err := temporalityToProto(token.Temporality)
	if err != nil {
		return nil, err
	}

	return &servicepb.HistoricalBalanceView{
		RequestedAt:     &commonpb.Timestamp{Data: token.RequestedAt},
		Temporality:     temporality,
		Ledger:          token.Ledger,
		AuditWatermark:  token.AuditWatermark,
		LogWatermark:    token.LogWatermark,
		ManifestVersion: token.ManifestVersion,
		ViewToken:       token.Token,
	}, nil
}

func historicalBalanceViewFromProto(view *servicepb.HistoricalBalanceView) (*ctrl.HistoricalBalanceViewToken, error) {
	if view == nil {
		return nil, errors.New("historical-balance view trailer is empty")
	}
	temporality, err := temporalityFromProto(view.GetTemporality())
	if err != nil {
		return nil, err
	}
	if view.GetRequestedAt() == nil || view.GetViewToken() == "" {
		return nil, errors.New("historical-balance view trailer is incomplete")
	}

	return &ctrl.HistoricalBalanceViewToken{
		RequestedAt:     view.GetRequestedAt().GetData(),
		Temporality:     temporality,
		Ledger:          view.GetLedger(),
		AuditWatermark:  view.GetAuditWatermark(),
		LogWatermark:    view.GetLogWatermark(),
		ManifestVersion: view.GetManifestVersion(),
		Token:           view.GetViewToken(),
	}, nil
}

func historicalBalanceViewMetadata(token *ctrl.HistoricalBalanceViewToken) (metadata.MD, error) {
	view, err := historicalBalanceViewToProto(token)
	if err != nil {
		return nil, err
	}
	if view == nil {
		return nil, nil
	}
	encoded, err := view.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("marshaling historical-balance view trailer: %w", err)
	}

	return metadata.Pairs(HistoricalBalanceViewTrailerKey, string(encoded)), nil
}

func historicalBalanceViewFromMetadata(trailer metadata.MD) (*ctrl.HistoricalBalanceViewToken, error) {
	values := trailer.Get(HistoricalBalanceViewTrailerKey)
	if len(values) != 1 {
		return nil, errors.New("historical-balance response is missing its immutable view trailer")
	}

	view := &servicepb.HistoricalBalanceView{}
	if err := view.UnmarshalVT([]byte(values[0])); err != nil {
		return nil, fmt.Errorf("decoding historical-balance view trailer: %w", err)
	}

	return historicalBalanceViewFromProto(view)
}

func validateHistoricalBalanceView(selector *ctrl.HistoricalBalanceSelector, view *ctrl.HistoricalBalanceViewToken) error {
	if selector == nil || view == nil {
		return errors.New("historical-balance selector and immutable view are required")
	}
	if selector.At != view.RequestedAt || selector.Temporality != view.Temporality {
		return fmt.Errorf(
			"historical-balance response view does not match requested selector: requested at=%d temporality=%d, got at=%d temporality=%d",
			selector.At,
			selector.Temporality,
			view.RequestedAt,
			view.Temporality,
		)
	}

	return nil
}
