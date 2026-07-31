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

const PointInTimeViewTrailerKey = "x-point-in-time-view-bin"

func selectorToProto(selector *ctrl.PointInTimeSelector) (*servicepb.PointInTimeSelector, error) {
	if selector == nil {
		return nil, nil
	}
	axis, err := axisToProto(selector.Axis)
	if err != nil {
		return nil, err
	}

	return &servicepb.PointInTimeSelector{
		At:   &commonpb.Timestamp{Data: selector.At},
		Axis: axis,
	}, nil
}

func selectorFromProto(selector *servicepb.PointInTimeSelector) (*ctrl.PointInTimeSelector, error) {
	if selector == nil {
		return nil, nil
	}
	if selector.GetAt() == nil {
		return nil, errors.New("point_in_time.at is required")
	}

	axis, err := axisFromProto(selector.GetAxis())
	if err != nil {
		return nil, err
	}

	return &ctrl.PointInTimeSelector{At: selector.GetAt().GetData(), Axis: axis}, nil
}

func axisToProto(axis balancehistorystore.Axis) (servicepb.PointInTimeAxis, error) {
	switch axis {
	case balancehistorystore.AxisEffective:
		return servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_EFFECTIVE, nil
	case balancehistorystore.AxisInsertion:
		return servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_INSERTION, nil
	default:
		return 0, fmt.Errorf("unknown point-in-time axis value %d", axis)
	}
}

func axisFromProto(axis servicepb.PointInTimeAxis) (balancehistorystore.Axis, error) {
	switch axis {
	case servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_EFFECTIVE:
		return balancehistorystore.AxisEffective, nil
	case servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_INSERTION:
		return balancehistorystore.AxisInsertion, nil
	default:
		return 0, fmt.Errorf("unknown point_in_time.axis value %d", axis)
	}
}

func pointInTimeViewToProto(token *ctrl.VolumeViewToken) (*servicepb.PointInTimeView, error) {
	if token == nil {
		return nil, nil
	}
	if token.Token == "" {
		return nil, errors.New("point-in-time immutable view token is empty")
	}
	axis, err := axisToProto(token.Axis)
	if err != nil {
		return nil, err
	}

	return &servicepb.PointInTimeView{
		RequestedAt:          &commonpb.Timestamp{Data: token.RequestedAt},
		Axis:                 axis,
		LedgerId:             token.LedgerID,
		AuditWatermark:       token.AuditWatermark,
		LogWatermark:         token.LogWatermark,
		ManifestVersion:      token.ManifestVersion,
		HistoryAvailableFrom: &commonpb.Timestamp{Data: token.HistoryAvailableFrom},
		ViewToken:            token.Token,
	}, nil
}

func pointInTimeViewFromProto(view *servicepb.PointInTimeView) (*ctrl.VolumeViewToken, error) {
	if view == nil {
		return nil, errors.New("point-in-time view trailer is empty")
	}
	axis, err := axisFromProto(view.GetAxis())
	if err != nil {
		return nil, err
	}
	if view.GetRequestedAt() == nil || view.GetHistoryAvailableFrom() == nil || view.GetViewToken() == "" {
		return nil, errors.New("point-in-time view trailer is incomplete")
	}

	return &ctrl.VolumeViewToken{
		RequestedAt:          view.GetRequestedAt().GetData(),
		Axis:                 axis,
		LedgerID:             view.GetLedgerId(),
		AuditWatermark:       view.GetAuditWatermark(),
		LogWatermark:         view.GetLogWatermark(),
		ManifestVersion:      view.GetManifestVersion(),
		HistoryAvailableFrom: view.GetHistoryAvailableFrom().GetData(),
		Token:                view.GetViewToken(),
	}, nil
}

func pointInTimeViewMetadata(token *ctrl.VolumeViewToken) (metadata.MD, error) {
	view, err := pointInTimeViewToProto(token)
	if err != nil {
		return nil, err
	}
	if view == nil {
		return nil, nil
	}
	encoded, err := view.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("marshaling point-in-time view trailer: %w", err)
	}

	return metadata.Pairs(PointInTimeViewTrailerKey, string(encoded)), nil
}

func pointInTimeViewFromMetadata(trailer metadata.MD) (*ctrl.VolumeViewToken, error) {
	values := trailer.Get(PointInTimeViewTrailerKey)
	if len(values) != 1 {
		return nil, errors.New("point-in-time response is missing its immutable view trailer")
	}

	view := &servicepb.PointInTimeView{}
	if err := view.UnmarshalVT([]byte(values[0])); err != nil {
		return nil, fmt.Errorf("decoding point-in-time view trailer: %w", err)
	}

	return pointInTimeViewFromProto(view)
}

func validatePointInTimeView(selector *ctrl.PointInTimeSelector, view *ctrl.VolumeViewToken) error {
	if selector == nil || view == nil {
		return errors.New("point-in-time selector and immutable view are required")
	}
	if selector.At != view.RequestedAt || selector.Axis != view.Axis {
		return fmt.Errorf(
			"point-in-time response view does not match requested selector: requested at=%d axis=%d, got at=%d axis=%d",
			selector.At,
			selector.Axis,
			view.RequestedAt,
			view.Axis,
		)
	}

	return nil
}
