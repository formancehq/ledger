package http

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

const pointInTimeViewHeader = "X-Point-In-Time-View"

// aggregateVolumesResponseJSON is the camelCase JSON DTO for AggregateResult.
type aggregateVolumesResponseJSON struct {
	Volumes []*aggregatedVolumeJSON       `json:"volumes"`
	Groups  []*groupedAggregateResultJSON `json:"groups,omitempty"`
}

type aggregatedVolumeJSON struct {
	Asset string `json:"asset"`
	// Color is always emitted (even when empty) so clients can distinguish
	// the uncolored bucket from an older response shape that didn't carry
	// the color dimension at all.
	Color   string `json:"color"`
	Input   string `json:"input"`
	Output  string `json:"output"`
	Balance string `json:"balance"`
}

type groupedAggregateResultJSON struct {
	Prefix  string                  `json:"prefix"`
	Volumes []*aggregatedVolumeJSON `json:"volumes"`
}

func toAggregatedVolumeJSON(v *commonpb.AggregatedVolume) *aggregatedVolumeJSON {
	input := v.GetInput().ToBigInt()
	output := v.GetOutput().ToBigInt()
	balance := new(big.Int).Sub(input, output)

	return &aggregatedVolumeJSON{
		Asset:   v.GetAsset(),
		Color:   v.GetColor(),
		Input:   input.String(),
		Output:  output.String(),
		Balance: balance.String(),
	}
}

func toAggregateVolumesJSON(result *commonpb.AggregateResult) *aggregateVolumesResponseJSON {
	resp := &aggregateVolumesResponseJSON{}

	resp.Volumes = make([]*aggregatedVolumeJSON, 0, len(result.GetVolumes()))
	for _, v := range result.GetVolumes() {
		resp.Volumes = append(resp.Volumes, toAggregatedVolumeJSON(v))
	}

	if len(result.GetGroups()) > 0 {
		resp.Groups = make([]*groupedAggregateResultJSON, 0, len(result.GetGroups()))
		for _, g := range result.GetGroups() {
			group := &groupedAggregateResultJSON{
				Prefix:  g.GetPrefix(),
				Volumes: make([]*aggregatedVolumeJSON, 0, len(g.GetVolumes())),
			}
			for _, v := range g.GetVolumes() {
				group.Volumes = append(group.Volumes, toAggregatedVolumeJSON(v))
			}

			resp.Groups = append(resp.Groups, group)
		}
	}

	return resp
}

// handleAggregateVolumes handles GET /{ledgerName}/volumes.
func (s *Server) handleAggregateVolumes(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	useMaxPrecision := queryParamBool(r, "useMaxPrecision")
	collapseColors := queryParamBool(r, "collapseColors")
	pointInTime, err := parseHTTPPointInTime(r)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "VALIDATION", err)

		return
	}

	var groupByPrefixes []string
	if g := r.URL.Query().Get("groupByPrefixes"); g != "" {
		groupByPrefixes = strings.Split(g, ",")
	}

	// The `filter` query parameter accepts either the textual filterexpr grammar
	// or the structured v2 JSON DSL (EN-1511) and is the sole account selector.
	// It is compiled for the Accounts target and forwarded unchanged; aggregation
	// options (precision, grouping, color collapsing) stay independent of it.
	filter, ok := parseListFilter(w, r, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	if !ok {
		return
	}

	ctx, profile := query.WithProfile(r.Context())

	result, err := s.backend.AggregateVolumes(ctx, ledgerName, filter, query.AggregateOptions{
		UseMaxPrecision: useMaxPrecision,
		CollapseColors:  collapseColors,
		GroupByPrefixes: groupByPrefixes,
	}, ctrl.AggregateVolumesReadOptions{
		PointInTime: pointInTime,
	})
	if err != nil {
		var (
			building *balancehistorystore.ErrBuilding
			behind   *balancehistorystore.ErrBehind
		)
		if errors.As(err, &building) || errors.As(err, &behind) {
			// Both states are transient replica-local projection lag. Keep the
			// retry hint scoped to PIT aggregation instead of applying it to all
			// domain.KindUnavailable errors globally.
			w.Header().Set("Retry-After", "1")
		}
		handleError(w, r, err)

		return
	}

	if wantsHTTPProfile(r) {
		writeProfileHeader(w, profile)
	}
	if pointInTime != nil {
		if result == nil || result.View == nil {
			writeInternalServerError(w, r, errors.New("point-in-time aggregation returned no immutable view token"))

			return
		}
		if result.View.RequestedAt != pointInTime.At || result.View.Axis != pointInTime.Axis {
			writeInternalServerError(w, r, fmt.Errorf(
				"point-in-time aggregation view does not match requested selector: requested at=%d axis=%d, got at=%d axis=%d",
				pointInTime.At,
				pointInTime.Axis,
				result.View.RequestedAt,
				result.View.Axis,
			))

			return
		}
		if err := writePointInTimeViewHeader(w, result.View); err != nil {
			writeInternalServerError(w, r, err)

			return
		}
	}

	writeOK(w, toAggregateVolumesJSON(result.Aggregate))
}

func parseHTTPPointInTime(r *http.Request) (*ctrl.PointInTimeSelector, error) {
	values := r.URL.Query()
	pitValues := values["pit"]
	if len(pitValues) > 1 {
		return nil, errors.New("pit must be supplied once")
	}
	var pit string
	if len(pitValues) == 1 {
		pit = pitValues[0]
	}

	camelValue, camelPresent, err := strictOptionalBool(values["useInsertionDate"])
	if err != nil {
		return nil, fmt.Errorf("invalid useInsertionDate: %w", err)
	}
	snakeValue, snakePresent, err := strictOptionalBool(values["use_insertion_date"])
	if err != nil {
		return nil, fmt.Errorf("invalid use_insertion_date: %w", err)
	}
	if camelPresent && snakePresent && camelValue != snakeValue {
		return nil, errors.New("useInsertionDate and use_insertion_date disagree")
	}
	useInsertionDate := camelValue
	if snakePresent {
		useInsertionDate = snakeValue
	}
	if pit == "" {
		if camelPresent || snakePresent {
			return nil, errors.New("useInsertionDate requires pit")
		}

		return nil, nil
	}

	parsed, err := time.Parse(time.RFC3339Nano, pit)
	if err != nil {
		return nil, fmt.Errorf("pit must be an RFC3339 timestamp: %w", err)
	}
	if parsed.UnixMicro() < 0 {
		return nil, commonpb.ErrTimestampBeforeEpoch
	}

	axis := balancehistorystore.AxisEffective
	if useInsertionDate {
		axis = balancehistorystore.AxisInsertion
	}

	return &ctrl.PointInTimeSelector{At: uint64(parsed.UnixMicro()), Axis: axis}, nil
}

func strictOptionalBool(values []string) (bool, bool, error) {
	if len(values) == 0 {
		return false, false, nil
	}
	if len(values) != 1 {
		return false, true, errors.New("parameter must be supplied once")
	}
	value, err := strconv.ParseBool(values[0])
	if err != nil || (values[0] != "true" && values[0] != "false") {
		return false, true, errors.New("expected true or false")
	}

	return value, true, nil
}

func writePointInTimeViewHeader(w http.ResponseWriter, token *ctrl.VolumeViewToken) error {
	if token.Token == "" {
		return errors.New("point-in-time immutable view token is empty")
	}
	var axis servicepb.PointInTimeAxis
	switch token.Axis {
	case balancehistorystore.AxisEffective:
		axis = servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_EFFECTIVE
	case balancehistorystore.AxisInsertion:
		axis = servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_INSERTION
	default:
		return fmt.Errorf("unknown point-in-time axis value %d", token.Axis)
	}
	view := &servicepb.PointInTimeView{
		RequestedAt:          &commonpb.Timestamp{Data: token.RequestedAt},
		Axis:                 axis,
		LedgerId:             token.LedgerID,
		AuditWatermark:       token.AuditWatermark,
		LogWatermark:         token.LogWatermark,
		ManifestVersion:      token.ManifestVersion,
		HistoryAvailableFrom: &commonpb.Timestamp{Data: token.HistoryAvailableFrom},
		ViewToken:            token.Token,
	}
	encoded, err := view.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling HTTP point-in-time view: %w", err)
	}
	w.Header().Set(pointInTimeViewHeader, base64.StdEncoding.EncodeToString(encoded))

	return nil
}
