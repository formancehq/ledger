package http

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

const historicalBalanceViewHeader = "X-Historical-Balance-View"

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
	historicalBalance, err := parseHTTPHistoricalBalance(r)
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
		HistoricalBalance: historicalBalance,
	})
	if err != nil {
		var (
			building *balancehistorystore.ErrBuilding
			behind   *balancehistorystore.ErrBehind
		)
		if errors.As(err, &building) || errors.As(err, &behind) {
			// Both states are transient replica-local projection lag. Keep the
			// retry hint scoped to historical aggregation instead of applying it to all
			// domain.KindUnavailable errors globally.
			w.Header().Set("Retry-After", "1")
		}
		handleError(w, r, err)

		return
	}

	if wantsHTTPProfile(r) {
		writeProfileHeader(w, profile)
	}
	if historicalBalance != nil {
		if result == nil || result.View == nil {
			writeInternalServerError(w, r, errors.New("historical-balance aggregation returned no immutable view token"))

			return
		}
		if result.View.RequestedAt != historicalBalance.At || result.View.Temporality != historicalBalance.Temporality {
			writeInternalServerError(w, r, fmt.Errorf(
				"historical-balance aggregation view does not match requested selector: requested at=%d temporality=%d, got at=%d temporality=%d",
				historicalBalance.At,
				historicalBalance.Temporality,
				result.View.RequestedAt,
				result.View.Temporality,
			))

			return
		}
		if err := writeHistoricalBalanceViewHeader(w, result.View); err != nil {
			writeInternalServerError(w, r, err)

			return
		}
	}

	writeOK(w, toAggregateVolumesJSON(result.Aggregate))
}

func parseHTTPHistoricalBalance(r *http.Request) (*ctrl.HistoricalBalanceSelector, error) {
	values := r.URL.Query()
	atValues := values["at"]
	if len(atValues) > 1 {
		return nil, errors.New("at must be supplied once")
	}
	var at string
	if len(atValues) == 1 {
		at = atValues[0]
	}
	temporalityValues := values["temporality"]
	if len(temporalityValues) > 1 {
		return nil, errors.New("temporality must be supplied once")
	}
	if at == "" {
		if len(temporalityValues) != 0 {
			return nil, errors.New("temporality requires at")
		}

		return nil, nil
	}

	parsed, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return nil, fmt.Errorf("at must be an RFC3339 timestamp: %w", err)
	}
	if parsed.UnixMicro() < 0 {
		return nil, commonpb.ErrTimestampBeforeEpoch
	}

	temporality := balancehistorystore.TemporalityEffective
	if len(temporalityValues) == 1 {
		switch temporalityValues[0] {
		case "effective":
		case "insertion":
			temporality = balancehistorystore.TemporalityInsertion
		default:
			return nil, errors.New("temporality must be effective or insertion")
		}
	}

	return &ctrl.HistoricalBalanceSelector{At: uint64(parsed.UnixMicro()), Temporality: temporality}, nil
}

func writeHistoricalBalanceViewHeader(w http.ResponseWriter, token *ctrl.HistoricalBalanceViewToken) error {
	if token.Token == "" {
		return errors.New("historical-balance immutable view token is empty")
	}
	var temporality servicepb.HistoricalBalanceTemporality
	switch token.Temporality {
	case balancehistorystore.TemporalityEffective:
		temporality = servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE
	case balancehistorystore.TemporalityInsertion:
		temporality = servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION
	default:
		return fmt.Errorf("unknown historical-balance temporality value %d", token.Temporality)
	}
	view := &servicepb.HistoricalBalanceView{
		RequestedAt:     &commonpb.Timestamp{Data: token.RequestedAt},
		Temporality:     temporality,
		Ledger:          token.Ledger,
		AuditWatermark:  token.AuditWatermark,
		LogWatermark:    token.LogWatermark,
		ManifestVersion: token.ManifestVersion,
		ViewToken:       token.Token,
	}
	encoded, err := view.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling HTTP historical-balance view: %w", err)
	}
	w.Header().Set(historicalBalanceViewHeader, base64.StdEncoding.EncodeToString(encoded))

	return nil
}
