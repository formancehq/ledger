package http

import (
	"math/big"
	"net/http"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

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
	Color   string                 `json:"color"`
	Input   *commonpb.BigUint      `json:"input"`
	Output  *commonpb.BigUint      `json:"output"`
	Balance *commonpb.SignedBigInt `json:"balance"`
}

type groupedAggregateResultJSON struct {
	Prefix  string                  `json:"prefix"`
	Volumes []*aggregatedVolumeJSON `json:"volumes"`
}

func toAggregatedVolumeJSON(v *commonpb.AggregatedVolume) (*aggregatedVolumeJSON, error) {
	input, output, err := v.Totals()
	if err != nil {
		return nil, err
	}
	balance := new(big.Int).Sub(input, output)
	inputAmount, err := commonpb.NewBigUint(input)
	if err != nil {
		return nil, err
	}
	outputAmount, err := commonpb.NewBigUint(output)
	if err != nil {
		return nil, err
	}

	return &aggregatedVolumeJSON{
		Asset:   v.GetAsset(),
		Color:   v.GetColor(),
		Input:   inputAmount,
		Output:  outputAmount,
		Balance: commonpb.NewSignedBigInt(balance),
	}, nil
}

func toAggregatedVolumesJSON(volumes []*commonpb.AggregatedVolume) ([]*aggregatedVolumeJSON, error) {
	out := make([]*aggregatedVolumeJSON, 0, len(volumes))
	for _, v := range volumes {
		vol, err := toAggregatedVolumeJSON(v)
		if err != nil {
			return nil, err
		}
		out = append(out, vol)
	}

	return out, nil
}

func toAggregateVolumesJSON(result *commonpb.AggregateResult) (*aggregateVolumesResponseJSON, error) {
	resp := &aggregateVolumesResponseJSON{}

	var err error
	if resp.Volumes, err = toAggregatedVolumesJSON(result.GetVolumes()); err != nil {
		return nil, err
	}

	if len(result.GetGroups()) > 0 {
		resp.Groups = make([]*groupedAggregateResultJSON, 0, len(result.GetGroups()))
		for _, g := range result.GetGroups() {
			volumes, err := toAggregatedVolumesJSON(g.GetVolumes())
			if err != nil {
				return nil, err
			}

			resp.Groups = append(resp.Groups, &groupedAggregateResultJSON{
				Prefix:  g.GetPrefix(),
				Volumes: volumes,
			})
		}
	}

	return resp, nil
}

// handleAggregateVolumes handles GET /{ledgerName}/volumes.
func (s *Server) handleAggregateVolumes(w http.ResponseWriter, r *http.Request) {
	// See handleListTransactions: the profile clock started in the routing layer.
	ctx := r.Context()
	profile := profileFromRequest(r)

	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	useMaxPrecision := queryParamBool(r, "useMaxPrecision")
	collapseColors := queryParamBool(r, "collapseColors")

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

	profile.EnterExecute()
	result, err := s.backend.AggregateVolumes(ctx, ledgerName, filter, query.AggregateOptions{
		UseMaxPrecision: useMaxPrecision,
		CollapseColors:  collapseColors,
		GroupByPrefixes: groupByPrefixes,
	})
	profile.LeaveExecute()

	if err != nil {
		handleError(w, r, err)

		return
	}

	resp, err := toAggregateVolumesJSON(result)
	if err != nil {
		handleError(w, r, err)

		return
	}

	finishProfile(w, r, profile)
	writeOK(w, r, resp)
}
